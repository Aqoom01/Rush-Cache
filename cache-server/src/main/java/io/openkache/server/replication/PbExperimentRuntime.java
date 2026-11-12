package io.openkache.server.replication;

import io.openkache.core.kv.KvStorage;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.spi.MetricsSink;
import io.openkache.core.spi.Transport;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/** Explicit experiment-only storage, deterministic I/O completion stall and evidence counters. */
public final class PbExperimentRuntime implements MetricsSink,AutoCloseable {
    private final Path storageDir, faultFile;
    private final boolean direct;
    private final ConcurrentHashMap<String,AtomicLong> counters=new ConcurrentHashMap<>();
    private final ScheduledExecutorService delayed;
    private final Set<CompletableFuture<Void>> sends=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed=new AtomicBoolean();
    private final Random random=new Random(19);
    public PbExperimentRuntime(Path storageDir,boolean direct,Path faultFile) {
        this.storageDir=storageDir; this.direct=direct; this.faultFile=faultFile;
        delayed=faultFile==null?null:Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"experiment-message-delay"));
    }
    public static PbExperimentRuntime defaults() { return new PbExperimentRuntime(null,true,null); }
    public static PbExperimentRuntime environment() {
        Map<String,String> env=System.getenv();
        return new PbExperimentRuntime(path(env.get("OPENKACHE_PB_STORAGE_DIR")),
                Boolean.parseBoolean(env.getOrDefault("OPENKACHE_PB_STORAGE_DIRECT","true")),
                path(env.get("OPENKACHE_PB_FAULT_FILE")));
    }
    private static Path path(String value) { return value==null || value.isBlank()?null:Path.of(value).toAbsolutePath(); }
    KvStorage storage() {
        if(storageDir==null) return KvStorage.memory();
        // NvmeKvStore clears old segments: only hand it a newly-created per-run directory.
        try { Files.createDirectory(storageDir); }
        catch(IOException e) { throw new IllegalStateException("NVMe experiment needs a fresh directory: "+storageDir,e); }
        return new KvStorage.Nvme(storageDir,128L*1024*1024,8L*1024*1024,4096,direct);
    }
    Mailbox kvMailbox(Mailbox delegate) {
        return new Mailbox() {
            @Override public boolean offer(Runnable task) {
                return delegate.offer(()->{
                    long began=System.nanoTime();
                    boolean injected=false;
                    while(!Thread.currentThread().isInterrupted() && stallUntil()>System.currentTimeMillis()) {
                        injected=true;
                        LockSupport.parkNanos(10_000_000);
                    }
                    long waited=System.nanoTime()-began;
                    if(injected) { increment("injected_io_stall_tasks",1); recordNanos("injected_io_stall",waited); }
                    task.run();
                });
            }
            @Override public int capacity() { return delegate.capacity(); }
        };
    }
    private long stallUntil() {
        Properties p=fault();
        return "io-stall".equals(p.getProperty("kind"))?Long.parseLong(p.getProperty("until.epoch.ms","0")):0;
    }
    private Properties fault() {
        Properties p=new Properties();
        if(faultFile==null || !Files.exists(faultFile)) return p;
        try(var in=Files.newBufferedReader(faultFile)) {
            p.load(in);
            if(Long.parseLong(p.getProperty("until.epoch.ms","0"))<=System.currentTimeMillis()) p.clear();
        } catch(IOException | IllegalArgumentException e) { p.clear(); }
        return p;
    }
    public boolean dropIncoming(String plane) {
        Properties p=fault();
        return "partition".equals(p.getProperty("kind")) && applies(p,plane);
    }
    private static boolean applies(Properties p,String plane) { return "udp".equals(plane) || "both".equals(p.getProperty("scope")); }
    Transport transport(Transport delegate,String plane) {
        if(faultFile==null) return delegate;
        return envelope->{
            Properties p=fault();String kind=p.getProperty("kind","");
            if(!applies(p,plane)) return delegate.send(envelope);
            if(kind.equals("partition") || kind.equals("loss") && random.nextDouble()<.30) {
                increment("injected_message_drops",1);return CompletableFuture.completedFuture(null);
            }
            if(!kind.equals("delay")) return delegate.send(envelope);
            if(closed.get() || sends.size()>=1024) return CompletableFuture.failedFuture(new IllegalStateException("experiment delay queue full/closed"));
            CompletableFuture<Void> future=new CompletableFuture<>();sends.add(future);
            future.whenComplete((v,e)->sends.remove(future));increment("injected_message_delays",1);
            try {
                delayed.schedule(()->{
                    if(closed.get()) { future.completeExceptionally(new IllegalStateException("experiment closed"));return; }
                    try { delegate.send(envelope).whenComplete((v,e)->{ if(e==null) future.complete(null);else future.completeExceptionally(e); }); }
                    catch(RuntimeException e) { future.completeExceptionally(e); }
                },200+random.nextInt(101),TimeUnit.MILLISECONDS);
            } catch(RejectedExecutionException e) { future.completeExceptionally(e); }
            return future;
        };
    }
    @Override public void close() {
        closed.set(true);if(delayed!=null) delayed.shutdownNow();
        sends.forEach(f->f.completeExceptionally(new IllegalStateException("experiment closed")));
    }
    @Override public void increment(String name,long delta) { counters.computeIfAbsent(name,k->new AtomicLong()).addAndGet(delta); }
    @Override public void recordNanos(String name,long nanos) {
        increment(name+"_nanos",nanos); counters.computeIfAbsent(name+"_max_nanos",k->new AtomicLong()).accumulateAndGet(nanos,Math::max);
    }
    @Override public void gauge(String name,double value) {}
    String json() {
        String values=counters.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(e->"\""+e.getKey()+"\":"+e.getValue().get()).collect(java.util.stream.Collectors.joining(",","{","}"));
        return ",\"storageEngine\":\""+(storageDir==null?"memory":"nvme")+"\",\"storageDirectIo\":"+direct+",\"storageCounters\":"+values;
    }
}
