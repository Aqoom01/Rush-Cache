package io.openkache.server.ionia_sixth;

import io.openkache.core.replication.ionia.IoniaCommands;
import io.openkache.core.replication.ionia_sixth.IoniaWorkerSettings;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaStorage;
import io.openkache.core.runtime.*;
import io.openkache.core.spi.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.zip.CRC32C;

/** V04b: 고정 파일 shard와 전용 I/O worker 수를 분리한 JDK 파일 KV 어댑터. Values are read from disk; RAM retains key/offset metadata.
 * shard % W의 고정 owner가 해당 파일 I/O를 직렬화한다. syncKvWrites controls KV force only, NOT fast-ACK durability logs.
 * This functional Java adapter does not claim SplinterDB/LSM performance equivalence.
 */
public final class IoniaWorkerFileStorage implements IoniaStorage {
    private record Entry(long offset, int length, long version) { }
    private static final int MAX_RECORD = 32 * 1024 * 1024;
    private final NodeId node;
    private final TimeSource time;
    private final List<OwnerWorker> workers = new ArrayList<>();
    private final List<FileChannel> files = new ArrayList<>();
    private final List<Map<Bytes, Entry>> indexes = new ArrayList<>();
    private final Set<CompletableFuture<?>> pending = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final boolean syncKvWrites;
    private final IoniaWorkerSettings settings;
    private final IoHook hook;
    private final AtomicLongArray submitted,started,finished,rejected,threadIds;
    private final AtomicInteger activeTasks=new AtomicInteger(),peakTasks=new AtomicInteger();
    @FunctionalInterface interface IoHook { void before(int shard,boolean reset) throws IOException,InterruptedException; }
    /** 비원자 관측 카운터. active/queued는 작업 수이며 disk I/O나 처리량 지표가 아니다. */
    public record WorkerSnapshot(int worker,long submitted,long started,long finished,long rejected,long queuedEstimate,long lastThreadId) { }
    public record Diagnostics(int storageShards,int ioWorkers,int activeTasks,int peakTasks,boolean closed,List<WorkerSnapshot> workers) {
        public Diagnostics { workers=List.copyOf(workers); }
    }
    private final FileChannel lockChannel;
    private final FileLock lock;

    /** 파일 수는 S, 전용 thread 수는 W다. true force는 KV 파일에만 적용한다. */
    public IoniaWorkerFileStorage(NodeId node,Path directory,IoniaWorkerSettings settings,TimeSource time,boolean syncKvWrites) throws IOException {
        this(node,directory,settings,time,syncKvWrites,(shard,reset)->{ });
    }
    IoniaWorkerFileStorage(NodeId node,Path directory,IoniaWorkerSettings settings,TimeSource time,boolean syncKvWrites,IoHook hook) throws IOException {
        this(node,directory,settings,time,syncKvWrites,hook,IoniaWorkerSettings.MAILBOX_CAPACITY);
    }
    IoniaWorkerFileStorage(NodeId node,Path directory,IoniaWorkerSettings settings,TimeSource time,boolean syncKvWrites,IoHook hook,int mailboxCapacity) throws IOException {
        if(mailboxCapacity<1 || mailboxCapacity>IoniaWorkerSettings.MAILBOX_CAPACITY)throw new IllegalArgumentException("invalid mailbox capacity");
        this.settings=Objects.requireNonNull(settings);this.hook=Objects.requireNonNull(hook);
        submitted=new AtomicLongArray(settings.ioWorkers());started=new AtomicLongArray(settings.ioWorkers());
        finished=new AtomicLongArray(settings.ioWorkers());rejected=new AtomicLongArray(settings.ioWorkers());threadIds=new AtomicLongArray(settings.ioWorkers());
        this.node = Objects.requireNonNull(node); this.time = Objects.requireNonNull(time); this.syncKvWrites = syncKvWrites;
        Files.createDirectories(directory);
        lockChannel = FileChannel.open(directory.resolve("storage.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired;
        try { acquired = lockChannel.tryLock(); }
        catch (IOException | RuntimeException error) { lockChannel.close(); throw error; }
        if (acquired == null) { lockChannel.close(); throw new IOException("storage directory is already in use"); }
        lock = acquired;
        try {
            validateLayout(directory);
            for (int i = 0; i < settings.storageShards(); i++) {
                FileChannel file = FileChannel.open(directory.resolve("kv-" + i + ".data"),
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                files.add(file); indexes.add(new HashMap<>()); scan(i);
            }
            for(int i=0;i<settings.ioWorkers();i++) {
                workers.add(new OwnerWorker(new WorkerId(i),mailboxCapacity));
            }
        } catch (IOException | RuntimeException error) { close(); throw error; }
    }

    @Override public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        if (!IoniaCommands.supports(command)) {
            return CompletableFuture.completedFuture(result(ResultStatus.BAD_REQUEST));
        }
        CompletableFuture<Result> future = track(new CompletableFuture<>());
        if (closed.get()) { future.complete(result(ResultStatus.UNAVAILABLE)); return future; }
        int shard = Math.floorMod(IoniaCommands.key(command).hashCode(), files.size());
        if (!offerTask(shard,() -> {
            if (closed.get()) future.complete(result(ResultStatus.UNAVAILABLE));
            else if (context.deadline().isExpired(time)) future.complete(result(ResultStatus.TIMEOUT));
            else try { hook.before(shard,false);future.complete(run(shard, command)); }
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();future.completeExceptionally(interrupted);}
            catch (IOException | RuntimeException error) { future.completeExceptionally(error); }
        })) future.complete(result(ResultStatus.BUSY));
        return future;
    }
    private Result run(int shard, Command command) throws IOException {
        Map<Bytes, Entry> index = indexes.get(shard);
        FileChannel file = files.get(shard);
        if (command instanceof Command.Get get) {
            Entry entry = index.get(get.key());
            if (entry == null) return result(ResultStatus.MISS);
            ByteBuffer bytes = ByteBuffer.allocate(entry.length());
            readFully(file, bytes, entry.offset());
            return Result.ok(Bytes.copyOf(bytes.array()), entry.version(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
        Entry previous = index.get(IoniaCommands.key(command));
        long version = previous == null ? 1 : Math.incrementExact(previous.version());
        byte[] key = IoniaCommands.key(command).toByteArray();
        byte[] value = command instanceof Command.Set set ? set.value().toByteArray() : new byte[0];
        long payloadLength = 17L + key.length + value.length;
        if (payloadLength > MAX_RECORD) return result(ResultStatus.CAPACITY_EXCEEDED);
        ByteBuffer payload = ByteBuffer.allocate((int) payloadLength);
        payload.put((byte) (command instanceof Command.Set ? 1 : 2)).putInt(key.length).putInt(value.length).putLong(version).put(key).put(value);
        CRC32C crc = new CRC32C(); crc.update(payload.array());
        ByteBuffer record = ByteBuffer.allocate(payload.capacity() + 8).putInt(payload.capacity()).put(payload.array()).putInt((int) crc.getValue());
        record.flip(); long start = file.size();
        while (record.hasRemaining()) file.write(record, start + record.position());
        if (syncKvWrites) file.force(false);
        if (command instanceof Command.Set) {
            index.put(IoniaCommands.key(command), new Entry(start + 21L + key.length, value.length, version));
            return new Result(ResultStatus.OK, Optional.empty(), version, RoutingVersion.ZERO, OwnershipEpoch.ZERO, Optional.empty());
        }
        index.remove(IoniaCommands.key(command));
        return result(previous == null ? ResultStatus.MISS : ResultStatus.OK);
    }

    private void scan(int shard) throws IOException {
        FileChannel file = files.get(shard); Map<Bytes, Entry> index = indexes.get(shard);
        long position = 0, size = file.size();
        while (position < size) {
            if (size - position < 4) { file.truncate(position); break; }
            ByteBuffer length = ByteBuffer.allocate(4); readFully(file, length, position); length.flip(); int n = length.getInt();
            if (n < 17 || n > MAX_RECORD) throw new IOException("invalid KV record length at " + position);
            if (size - position < n + 8L) { file.truncate(position); break; }
            ByteBuffer body = ByteBuffer.allocate(n + 4); readFully(file, body, position + 4); body.flip();
            byte[] payload = new byte[n]; body.get(payload); int checksum = body.getInt();
            CRC32C crc = new CRC32C(); crc.update(payload);
            if ((int) crc.getValue() != checksum) throw new IOException("KV checksum mismatch at " + position);
            ByteBuffer decoded = ByteBuffer.wrap(payload); byte op = decoded.get();
            int keyLength = decoded.getInt(), valueLength = decoded.getInt(); long version = decoded.getLong();
            if (keyLength < 0 || valueLength < 0 || 17L + keyLength + valueLength != n || version < 1 || (op != 1 && op != 2))
                throw new IOException("invalid KV record at " + position);
            byte[] key = new byte[keyLength]; decoded.get(key); Bytes k = Bytes.copyOf(key);
            if (op == 1) index.put(k, new Entry(position + 21L + keyLength, valueLength, version)); else index.remove(k);
            position += n + 8L;
        }
    }
    private static void readFully(FileChannel file, ByteBuffer buffer, long offset) throws IOException {
        while (buffer.hasRemaining()) if (file.read(buffer, offset + buffer.position()) < 0) throw new EOFException("truncated KV data");
    }
    @Override public CompletionStage<Void> reset() {
        List<CompletableFuture<Void>> resets = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            int shard = i; CompletableFuture<Void> future = track(new CompletableFuture<>()); resets.add(future);
            if (closed.get() || !offerTask(shard,() -> {
                try { hook.before(shard,true);files.get(shard).truncate(0); indexes.get(shard).clear(); future.complete(null); }
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();future.completeExceptionally(interrupted);}
                catch (IOException | RuntimeException error) { future.completeExceptionally(error); }
            })) future.completeExceptionally(new IllegalStateException("storage closed/busy"));
        }
        return CompletableFuture.allOf(resets.toArray(CompletableFuture<?>[]::new));
    }
    private boolean offerTask(int shard,Runnable task){
        int worker=settings.workerForShard(shard);submitted.incrementAndGet(worker);
        boolean accepted=workers.get(worker).offer(()->{
            started.incrementAndGet(worker);threadIds.set(worker,Thread.currentThread().threadId());
            int active=activeTasks.incrementAndGet();peakTasks.accumulateAndGet(active,Math::max);
            try{task.run();}finally{activeTasks.decrementAndGet();finished.incrementAndGet(worker);}
        });
        if(!accepted)rejected.incrementAndGet(worker);return accepted;
    }
    private void validateLayout(Path directory)throws IOException{
        Path manifest=directory.resolve("io-layout-v1.txt");
        String expected="ionia-worker-kv-v1|shards="+settings.storageShards()+"|routing=bytesHashMod\n";
        Set<String> dataNames=new HashSet<>();
        try(var paths=Files.list(directory)){
            for(Path path:paths.toList())if(path.getFileName().toString().matches("kv-[0-9]+[.]data"))dataNames.add(path.getFileName().toString());
        }
        if(Files.exists(manifest)){
            if(!expected.equals(Files.readString(manifest)))throw new IOException("storage layout mismatch");
            for(String name:dataNames){boolean valid=false;for(int i=0;i<settings.storageShards();i++)if(name.equals("kv-"+i+".data"))valid=true;
                if(!valid)throw new IOException("unexpected storage shard: "+name);}
        }else{
            if(!dataNames.isEmpty())throw new IOException("missing V04b layout metadata; no automatic migration");
            byte[] bytes=expected.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            try(FileChannel channel=FileChannel.open(manifest,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){
                ByteBuffer data=ByteBuffer.wrap(bytes);while(data.hasRemaining())channel.write(data);channel.force(true);
            }
        }
    }
    /** thread-safe 관측. W>S이면 사용되지 않는 worker도 표시한다. idle 시 카운터가 정확히 수렴한다. */
    public Diagnostics diagnostics(){
        List<WorkerSnapshot> snapshots=new ArrayList<>();
        for(int i=0;i<settings.ioWorkers();i++)snapshots.add(new WorkerSnapshot(i,submitted.get(i),started.get(i),finished.get(i),rejected.get(i),
                Math.max(0,submitted.get(i)-started.get(i)-rejected.get(i)),threadIds.get(i)));
        return new Diagnostics(settings.storageShards(),settings.ioWorkers(),activeTasks.get(),peakTasks.get(),closed.get(),snapshots);
    }
    private <T> CompletableFuture<T> track(CompletableFuture<T> future) {
        pending.add(future); future.whenComplete((result, error) -> pending.remove(future)); return future;
    }
    @Override public EngineSnapshot snapshot() { return new EngineSnapshot(node, time.monotonicNanos(), RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
    @Override public void onPeerMessage(NodeId from, PeerMessage message) { }
    @Override public void onTimer(TimerEvent event) { }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        pending.forEach(future -> future.completeExceptionally(new IllegalStateException("storage closed")));
        workers.forEach(OwnerWorker::close);
        for (FileChannel file : files) try { file.close(); } catch (IOException ignored) { }
        try { lock.release(); } catch (IOException ignored) { }
        try { lockChannel.close(); } catch (IOException ignored) { }
    }
    private static Result result(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
}
