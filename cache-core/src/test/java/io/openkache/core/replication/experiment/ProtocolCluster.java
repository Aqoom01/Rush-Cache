package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.replication.vr.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/** PB/VR에서 동일하게 사용하는 이벤트 실행기. 연구 테스트와 Runner가 함께 사용한다. */
final class ProtocolCluster implements AutoCloseable {
    enum Variant { PB, VR, VR_INCREMENTAL }
    final Variant variant;
    final ReplicationExperiment.Scheduler scheduler = new ReplicationExperiment.Scheduler();
    final Map<Integer, CacheEngine> nodes = new LinkedHashMap<>();
    final Map<Integer, KvEngine> stores = new LinkedHashMap<>();
    final Set<Integer> dead = new HashSet<>();
    final boolean[][] links = {{true,true,true},{true,true,true},{true,true,true}};
    Predicate<PeerEnvelope> drop = e -> false;
    final List<String> events = new ArrayList<>();
    int messages, logEntries, slowAck, jitter;
    boolean duplicate;
    final Random random;
    final LinkNetwork network;
    ProtocolCluster(Variant variant, int delay, int timeout, long seed) {
        this(variant,delay,timeout,seed,-1);
    }
    ProtocolCluster(Variant variant, int delay, int timeout, long seed,long bytesPerSecond) {
        this.variant = variant; random = new Random(seed);
        network=bytesPerSecond<0?null:new LinkNetwork(scheduler,delay,bytesPerSecond);
        List<NodeId> ids = List.of(new NodeId(0),new NodeId(1),new NodeId(2));
        Transport transport = e -> {
            messages++;
            if (e.message() instanceof VrMessage.Prepare p) logEntries += p.log().size();
            if (e.message() instanceof VrMessage.DeltaPrepare p) logEntries += p.entries().size();
            if (e.message() instanceof VrMessage.StartView p) logEntries += p.log().size();
            if (e.message() instanceof VrMessage.DoViewChange p) logEntries += p.log().size();
            int from = Math.toIntExact(e.from().value()), to = Math.toIntExact(e.to().value());
            boolean ack = e.message() instanceof VrMessage.PrepareOk || e.message() instanceof PrimaryBackupMessage.ReplicateAck;
            if(network!=null) {
                network.send(e,()->!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e),
                        ()->nodes.get(to).onPeerMessage(e.from(),e.message()),from==2 && ack?slowAck:0);
                return CompletableFuture.completedFuture(null);
            }
            if (dead.contains(from) || !links[from][to] || drop.test(e)) return CompletableFuture.completedFuture(null);
            int extra = (from == 2 && ack ? slowAck : 0) + (jitter == 0 ? 0 : random.nextInt(jitter+1));
            Runnable deliver = () -> {
                if (!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e)) nodes.get(to).onPeerMessage(e.from(),e.message());
            };
            scheduler.schedule(now()+ms(delay+extra), deliver);
            if (duplicate) scheduler.schedule(now()+ms(delay+extra+1), deliver);
            return CompletableFuture.completedFuture(null);
        };
        for (int i=0; i<3; i++) {
            NodeId id = ids.get(i); KvEngine kv = new KvEngine(id,List.of(new DirectMailbox()),scheduler.time);
            stores.put(i,kv);
            nodes.put(i,variant != Variant.PB ? new VrEngine(id,ids,kv,new DirectMailbox(),transport,scheduler.time,
                    Duration.ofMillis(10),Duration.ofMillis(timeout),variant==Variant.VR?VrEngine.Mode.FULL_LOG:VrEngine.Mode.INCREMENTAL)
                    : new CrashOnlyPrimaryBackupEngine(new PrimaryBackupConfig(id,ids.get(0),ids.subList(1,3),new OwnershipEpoch(1)),
                    kv,new DirectMailbox(),transport,scheduler.time,Duration.ofMillis(10),Duration.ofMillis(timeout)));
        }
        tick();
    }
    static long ms(long value) { return Duration.ofMillis(value).toNanos(); }
    long now() { return scheduler.time.monotonicNanos(); }
    long nowMs() { return Duration.ofNanos(now()).toMillis(); }
    void tick() {
        nodes.forEach((id,node) -> { if (!dead.contains(id)) node.onTimer(new TimerEvent(variant != Variant.PB ? VrEngine.TICK : CrashOnlyPrimaryBackupEngine.TICK,now())); });
        scheduler.schedule(now()+ms(1),this::tick);
    }
    void advance(int durationMs) { until(now()+ms(durationMs)); }
    void until(long target) {
        int steps=0;
        while (!scheduler.events.isEmpty() && scheduler.events.peek().at() <= target) {
            if (++steps>2_000_000) throw new IllegalStateException("event limit");
            var e=scheduler.events.remove(); scheduler.time.advanceTo(e.at()); e.action().run();
        }
        scheduler.time.advanceTo(target);
    }
    void isolate(int node) { for(int i=0;i<3;i++) if(i!=node) links[node][i]=links[i][node]=false; events.add(nowMs()+": isolate "+node); }
    void heal() { for(var row:links) Arrays.fill(row,true); events.add(nowMs()+": heal"); }
    void crash(int node) { dead.add(node); nodes.get(node).close(); events.add(nowMs()+": crash "+node); }
    RequestContext context(long operation, int node, int deadlineMs) {
        return new RequestContext(new ClientId(7),new OperationId(operation),RoutingVersion.ZERO,
                nodes.get(node).snapshot().ownershipEpoch(),new Deadline(now()+ms(deadlineMs)));
    }
    CompletableFuture<Result> submit(int node, long operation, Command command, int deadlineMs) {
        var future=nodes.get(node).execute(command,context(operation,node,deadlineMs)).toCompletableFuture();
        long start=nowMs(); future.thenAccept(r->events.add(start+"->"+nowMs()+": node="+node+" op="+operation+" "+r.status()));
        return future;
    }
    CompletableFuture<Result> set(int node,long operation,String key,String value,int deadlineMs) {
        return submit(node,operation,new Command.Set(Bytes.utf8(key),Bytes.utf8(value)),deadlineMs);
    }
    Result local(int node,String key) {
        return stores.get(node).execute(new Command.Get(Bytes.utf8(key)),context(99999,node,1000)).toCompletableFuture().join();
    }
    VrEngine.Observation observation(int node) { return ((VrEngine)nodes.get(node)).observation(); }
    int leader() {
        for(int i=0;i<3;i++) if (!dead.contains(i)) {
            if(variant!=Variant.PB) { var o=observation(i); if(o.state()==VrEngine.State.NORMAL && o.view()%3==i) return i; }
        }
        return -1;
    }
    @Override public void close() { nodes.values().forEach(CacheEngine::close); stores.values().forEach(KvEngine::close); }
}
