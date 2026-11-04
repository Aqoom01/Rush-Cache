package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.chain.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/** 외부 master는 정확한 영구 crash만 제거한다. 데이터 복사는 엔진의 peer 경로로 수행한다. */
final class ChainCluster implements AutoCloseable {
    final ReplicationExperiment.Scheduler scheduler=new ReplicationExperiment.Scheduler();
    final List<ChainEngine> nodes=new ArrayList<>();
    final List<KvEngine> stores=new ArrayList<>();
    final Set<Integer> dead=new HashSet<>();
    final boolean[][] links={{true,true,true},{true,true,true},{true,true,true}};
    final LinkNetwork network;
    Predicate<PeerEnvelope> drop=e->false;
    int slowAck;
    boolean duplicate,masterEnabled=true,masterBusy;
    long detectedAt=-1,readyAt=-1,masterEpoch=1;
    private final int detectionMs;
    private List<NodeId> installed=List.of(new NodeId(0),new NodeId(1),new NodeId(2));
    ChainCluster(int delay,int timeout,long rate) {
        detectionMs=timeout; network=new LinkNetwork(scheduler,delay,rate);
        Transport transport=e->{
            int from=(int)e.from().value(),to=(int)e.to().value();
            Runnable delivery=()->nodes.get(to).onPeerMessage(e.from(),e.message());
            boolean confirmation=e.message() instanceof ChainMessage.Ack || e.message() instanceof ChainMessage.Completed;
            network.send(e,()->!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e),delivery,
                    from==2 && confirmation?slowAck:0);
            if(duplicate) scheduler.schedule(now()+ProtocolCluster.ms(delay+4),()->{
                if(!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e)) delivery.run();
            });
            return CompletableFuture.completedFuture(null);
        };
        for(int i=0;i<3;i++) {
            NodeId id=new NodeId(i); var kv=new KvEngine(id,List.of(new DirectMailbox()),scheduler.time); stores.add(kv);
            nodes.add(new ChainEngine(id,installed,1,kv,new DirectMailbox(),transport,scheduler.time,Duration.ofSeconds(1)));
        }
        tick();
    }
    private void tick() {
        for(int i=0;i<3;i++) if(!dead.contains(i)) nodes.get(i).onTimer(new TimerEvent(ChainEngine.TICK,now()));
        scheduler.schedule(now()+ProtocolCluster.ms(1),this::tick);
    }
    long now() { return scheduler.time.monotonicNanos(); }
    void advance(int ms) { until(now()+ProtocolCluster.ms(ms)); }
    void until(long target) {
        int steps=0;
        while(!scheduler.events.isEmpty() && scheduler.events.peek().at()<=target) {
            if(++steps>2_000_000) throw new IllegalStateException("event limit");
            var e=scheduler.events.remove(); scheduler.time.advanceTo(e.at()); e.action().run();
        }
        scheduler.time.advanceTo(target);
    }
    int head() { return (int)installed.getFirst().value(); }
    int tail() { return (int)installed.getLast().value(); }
    int readyHead() { return dead.contains(head()) || nodes.get(head()).observation().state()!=ChainEngine.State.RUNNING?-1:head(); }
    void isolate(int node) { for(int i=0;i<3;i++) if(i!=node) links[node][i]=links[i][node]=false; }
    void heal() { for(var row:links) Arrays.fill(row,true); }
    void crash(int node) {
        if(!dead.add(node)) throw new IllegalArgumentException("already dead"); nodes.get(node).close();
        if(masterEnabled) scheduler.schedule(now()+ProtocolCluster.ms(detectionMs),this::detect);
    }
    private void detect() {
        if(masterBusy) { scheduler.schedule(now()+ProtocolCluster.ms(1),this::detect); return; }
        List<NodeId> survivors=installed.stream().filter(n->!dead.contains((int)n.value())).toList();
        if(survivors.isEmpty() || survivors.equals(installed)) return;
        detectedAt=now(); readyAt=-1; masterBusy=true; masterEpoch++;
        var futures=survivors.stream().map(n->nodes.get((int)n.value()).installConfiguration(masterEpoch,survivors).toCompletableFuture()).toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(futures).whenComplete((ignored,error)->{
            if(error!=null) throw new IllegalStateException("master freeze failed",error);
            installed=survivors;
            // master의 freeze/open 조정에 각각 2ms. master 통신은 데이터 링크 통계와 분리한다.
            scheduler.schedule(now()+ProtocolCluster.ms(2),()->{
                for(NodeId n:survivors) nodes.get((int)n.value()).beginRepair(masterEpoch).toCompletableFuture().join();
                checkRepair();
            });
        });
    }
    private void checkRepair() {
        int length=nodes.get(head()).observation().logLength(); boolean complete=true;
        for(NodeId id:installed) {
            var o=nodes.get((int)id.value()).observation();
            complete&=o.state()==ChainEngine.State.REPAIRING && o.applied()==length && o.logLength()==length && o.acknowledged()==length;
        }
        if(!complete) { scheduler.schedule(now()+ProtocolCluster.ms(1),this::checkRepair); return; }
        var expected=nodes.get(head()).logSnapshot().toCompletableFuture().join();
        for(NodeId n:installed) if(!expected.equals(nodes.get((int)n.value()).logSnapshot().toCompletableFuture().join()))
            throw new IllegalStateException("repaired chain prefixes differ");
        scheduler.schedule(now()+ProtocolCluster.ms(2),()->{
            for(NodeId n:installed) nodes.get((int)n.value()).openConfiguration(masterEpoch,length).toCompletableFuture().join();
            readyAt=now(); masterBusy=false;
        });
    }
    CompletableFuture<Result> execute(int node,long op,Command command,int deadlineMs) {
        return nodes.get(node).execute(command,new RequestContext(new ClientId(7),new OperationId(op),RoutingVersion.ZERO,
                nodes.get(node).snapshot().ownershipEpoch(),new Deadline(now()+ProtocolCluster.ms(deadlineMs)))).toCompletableFuture();
    }
    CompletableFuture<Result> set(long op,String key,String value,int deadlineMs) {
        return execute(head(),op,new Command.Set(Bytes.utf8(key),Bytes.utf8(value)),deadlineMs);
    }
    Result read(int node,String key) { return execute(node,999,new Command.Get(Bytes.utf8(key)),1000).join(); }
    Result local(int node,String key) {
        return stores.get(node).execute(new Command.Get(Bytes.utf8(key)),new RequestContext(new ClientId(99),new OperationId(1),RoutingVersion.ZERO,
                OwnershipEpoch.ZERO,new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
    }
    void assertPrefix() {
        List<ChainMessage.Request> previous=null;
        for(int i=0;i<3;i++) if(!dead.contains(i)) {
            var log=nodes.get(i).logSnapshot().toCompletableFuture().join();
            if(previous!=null && (previous.size()<log.size() || !previous.subList(0,log.size()).equals(log))) throw new IllegalStateException("chain prefix invariant failed");
            previous=log;
        }
    }
    @Override public void close() { nodes.forEach(ChainEngine::close); stores.forEach(KvEngine::close); }
}
