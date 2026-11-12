package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.rose.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/** 정확한 단일 Primary 영구 crash, 최장 생존 prefix 선택 및 peer 복구 모델이다. */
final class RoseCluster implements AutoCloseable {
    final ReplicationExperiment.Scheduler scheduler=new ReplicationExperiment.Scheduler();
    final List<RoseEngine> nodes=new ArrayList<>();
    final List<KvEngine> stores=new ArrayList<>();
    final Set<Integer> dead=new HashSet<>();
    final boolean[][] links={{true,true,true},{true,true,true},{true,true,true}};
    final LinkNetwork network;
    Predicate<PeerEnvelope> drop=e->false;
    int slowData,primary;
    boolean duplicate;
    long detectedAt=-1,readyAt=-1;
    private final int detectionMs;
    RoseCluster(int limit,int delay,int detectionMs,long rate) {
        this.detectionMs=detectionMs; network=new LinkNetwork(scheduler,delay,rate);
        var ids=List.of(new NodeId(0),new NodeId(1),new NodeId(2));
        Transport transport=e->{
            int from=(int)e.from().value(),to=(int)e.to().value(); Runnable deliver=()->nodes.get(to).onPeerMessage(e.from(),e.message());
            network.send(e,()->!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e),deliver,
                    to==1 && e.message() instanceof RoseMessage.Append?slowData:0);
            if(duplicate) scheduler.schedule(now()+ProtocolCluster.ms(delay+5),()->{
                if(!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e)) deliver.run();
            });
            return CompletableFuture.completedFuture(null);
        };
        for(NodeId id:ids) {
            var kv=new KvEngine(id,List.of(new DirectMailbox()),scheduler.time); stores.add(kv);
            nodes.add(new RoseEngine(id,ids,ids.getFirst(),1,limit,kv,new DirectMailbox(),transport,scheduler.time,Duration.ofSeconds(1)));
        }
        tick();
    }
    private void tick() {
        for(int i=0;i<3;i++) if(!dead.contains(i)) nodes.get(i).onTimer(new TimerEvent(RoseEngine.TICK,now()));
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
    void isolate(int node) { for(int i=0;i<3;i++) if(i!=node) links[node][i]=links[i][node]=false; }
    void heal() { for(var row:links) Arrays.fill(row,true); }
    void crashPrimary() {
        if(primary!=0 || !dead.isEmpty()) throw new IllegalStateException("one original Primary crash only");
        dead.add(0); nodes.get(0).close(); scheduler.schedule(now()+ProtocolCluster.ms(detectionMs),this::detect);
    }
    private void detect() {
        detectedAt=now(); var survivors=List.of(new NodeId(1),new NodeId(2));
        var all=survivors.stream().map(n->nodes.get((int)n.value()).freezeAfterCrash(2).toCompletableFuture()).toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(all).whenComplete((ignored,error)->{
            if(error!=null) throw new IllegalStateException("freeze failed",error);
            primary=nodes.get(1).observation().applied()>=nodes.get(2).observation().applied()?1:2;
            scheduler.schedule(now()+ProtocolCluster.ms(2),()->{
                for(NodeId n:survivors) nodes.get((int)n.value()).beginRepair(2,new NodeId(primary),survivors).toCompletableFuture().join();
                checkRepair();
            });
        });
    }
    private void checkRepair() {
        var p=nodes.get(primary).observation(); boolean complete=p.applied()==p.logLength() && p.minAck()==p.applied();
        for(int i=1;i<3;i++) { var o=nodes.get(i).observation(); complete&=o.state()==RoseEngine.State.REPAIRING && o.applied()==p.applied() && o.logLength()==p.logLength(); }
        if(!complete) { scheduler.schedule(now()+ProtocolCluster.ms(1),this::checkRepair); return; }
        if(!nodes.get(1).logSnapshot().toCompletableFuture().join().equals(nodes.get(2).logSnapshot().toCompletableFuture().join())) throw new IllegalStateException("repair prefixes differ");
        scheduler.schedule(now()+ProtocolCluster.ms(2),()->{
            for(int i=1;i<3;i++) nodes.get(i).openAfterRepair(2,p.applied()).toCompletableFuture().join(); readyAt=now();
        });
    }
    CompletableFuture<Result> execute(int node,long op,Command command,int deadlineMs) {
        return nodes.get(node).execute(command,new RequestContext(new ClientId(7),new OperationId(op),RoutingVersion.ZERO,
                nodes.get(node).snapshot().ownershipEpoch(),new Deadline(now()+ProtocolCluster.ms(deadlineMs)))).toCompletableFuture();
    }
    CompletableFuture<Result> set(long op,String key,String value,int deadlineMs) { return execute(primary,op,new Command.Set(Bytes.utf8(key),Bytes.utf8(value)),deadlineMs); }
    Result local(int node,String key) {
        return stores.get(node).execute(new Command.Get(Bytes.utf8(key)),new RequestContext(new ClientId(99),new OperationId(1),RoutingVersion.ZERO,
                OwnershipEpoch.ZERO,new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
    }
    boolean converged() {
        if(dead.contains(primary)) return false;
        var p=nodes.get(primary).observation(); if(p.state()!=RoseEngine.State.RUNNING || p.applied()!=p.logLength() || p.minAck()!=p.applied()) return false;
        for(int i=0;i<3;i++) if(!dead.contains(i)) { var o=nodes.get(i).observation(); if(o.state()!=RoseEngine.State.RUNNING || o.applied()!=p.applied() || o.logLength()!=p.logLength()) return false; }
        return network.livePendingData(dead)==0;
    }
    @Override public void close() { nodes.forEach(RoseEngine::close); stores.forEach(KvEngine::close); }
}
