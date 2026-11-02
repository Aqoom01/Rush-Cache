package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.raft.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/** 기존 PB/VR과 같은 scheduler/FIFO 링크 모델을 사용하는 Raft 검증 fixture. */
final class RaftCluster implements AutoCloseable {
    final ReplicationExperiment.Scheduler scheduler=new ReplicationExperiment.Scheduler();
    final List<RaftEngine> nodes=new ArrayList<>();
    final List<KvEngine> stores=new ArrayList<>();
    final Set<Integer> dead=new HashSet<>();
    final boolean[][] links={{true,true,true},{true,true,true},{true,true,true}};
    final LinkNetwork network;
    Predicate<PeerEnvelope> drop=e->false;
    int slowNode=-1,slowAck;
    boolean duplicate;
    RaftCluster(int delay,int timeout,long seed,long rate) {
        network=new LinkNetwork(scheduler,delay,rate);
        var ids=List.of(new NodeId(0),new NodeId(1),new NodeId(2));
        Transport transport=e->{
            int from=(int)e.from().value(),to=(int)e.to().value();
            Runnable delivery=()->nodes.get(to).onPeerMessage(e.from(),e.message());
            network.send(e,()->!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e),delivery,
                    from==slowNode && e.message() instanceof RaftMessage.AppendReply?slowAck:0);
            if(duplicate) scheduler.schedule(now()+ProtocolCluster.ms(delay+3),()->{
                if(!dead.contains(from) && !dead.contains(to) && links[from][to] && !drop.test(e)) delivery.run();
            });
            return CompletableFuture.completedFuture(null);
        };
        for(int i=0;i<3;i++) {
            var kv=new KvEngine(ids.get(i),List.of(new DirectMailbox()),scheduler.time); stores.add(kv);
            nodes.add(new RaftEngine(ids.get(i),ids,kv,new DirectMailbox(),transport,scheduler.time,
                    Duration.ofMillis(10),Duration.ofMillis(timeout),seed*1009+i*7919L));
        }
        tick();
    }
    private void tick() {
        for(int i=0;i<3;i++) if(!dead.contains(i)) nodes.get(i).onTimer(new TimerEvent(RaftEngine.TICK,now()));
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
    int leader() {
        long max=-1; int id=-1;
        for(int i=0;i<3;i++) if(!dead.contains(i)) {
            var o=nodes.get(i).observation();
            if(o.role()==RaftEngine.Role.LEADER && o.term()>max) { max=o.term(); id=i; }
        }
        return id;
    }
    int readyLeader(int budgetMs) {
        for(int i=0;i<budgetMs;i++) {
            int l=leader();
            if(l>=0) { var o=nodes.get(l).observation(); if(o.applied()==o.logLength()) return l; }
            advance(1);
        }
        throw new IllegalStateException("no ready leader");
    }
    void isolate(int node) { for(int i=0;i<3;i++) if(i!=node) links[node][i]=links[i][node]=false; }
    void heal() { for(var row:links) Arrays.fill(row,true); }
    void crash(int node) { dead.add(node); nodes.get(node).close(); }
    CompletableFuture<Result> set(int node,long operation,String key,String value,int deadlineMs) {
        return execute(node,operation,new Command.Set(Bytes.utf8(key),Bytes.utf8(value)),deadlineMs);
    }
    CompletableFuture<Result> execute(int node,long operation,Command command,int deadlineMs) {
        return nodes.get(node).execute(command,new RequestContext(new ClientId(7),new OperationId(operation),RoutingVersion.ZERO,
                nodes.get(node).snapshot().ownershipEpoch(),new Deadline(now()+ProtocolCluster.ms(deadlineMs)))).toCompletableFuture();
    }
    Result local(int node,String key) {
        return stores.get(node).execute(new Command.Get(Bytes.utf8(key)),new RequestContext(new ClientId(99),new OperationId(1),
                RoutingVersion.ZERO,new OwnershipEpoch(0),new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
    }
    @Override public void close() { nodes.forEach(RaftEngine::close); stores.forEach(KvEngine::close); }
}
