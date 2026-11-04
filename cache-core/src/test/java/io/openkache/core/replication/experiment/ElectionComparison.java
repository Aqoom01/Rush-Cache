package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.raft.*;
import io.openkache.core.replication.vr.*;
import io.openkache.core.replication.chain.*;
import io.openkache.core.spi.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** PB/VR/Raft 공통 링크·명령·재시도 정책의 선출 비교. 기존 실험 variant/결과는 수정하지 않는다. */
final class ElectionComparison {
    enum Scenario { NORMAL, SLOW_BACKUP, CRASH, PARTITION, INFLIGHT_CRASH, FOLLOWER_REJOIN }
    record Condition(String variant,Scenario scenario,int seed,int valueBytes,int mibps) {
        Condition {
            if(!List.of("PB","VR","RAFT","CHAIN").contains(variant) || scenario==null || seed<1 || seed>3
                    || !List.of(64,8192).contains(valueBytes) || !List.of(1,10).contains(mibps)) throw new IllegalArgumentException();
        }
        String id() { return variant+"-"+scenario+"-s"+seed+"-v"+valueBytes+"-b"+mibps; }
    }
    record Measurement(Condition condition,int initialLeader,int nextLeader,long startupNs,long faultAtNs,
                       long firstOkNs,int attempts,int preOk,int retainedPre,int postOk,int retainedPost,
                       int timedOut,int busy,int unavailable,int stale,int redirects,int minorityOk,
                       int inflightRetained,int converged,int elections,long enqueuedBytes,long messages,
                       long successP50Ns,long successP99Ns) {}
    static List<Condition> conditions(boolean quick) {
        List<Condition> result=new ArrayList<>();
        for(String v:List.of("PB","VR","RAFT")) for(Scenario s:Scenario.values())
            for(int seed=1;seed<=(quick?1:3);seed++) for(int value:new int[]{64,8192})
                for(int rate:quick?new int[]{1}:new int[]{1,10}) result.add(new Condition(v,s,seed,value,rate));
        return result;
    }
    /** 기존 fixture는 그대로 감싸서 과거 CSV 재현 조건을 보존한다. */
    private static final class Cluster implements AutoCloseable {
        final ProtocolCluster legacy;
        final RaftCluster raft;
        final ChainCluster chain;
        Cluster(Condition c) {
            chain=c.variant().equals("CHAIN")?new ChainCluster(1,200,c.mibps()*1_048_576L):null;
            raft=c.variant().equals("RAFT")?new RaftCluster(1,200,c.seed(),c.mibps()*1_048_576L):null;
            legacy=raft==null && chain==null?new ProtocolCluster(c.variant().equals("PB")?ProtocolCluster.Variant.PB:ProtocolCluster.Variant.VR_INCREMENTAL,
                    1,200,c.seed(),c.mibps()*1_048_576L):null;
        }
        long now() { return chain!=null?chain.now():raft!=null?raft.now():legacy.now(); }
        void advance(int ms) { if(chain!=null) chain.advance(ms); else if(raft!=null) raft.advance(ms); else legacy.advance(ms); }
        CacheEngine node(int i) { return chain!=null?chain.nodes.get(i):raft!=null?raft.nodes.get(i):legacy.nodes.get(i); }
        Set<Integer> dead() { return chain!=null?chain.dead:raft!=null?raft.dead:legacy.dead; }
        LinkNetwork network() { return chain!=null?chain.network:raft!=null?raft.network:legacy.network; }
        void drop(Predicate<PeerEnvelope> p) { if(chain!=null) chain.drop=p; else if(raft!=null) raft.drop=p; else legacy.drop=p; }
        void isolate(int n) { if(chain!=null) chain.isolate(n); else if(raft!=null) raft.isolate(n); else legacy.isolate(n); }
        void heal() { if(chain!=null) chain.heal(); else if(raft!=null) raft.heal(); else legacy.heal(); }
        void crash(int n) { if(chain!=null) chain.crash(n); else if(raft!=null) raft.crash(n); else legacy.crash(n); }
        int ready() {
            if(chain!=null) return chain.readyHead();
            if(raft!=null) {
                int l=raft.leader(); if(l<0) return -1;
                var o=raft.nodes.get(l).observation(); return o.applied()==o.logLength()?l:-1;
            }
            if(legacy.variant==ProtocolCluster.Variant.PB) return legacy.nodes.get(1).snapshot().ownershipEpoch().value()>1?1:0;
            int l=legacy.leader(); if(l<0) return -1;
            var o=legacy.observation(l); return o.applied()==o.logLength()?l:-1;
        }
        CompletableFuture<Result> execute(int node,long operation,Bytes key,Bytes value,long epoch,int deadline) {
            return node(node).execute(new Command.Set(key,value),new RequestContext(new ClientId(31),new OperationId(operation),
                    RoutingVersion.ZERO,new OwnershipEpoch(epoch),new Deadline(now()+ProtocolCluster.ms(deadline)))).toCompletableFuture();
        }
        boolean contains(int node,long operation,Bytes key,Bytes value) {
            if(chain!=null) return chain.nodes.get(node).logSnapshot().toCompletableFuture().join().stream()
                    .anyMatch(r->r.client().value()==31 && r.operation().value()==operation && r.command().equals(new Command.Set(key,value)));
            if(raft!=null) return raft.nodes.get(node).logSnapshot().toCompletableFuture().join().stream()
                    .flatMap(e->e.request().stream()).anyMatch(r->r.client().value()==31 && r.operation().value()==operation
                            && r.command().equals(new Command.Set(key,value)));
            if(legacy.variant!=ProtocolCluster.Variant.PB) return ((VrEngine)node(node)).logSnapshot().toCompletableFuture().join().stream()
                    .anyMatch(r->r.client().value()==31 && r.operation().value()==operation && r.command().equals(new Command.Set(key,value)));
            return local(node,key).value().filter(value::equals).isPresent();
        }
        Result local(int node,Bytes key) {
            CacheEngine store=chain!=null?chain.stores.get(node):raft!=null?raft.stores.get(node):legacy.stores.get(node);
            return store.execute(new Command.Get(key),new RequestContext(new ClientId(99),new OperationId(1),RoutingVersion.ZERO,
                    new OwnershipEpoch(0),new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
        }
        int elections() {
            if(raft!=null) return raft.nodes.stream().mapToInt(n->n.observation().elections()).sum();
            return -1;
        }
        @Override public void close() { if(chain!=null) chain.close(); else if(raft!=null) raft.close(); else legacy.close(); }
    }
    private static Bytes key(int i) { return Bytes.utf8(String.format(Locale.ROOT,"key-%012d",i)); }
    private static final class Client {
        int route,attempts,timedOut,busy,unavailable,stale,redirects;
        long epoch;
        long completedAt;
        final List<Long> latencies=new ArrayList<>();
        final Set<Integer> successes=new LinkedHashSet<>();
        Client(int route,long epoch) { this.route=route; this.epoch=epoch; }
        boolean write(Cluster c,int operation,Bytes value,int budgetMs) {
            long stop=c.now()+ProtocolCluster.ms(budgetMs);
            while(c.now()<stop) {
                long at=c.now(); int destination=route;
                int deadline=(int)Math.min(1000,(stop-c.now())/ProtocolCluster.ms(1));
                if(deadline<1) return false;
                var f=c.execute(destination,operation,key(operation),value,epoch,deadline); attempts++;
                long[] callback={-1}; f.whenComplete((r,e)->callback[0]=c.now());
                while(!f.isDone() && c.now()<stop) c.advance(1);
                require(f.isDone(),"attempt did not finish by its deadline");
                var r=f.join(); epoch=Math.max(epoch,r.ownershipEpoch().value());
                switch(r.status()) {
                    case OK -> { completedAt=callback[0]; latencies.add(completedAt-at); successes.add(operation); return true; }
                    case TIMEOUT -> { timedOut++; route=(destination+1)%3; }
                    case BUSY -> { busy++; route=(destination+1)%3; }
                    case STALE_EPOCH -> { stale++; route=(destination+1)%3; }
                    case NOT_PRIMARY -> { redirects++; route=(int)r.redirect().orElseThrow().target().value(); }
                    case UNAVAILABLE -> { unavailable++; route=(destination+1)%3; }
                    default -> throw new IllegalStateException("unexpected "+r.status());
                }
                c.advance(10);
            }
            return false;
        }
    }
    static Measurement run(Condition condition) {
        try(var c=new Cluster(condition)) {
            byte[] bytes=new byte[condition.valueBytes()]; Arrays.fill(bytes,(byte)7); Bytes value=Bytes.copyOf(bytes);
            int initial=-1;
            for(int i=0;i<2000 && initial<0;i++) { initial=c.ready(); if(initial<0) c.advance(1); }
            require(initial>=0,"startup election did not complete"); long startup=c.now();
            Client client=new Client(initial,c.node(initial).snapshot().ownershipEpoch().value());
            for(int i=1;i<=64;i++) require(client.write(c,i,value,2000),"warmup failed");
            c.advance(30); int pre=64,minorityOk=0,inflightRetained=0;
            Scenario scenario=condition.scenario();
            long fault=c.now(); CompletableFuture<Result> uncertain=null,minority=null;
            if(scenario==Scenario.INFLIGHT_CRASH) {
                c.drop(e->e.message() instanceof RaftMessage.AppendReply || e.message() instanceof VrMessage.PrepareOk
                        || e.message() instanceof ChainMessage.Ack || e.message() instanceof ChainMessage.Completed
                        || e.message() instanceof io.openkache.core.replication.primarybackup.PrimaryBackupMessage.ReplicateAck);
                uncertain=c.execute(initial,65,key(65),value,client.epoch,500); c.advance(40);
                require(!uncertain.isDone(),"inflight write already answered"); fault=c.now();
            }
            if(scenario==Scenario.CRASH || scenario==Scenario.INFLIGHT_CRASH) { c.crash(initial); c.drop(e->false); }
            if(scenario==Scenario.PARTITION) {
                c.isolate(initial); minority=c.execute(initial,65,key(65),value,client.epoch,500);
            }
            if(scenario==Scenario.SLOW_BACKUP) {
                int slow=(initial+1)%3;
                if(c.chain!=null) c.chain.slowAck=200;
                else if(c.raft!=null) { c.raft.slowNode=slow; c.raft.slowAck=200; }
                else { c.legacy.slowAck=200; } // legacy fixture의 고정 지연 대상은 node 2다.
            }
            if(scenario==Scenario.FOLLOWER_REJOIN) c.isolate((initial+1)%3);
            client.attempts=client.timedOut=client.busy=client.unavailable=client.stale=client.redirects=0; client.latencies.clear();
            client.successes.clear();
            long first=-1; int post=0;
            for(int op=100;op<116;op++) {
                if(scenario==Scenario.FOLLOWER_REJOIN && op==104) { c.heal(); c.advance(2000); }
                if(client.write(c,op,value,5000)) { post++; if(first<0) first=client.completedAt; }
            }
            int next=client.route;
            if(scenario==Scenario.PARTITION) {
                while(minority!=null && !minority.isDone() && c.now()<fault+ProtocolCluster.ms(501)) c.advance(1);
                require(minority!=null && minority.isDone(),"minority deadline unfinished");
                minorityOk=minority.join().status()==ResultStatus.OK?1:0; c.heal();
            }
            if(scenario==Scenario.FOLLOWER_REJOIN) c.heal();
            if(c.chain!=null) c.chain.slowAck=0; else if(c.raft!=null) c.raft.slowAck=0; else c.legacy.slowAck=0;
            c.advance(6000);
            int retainedPre=0,retainedPost=0,converged=1;
            for(int i=1;i<=64;i++) {
                boolean any=false; for(int n=0;n<3;n++) if(!c.dead().contains(n) && c.contains(n,i,key(i),value)) any=true;
                if(any) retainedPre++;
            }
            for(int i:client.successes) {
                boolean any=false; for(int n=0;n<3;n++) if(!c.dead().contains(n) && c.contains(n,i,key(i),value)) any=true;
                if(any) retainedPost++;
            }
            if(scenario==Scenario.INFLIGHT_CRASH) for(int n=0;n<3;n++) if(!c.dead().contains(n) && c.contains(n,65,key(65),value)) { inflightRetained=1; break; }
            for(int i=1;i<=115;i++) {
                if(i>64 && i<100) continue;
                Optional<Bytes> expected=null;
                for(int n=0;n<3;n++) if(!c.dead().contains(n)) {
                    var actual=c.local(n,key(i)).value(); if(expected==null) expected=actual; else if(!expected.equals(actual)) converged=0;
                }
            }
            require(retainedPre==pre,"acknowledged pre-fault write lost");
            require(retainedPost==post,"acknowledged post-fault write lost");
            if(!condition.variant().equals("PB")) require(minorityOk==0,"minority write committed");
            var stats=c.network().stats(); List<Long> lat=client.latencies.stream().sorted().toList();
            return new Measurement(condition,initial,next,startup,fault,first,client.attempts,pre,retainedPre,post,retainedPost,
                    client.timedOut,client.busy,client.unavailable,client.stale,client.redirects,minorityOk,inflightRetained,
                    converged,c.elections(),stats.enqueuedBytes(),stats.messages(),percentile(lat,.5),percentile(lat,.99));
        }
    }
    private static long percentile(List<Long> values,double fraction) { return values.isEmpty()?-1:values.get((int)Math.ceil(values.size()*fraction)-1); }
}
