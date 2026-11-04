package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.vr.*;
import java.nio.ByteBuffer;
import java.util.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 연속 입력 30초 중 11초에 실제 Primary를 종료한다. 전환 중 새 고유 요청도 계속 제출한다. */
public final class LoadCrashExperiment {
    public record Condition(String variant,String scenario,int intervalMs,int valueBytes,int mibps) {
        public String id() { return variant+"-"+scenario+"-i"+intervalMs+"-v"+valueBytes+"-b"+mibps; }
    }
    public record Measurement(Condition condition,int loadMs,long crashAtNs,long firstPostCrashOkNs,
            int preCrashOk,int preCrashPending,int evidenceCopiesAtCrash,int retainedPreCrashAtEnd,int retainedAllOkAtEnd,
            int verifiedPreCrashKvCopies,int verifiedAllOkKvCopies,int inflightRetainedAtEnd,long drainNs,boolean converged,
            int pendingAtLoadEnd,int okDuringLoad,int divergentKeys,LinkNetwork.Stats network,
            List<SustainedLoadExperiment.Phase> phases,List<SustainedLoadExperiment.Sample> samples) {}
    private static final class Request {
        final int operation,phase,node;
        final long submitted;
        long completed=-1;
        ResultStatus status;
        boolean preCrashOk,inflightAtCrash;
        Request(int operation,int phase,int node,long submitted) { this.operation=operation;this.phase=phase;this.node=node;this.submitted=submitted; }
    }
    private static final class Collector {
        final ProtocolCluster cluster;
        final Condition condition;
        final Request[] requests;
        final long crashAt;
        final Bytes value;
        long epoch,firstPostOk=-1;
        int target,offered,preOk,prePending,evidenceAtCrash;
        Collector(ProtocolCluster c,Condition condition,int loadMs) {
            cluster=c;this.condition=condition;crashAt=ProtocolCluster.ms(loadMs*11L/30);
            requests=new Request[loadMs/condition.intervalMs()];
            byte[] data=new byte[condition.valueBytes()];Arrays.fill(data,(byte)73);value=Bytes.copyOf(data);
            epoch=condition.variant().equals("PB")?1:0;
        }
        void offer(int operation,long at) {
            Request request=new Request(operation,at<crashAt?0:1,target,at);requests[operation-1]=request;offered++;
            var context=new RequestContext(new ClientId(23),new OperationId(operation),RoutingVersion.ZERO,new OwnershipEpoch(epoch),new Deadline(cluster.now()+ProtocolCluster.ms(1000)));
            cluster.nodes.get(request.node).execute(new Command.Set(key(operation),value),context).whenComplete((r,error)->{
                request.completed=cluster.now();request.status=error==null?r.status():ResultStatus.INTERNAL_ERROR;
                if(error!=null) return;
                if(r.status()==ResultStatus.OK && at>=crashAt && firstPostOk<0) firstPostOk=cluster.now();
                if(r.ownershipEpoch().value()>=epoch) {
                    epoch=r.ownershipEpoch().value();
                    if(r.status()==ResultStatus.NOT_PRIMARY) target=Math.toIntExact(r.redirect().orElseThrow().target().value());
                }
                // 죽은 노드의 낮은 epoch UNAVAILABLE도 접속 재탐색에 사용한다.
                // 이미 다른 노드로 이동한 뒤 도착한 과거 요청의 실패는 경로를 되돌리지 않는다.
                if(r.status()==ResultStatus.UNAVAILABLE && target==request.node) target=(target+1)%3;
            });
        }
        void crash() {
            if(cluster.variant!=ProtocolCluster.Variant.PB) {
                var primary=cluster.observation(0);
                require(primary.state()==VrEngine.State.NORMAL && primary.view()%3==0,"node 0 is not Primary at injection");
            }
            var logs=survivorLogs(cluster,value);
            for(Request r:requests) if(r!=null) {
                if(r.status==ResultStatus.OK) {
                    require(r.completed<crashAt,"boundary response incorrectly precedes crash");
                    r.preCrashOk=true;preOk++;
                    int copies=evidence(cluster,logs,r.operation,value);
                    require(copies>0,"pre-crash OK absent from survivors");evidenceAtCrash+=copies;
                }
                if(r.completed<0) {r.inflightAtCrash=true;prePending++;}
            }
            cluster.crash(0);
        }
        SustainedLoadExperiment.Sample sample() {
            int[] counts=new int[8];for(Request r:requests) if(r!=null) count(r,counts);
            long[] kv={cluster.dead.contains(0)?-1:countKv(cluster,0),countKv(cluster,1),countKv(cluster,2)};
            int lag=-1,apply=-1,view=0;
            if(cluster.variant!=ProtocolCluster.Variant.PB) {
                int maxLog=0,maxCommit=0;
                for(int i=0;i<3;i++) if(!cluster.dead.contains(i)) {var o=cluster.observation(i);maxLog=Math.max(maxLog,o.logLength());maxCommit=Math.max(maxCommit,o.commit());view=Math.max(view,Math.toIntExact(o.view()));}
                lag=maxLog-cluster.observation(2).logLength();apply=maxCommit-cluster.observation(2).applied();
            } else for(int i=1;i<=2;i++) view=Math.max(view,Math.toIntExact(cluster.nodes.get(i).snapshot().ownershipEpoch().value())-1);
            return new SustainedLoadExperiment.Sample(cluster.now(),offered,counts[0],counts[1],counts[2],counts[3],counts[4],counts[5],counts[6],counts[7],cluster.network.stats().pendingBytes(),cluster.network.livePendingData(cluster.dead),lag,apply,kv[0],kv[1],kv[2],view);
        }
    }
    private LoadCrashExperiment() {}
    public static List<Condition> conditions(boolean quick) {
        List<Condition> result=new ArrayList<>();
        for(String variant:List.of("PB","VR_INCREMENTAL")) for(String scenario:List.of("CRASH","SLOW_CRASH"))
            for(int interval:quick?new int[]{1,20}:new int[]{1,5,20}) for(int value:new int[]{64,8192})
                for(int rate:quick?new int[]{1}:new int[]{1,10}) result.add(new Condition(variant,scenario,interval,value,rate));
        return List.copyOf(result);
    }
    private static Bytes key(int operation) { return Bytes.copyOf(ByteBuffer.allocate(16).putLong(23).putLong(operation).array()); }
    private static Result read(ProtocolCluster c,int node,int operation) {
        return c.stores.get(node).execute(new Command.Get(key(operation)),c.context(99999,node,1000)).toCompletableFuture().join();
    }
    private static long countKv(ProtocolCluster c,int node) {
        Result result=c.stores.get(node).execute(new Command.Count(),c.context(99999,node,1000)).toCompletableFuture().join();
        require(result.status()==ResultStatus.OK,"COUNT failed");return ByteBuffer.wrap(result.value().orElseThrow().toByteArray()).getLong();
    }
    private static List<Set<Long>> survivorLogs(ProtocolCluster c,Bytes value) {
        List<Set<Long>> result=new ArrayList<>();
        for(int node=1;node<=2;node++) {
            Set<Long> operations=new HashSet<>();
            if(c.variant!=ProtocolCluster.Variant.PB) for(var request:((VrEngine)c.nodes.get(node)).logSnapshot().toCompletableFuture().join()) {
                require(request.client().value()==23 && request.command() instanceof Command.Set,"unexpected diagnostic log entry");
                var set=(Command.Set)request.command();
                require(set.key().equals(key(Math.toIntExact(request.operation().value()))) && set.value().equals(value),"log key/value corrupt");
                operations.add(request.operation().value());
            }
            result.add(operations);
        }
        return result;
    }
    private static int evidence(ProtocolCluster c,List<Set<Long>> logs,int operation,Bytes value) {
        int copies=0;
        for(int node=1;node<=2;node++) {
            if(c.variant!=ProtocolCluster.Variant.PB) {if(logs.get(node-1).contains((long)operation)) copies++;}
            else {Result r=read(c,node,operation);if(r.status()==ResultStatus.OK) {require(r.value().orElseThrow().equals(value),"KV corrupt");copies++;}}
        }
        return copies;
    }
    private static void count(Request r,int[] counts) {
        if(r.completed<0) {counts[7]++;return;}
        switch(r.status) {
            case OK -> counts[0]++;case BUSY -> counts[1]++;case TIMEOUT -> counts[2]++;case UNAVAILABLE -> counts[3]++;
            case STALE_EPOCH -> counts[4]++;case NOT_PRIMARY -> counts[5]++;default -> counts[6]++;
        }
    }
    private static boolean converged(ProtocolCluster c,SustainedLoadExperiment.Sample sample) {
        if(sample.pending()!=0 || sample.pendingDataBytes()!=0 || sample.kv1()!=sample.kv2()) return false;
        if(c.variant==ProtocolCluster.Variant.PB) return true;
        var first=c.observation(1);var second=c.observation(2);
        return first.state()==VrEngine.State.NORMAL && second.state()==VrEngine.State.NORMAL && first.view()==second.view()
                && first.logLength()==second.logLength() && first.commit()==first.logLength() && second.commit()==second.logLength()
                && first.applied()==first.logLength() && second.applied()==second.logLength();
    }
    public static Measurement run(Condition condition) {return run(condition,30000);}
    static Measurement run(Condition condition,int loadMs) {
        require(conditions(false).contains(condition) && loadMs>=6000 && loadMs%3000==0,"invalid condition/duration");
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.valueOf(condition.variant()),1,2000,1,condition.mibps()*NetworkCostExperiment.MIB)) {
            Collector collector=new Collector(c,condition,loadMs);
            c.scheduler.schedule(ProtocolCluster.ms(loadMs/3),()->{if(condition.scenario().equals("SLOW_CRASH")) c.slowAck=200;});
            c.scheduler.schedule(collector.crashAt,collector::crash);
            c.scheduler.schedule(ProtocolCluster.ms(loadMs*2L/3),()->c.slowAck=0);
            for(int at=0,operation=1;at<loadMs;at+=condition.intervalMs(),operation++) {int id=operation;long time=ProtocolCluster.ms(at);c.scheduler.schedule(time,()->collector.offer(id,time));}
            List<SustainedLoadExperiment.Sample> samples=new ArrayList<>();
            for(int at=0;at<=loadMs;at+=100) {c.until(ProtocolCluster.ms(at));samples.add(collector.sample());}
            int pendingAtStop=samples.getLast().pending(),during=0;
            for(Request r:collector.requests) if(r.status==ResultStatus.OK && r.completed<ProtocolCluster.ms(loadMs)) during++;
            long loadEnd=ProtocolCluster.ms(loadMs),limit=loadEnd+ProtocolCluster.ms(30000);
            boolean settled=converged(c,samples.getLast());
            while(!settled && c.now()<limit) {c.until(Math.min(limit,c.now()+ProtocolCluster.ms(100)));var sample=collector.sample();samples.add(sample);settled=converged(c,sample);}
            List<SustainedLoadExperiment.Phase> phases=new ArrayList<>();
            for(int phase=0;phase<2;phase++) {
                int[] counts=new int[8];int offered=0,completed=0;List<Long> latency=new ArrayList<>();
                long from=phase==0?0:collector.crashAt,to=phase==0?collector.crashAt:loadEnd;
                for(Request r:collector.requests) {
                    if(r.status==ResultStatus.OK && r.completed>=from && r.completed<to) completed++;
                    if(r.phase==phase) {offered++;count(r,counts);if(r.status==ResultStatus.OK) latency.add(r.completed-r.submitted);}
                }
                require(counts[6]==0,"unexpected request status");
                phases.add(new SustainedLoadExperiment.Phase(phase,offered,counts[0],counts[1],counts[2],counts[3],counts[4],counts[5],counts[6],counts[7],SustainedLoadExperiment.percentile(latency,50),SustainedLoadExperiment.percentile(latency,99),completed));
            }
            var logs=survivorLogs(c,collector.value);
            int retainedPre=0,retainedAll=0,preKv=0,allKv=0,inflightRetained=0,divergent=0;
            for(Request r:collector.requests) {
                int kvCopies=0;
                for(int node=1;node<=2;node++) {Result result=read(c,node,r.operation);if(result.status()==ResultStatus.OK) {require(result.value().orElseThrow().equals(collector.value),"KV value corrupt");kvCopies++;}else require(result.status()==ResultStatus.MISS,"unexpected KV status");}
                int retained=c.variant==ProtocolCluster.Variant.PB?kvCopies:(logs.get(0).contains((long)r.operation)?1:0)+(logs.get(1).contains((long)r.operation)?1:0);
                if(r.preCrashOk) {require(retained>0,"pre-crash OK lost from both survivors");retainedPre++;preKv+=kvCopies;}
                if(r.status==ResultStatus.OK) {require(retained>0,"acknowledged operation lost from survivors");retainedAll++;allKv+=kvCopies;}
                if(r.inflightAtCrash && retained>0) inflightRetained++;
                if(r.status==ResultStatus.BUSY || r.status==ResultStatus.STALE_EPOCH || r.status==ResultStatus.NOT_PRIMARY) require(kvCopies==0,"rejected unique request applied");
                if(r.submitted<collector.crashAt && r.status==ResultStatus.OK) require(r.completed<collector.crashAt,"old Primary returned OK after crash");
                if(kvCopies==1) divergent++;
            }
            require(retainedPre==collector.preOk && (!settled || divergent==0),"invalid preservation/convergence");
            return new Measurement(condition,loadMs,collector.crashAt,collector.firstPostOk,collector.preOk,collector.prePending,collector.evidenceAtCrash,retainedPre,retainedAll,preKv,allKv,inflightRetained,c.now()-loadEnd,settled,pendingAtStop,during,divergent,c.network.stats(),List.copyOf(phases),List.copyOf(samples));
        }
    }
}
