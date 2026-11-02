package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.vr.VrEngine;
import java.nio.ByteBuffer;
import java.util.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 고정 간격 open-loop 입력과 ACK 지연·복구. 재시도 없이 제출 결과와 완료 시각을 분리한다. */
public final class SustainedLoadExperiment {
    public static final int LOAD_MS=30000, DRAIN_MS=30000, SAMPLE_MS=100;
    public record Condition(String variant,String scenario,int intervalMs,int valueBytes,int mibps) {
        public String id() { return variant+"-"+scenario+"-i"+intervalMs+"-v"+valueBytes+"-b"+mibps; }
    }
    public record Phase(int phase,int offered,int ok,int busy,int timeout,int unavailable,int stale,int redirect,int other,int pending,long p50Ns,long p99Ns,int completedInWindow) {}
    public record Sample(long timeNs,int offered,int ok,int busy,int timeout,int unavailable,int stale,int redirect,int other,int pending,
            long pendingBytes,long pendingDataBytes,int logLag,int applyLag,long kv0,long kv1,long kv2,int viewAdvance) {}
    public record Measurement(Condition condition,int loadMs,long drainNs,boolean converged,int pendingAtLoadEnd,int okDuringLoad,
            int watermarkOk,int watermarkCommit,long catchupNs,int verifiedOkCopies,int missingOkCopies,int divergentKeys,
            LinkNetwork.Stats network,List<Phase> phases,List<Sample> samples) {}
    private static final class Request {
        final int operation,phase;
        final long submitted;
        long completed=-1;
        ResultStatus status;
        Request(int operation,int phase,long submitted) { this.operation=operation; this.phase=phase; this.submitted=submitted; }
    }
    private static final class Collector {
        final ProtocolCluster cluster;
        final Request[] requests;
        final int loadMs,phaseMs;
        final Bytes value;
        long epoch;
        int target,offered,watermarkOk,watermarkCommit=-1;
        long catchupNs=-1;
        boolean watermarkSet;
        Collector(ProtocolCluster cluster,Condition condition,int loadMs) {
            this.cluster=cluster; this.loadMs=loadMs; phaseMs=loadMs/3;
            requests=new Request[loadMs/condition.intervalMs()];
            byte[] bytes=new byte[condition.valueBytes()]; Arrays.fill(bytes,(byte)91); value=Bytes.copyOf(bytes);
            epoch=condition.variant().equals("PB")?1:0;
        }
        void offer(int operation,long at) {
            int phase=Math.min(2,(int)(at/ProtocolCluster.ms(phaseMs)));
            Request request=new Request(operation,phase,at); requests[operation-1]=request; offered++;
            var context=new RequestContext(new ClientId(19),new OperationId(operation),RoutingVersion.ZERO,new OwnershipEpoch(epoch),
                    new Deadline(cluster.now()+ProtocolCluster.ms(1000)));
            cluster.nodes.get(target).execute(new Command.Set(key(operation),value),context).whenComplete((result,error)->{
                request.completed=cluster.now();
                if(error!=null) { request.status=ResultStatus.INTERNAL_ERROR; return; }
                request.status=result.status();
                if(result.ownershipEpoch().value()>=epoch) {
                    epoch=result.ownershipEpoch().value();
                    if(result.status()==ResultStatus.NOT_PRIMARY) target=Math.toIntExact(result.redirect().orElseThrow().target().value());
                    else if(result.status()==ResultStatus.UNAVAILABLE) target=(target+1)%3;
                }
            });
        }
        void freezeWatermark() {
            watermarkSet=true;
            for(Request r:requests) if(r!=null && r.status==ResultStatus.OK) watermarkOk++;
            if(cluster.variant!=ProtocolCluster.Variant.PB)
                for(int i=0;i<3;i++) watermarkCommit=Math.max(watermarkCommit,cluster.observation(i).commit());
            else {
                // PB의 OK는 이미 Backup 2 적용 ACK를 포함한다.
                for(Request r:requests) if(r!=null && r.status==ResultStatus.OK) require(read(cluster,2,r.operation).status()==ResultStatus.OK,"PB acknowledged key missing at restoration");
                catchupNs=0;
            }
        }
        Sample sample() {
            int[] counts=new int[8];
            for(Request r:requests) if(r!=null) count(r,counts);
            long[] kv={countKv(cluster,0),countKv(cluster,1),countKv(cluster,2)};
            int logLag=-1,applyLag=-1,view=0;
            if(cluster.variant!=ProtocolCluster.Variant.PB) {
                int maxLog=0,maxCommit=0;
                for(int i=0;i<3;i++) { var o=cluster.observation(i); maxLog=Math.max(maxLog,o.logLength()); maxCommit=Math.max(maxCommit,o.commit()); view=Math.max(view,Math.toIntExact(o.view())); }
                logLag=maxLog-cluster.observation(2).logLength(); applyLag=maxCommit-cluster.observation(2).applied();
                if(watermarkSet && catchupNs<0 && cluster.observation(2).applied()>=watermarkCommit)
                    catchupNs=cluster.now()-ProtocolCluster.ms(phaseMs*2L);
            } else for(var node:cluster.nodes.values()) view=Math.max(view,Math.toIntExact(node.snapshot().ownershipEpoch().value())-1);
            return new Sample(cluster.now(),offered,counts[0],counts[1],counts[2],counts[3],counts[4],counts[5],counts[6],counts[7],
                    cluster.network.stats().pendingBytes(),cluster.network.pendingDataBytes,logLag,applyLag,kv[0],kv[1],kv[2],view);
        }
    }
    private SustainedLoadExperiment() {}
    public static List<Condition> conditions(boolean quick) {
        List<Condition> result=new ArrayList<>();
        for(String variant:List.of("PB","VR_INCREMENTAL")) for(String scenario:List.of("NORMAL","SLOW_RECOVER"))
            for(int interval:quick?new int[]{1,20}:new int[]{1,5,20}) for(int value:new int[]{64,8192})
                for(int rate:quick?new int[]{1}:new int[]{1,10}) result.add(new Condition(variant,scenario,interval,value,rate));
        return List.copyOf(result);
    }
    static Bytes key(int operation) { return Bytes.copyOf(ByteBuffer.allocate(16).putLong(19).putLong(operation).array()); }
    private static Result read(ProtocolCluster c,int node,int operation) {
        return c.stores.get(node).execute(new Command.Get(key(operation)),c.context(99999,node,1000)).toCompletableFuture().join();
    }
    private static long countKv(ProtocolCluster c,int node) {
        Result r=c.stores.get(node).execute(new Command.Count(),c.context(99999,node,1000)).toCompletableFuture().join();
        require(r.status()==ResultStatus.OK,"COUNT failed"); return ByteBuffer.wrap(r.value().orElseThrow().toByteArray()).getLong();
    }
    private static void count(Request request,int[] counts) {
        if(request.completed<0) { counts[7]++; return; }
        switch(request.status) {
            case OK -> counts[0]++;
            case BUSY -> counts[1]++;
            case TIMEOUT -> counts[2]++;
            case UNAVAILABLE -> counts[3]++;
            case STALE_EPOCH -> counts[4]++;
            case NOT_PRIMARY -> counts[5]++;
            default -> counts[6]++;
        }
    }
    static long percentile(List<Long> samples,int percent) {
        if(samples.isEmpty()) return -1;
        var sorted=new ArrayList<>(samples); sorted.sort(Long::compare);
        return sorted.get((int)(((long)sorted.size()*percent+99)/100)-1);
    }
    private static boolean converged(ProtocolCluster c,Sample s) {
        if(s.pending()!=0 || s.pendingDataBytes()!=0 || s.kv0()!=s.kv1() || s.kv1()!=s.kv2()) return false;
        if(c.variant==ProtocolCluster.Variant.PB) return true;
        var first=c.observation(0);
        for(int i=0;i<3;i++) {
            var o=c.observation(i);
            if(o.state()!=VrEngine.State.NORMAL || o.view()!=first.view() || o.logLength()!=first.logLength()
                    || o.applied()!=o.logLength() || o.commit()!=o.logLength()) return false;
        }
        return true;
    }
    public static Measurement run(Condition condition) { return run(condition,LOAD_MS); }
    static Measurement run(Condition condition,int loadMs) {
        require(conditions(false).contains(condition) && loadMs>=3000 && loadMs%300==0 && loadMs%condition.intervalMs()==0,"invalid condition/duration");
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.valueOf(condition.variant()),1,2000,1,condition.mibps()*NetworkCostExperiment.MIB)) {
            Collector collector=new Collector(c,condition,loadMs); int phaseMs=loadMs/3;
            c.scheduler.schedule(ProtocolCluster.ms(phaseMs),()->{if(condition.scenario().equals("SLOW_RECOVER")) c.slowAck=200;});
            c.scheduler.schedule(ProtocolCluster.ms(phaseMs*2L),()->{c.slowAck=0;collector.freezeWatermark();});
            for(int at=0,operation=1;at<loadMs;at+=condition.intervalMs(),operation++) {
                int id=operation; long time=ProtocolCluster.ms(at);
                c.scheduler.schedule(time,()->collector.offer(id,time));
            }
            List<Sample> samples=new ArrayList<>();
            for(int at=0;at<=loadMs;at+=SAMPLE_MS) { c.until(ProtocolCluster.ms(at)); samples.add(collector.sample()); }
            int pendingAtStop=samples.getLast().pending();
            int duringLoad=0; for(Request r:collector.requests) if(r.status==ResultStatus.OK && r.completed<ProtocolCluster.ms(loadMs)) duringLoad++;
            boolean settled=converged(c,samples.getLast());
            long loadEnd=ProtocolCluster.ms(loadMs),limit=loadEnd+ProtocolCluster.ms(DRAIN_MS);
            while(!settled && c.now()<limit) {
                c.until(Math.min(limit,c.now()+ProtocolCluster.ms(SAMPLE_MS)));
                var sample=collector.sample(); samples.add(sample); settled=converged(c,sample);
            }
            List<Phase> phases=new ArrayList<>();
            for(int phase=0;phase<3;phase++) {
                int[] counts=new int[8]; int offered=0,window=0; List<Long> latencies=new ArrayList<>();
                long windowStart=ProtocolCluster.ms(phase*(long)phaseMs),windowEnd=windowStart+ProtocolCluster.ms(phaseMs);
                for(Request r:collector.requests) {
                    if(r.status==ResultStatus.OK && r.completed>=windowStart && r.completed<windowEnd) window++;
                    if(r.phase==phase) {
                        offered++; count(r,counts);
                        if(r.status==ResultStatus.OK) latencies.add(r.completed-r.submitted);
                    }
                }
                require(counts[6]==0,"unexpected request status");
                phases.add(new Phase(phase,offered,counts[0],counts[1],counts[2],counts[3],counts[4],counts[5],counts[6],counts[7],percentile(latencies,50),percentile(latencies,99),window));
            }
            int verified=0,missing=0,divergent=0;
            for(Request r:collector.requests) {
                int copies=0;
                for(int node=0;node<3;node++) {
                    Result result=read(c,node,r.operation);
                    if(result.status()==ResultStatus.OK) {
                        require(result.value().orElseThrow().equals(collector.value),"corrupted value"); copies++;
                    } else require(result.status()==ResultStatus.MISS,"unexpected audit status");
                }
                if(r.status==ResultStatus.OK) { require(copies>0,"lost acknowledged write"); verified+=copies; missing+=3-copies; }
                if(r.status==ResultStatus.BUSY || r.status==ResultStatus.NOT_PRIMARY || r.status==ResultStatus.STALE_EPOCH)
                    require(copies==0,"rejected unique request applied");
                if(copies!=0 && copies!=3) divergent++;
            }
            require(!settled || divergent==0,"declared convergence with unequal key sets");
            return new Measurement(condition,loadMs,c.now()-loadEnd,settled,pendingAtStop,duringLoad,collector.watermarkOk,collector.watermarkCommit,
                    collector.catchupNs,verified,missing,divergent,c.network.stats(),List.copyOf(phases),List.copyOf(samples));
        }
    }
}
