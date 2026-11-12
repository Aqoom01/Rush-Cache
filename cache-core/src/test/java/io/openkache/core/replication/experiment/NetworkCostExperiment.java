package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 준비된 동일 이력에서 대역폭을 활성화한 뒤 새 SET 또는 Primary crash를 관측한다. */
public final class NetworkCostExperiment {
    public static final long MIB=1_048_576;
    public static final int LIMIT_MS=5000;
    public record Condition(String variant,String scenario,int history,int valueBytes,int mibps,int timeoutMs) {
        public String id() { return variant+"-"+scenario+"-h"+history+"-v"+valueBytes+"-b"+mibps+"-t"+timeoutMs; }
    }
    /** callback 지연과 관측 구간·큐 대기는 ns, bytes는 논리 frame의 바이트 수다. -1은 미완료/해당 없음. */
    public record Measurement(Condition condition,String outcome,long responseNs,long recoveryNs,long observedNs,int attempts,
            int preservedCopies,int probeCopies,int maxViewAdvance,LinkNetwork.Stats network) {}
    private NetworkCostExperiment() {}
    public static List<Condition> conditions(boolean quick) {
        return conditions(quick,false);
    }
    public static List<Condition> conditions(boolean quick,boolean optimized) {
        List<Condition> cases=new ArrayList<>();
        for(String variant:List.of("PB",optimized?"VR_INCREMENTAL":"VR")) for(String scenario:List.of("NORMAL","CRASH"))
            for(int history:quick?new int[]{1,64}:new int[]{1,16,64})
                for(int value:quick?new int[]{64,8192}:new int[]{64,1024,8192})
                    for(int bandwidth:quick?new int[]{1,100}:new int[]{1,10,100})
                        for(int timeout:new int[]{100,2000}) cases.add(new Condition(variant,scenario,history,value,bandwidth,timeout));
        return List.copyOf(cases);
    }
    static Bytes key(int operation) { return Bytes.copyOf(ByteBuffer.allocate(16).putLong(7).putLong(operation).array()); }
    static Bytes value(int operation,int bytes) { byte[] data=new byte[bytes]; Arrays.fill(data,(byte)operation); return Bytes.copyOf(data); }
    private static Command.Set set(int operation,int size) { return new Command.Set(key(operation),value(operation,size)); }
    private static Result local(ProtocolCluster c,int node,int operation) {
        return c.stores.get(node).execute(new Command.Get(key(operation)),c.context(99999,node,1000)).toCompletableFuture().join();
    }
    public static Measurement run(Condition condition) {
        require(conditions(false,condition.variant().equals("VR_INCREMENTAL")).contains(condition),"unsupported condition");
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.valueOf(condition.variant()),1,condition.timeoutMs(),1,0)) {
            // 이력 크기의 영향을 분리하기 위해 준비 단계는 serialization 비용이 0이다.
            for(int i=1;i<=condition.history();i++) {
                var f=c.submit(0,i,set(i,condition.valueBytes()),1000);
                long bound=c.now()+ProtocolCluster.ms(20);
                while(!f.isDone() && c.now()<bound) c.advance(1);
                require(f.isDone() && f.join().status()==ResultStatus.OK,"history preparation failed");
                c.until(ProtocolCluster.ms((c.nowMs()/10+1)*10+9));
            }
            require(c.network.stats().pendingBytes()==0,"bootstrap network not idle");
            if(!condition.variant().equals("PB")) for(int node=0;node<3;node++)
                require(c.observation(node).logLength()==condition.history() && c.observation(node).applied()==condition.history(),"history not committed everywhere");
            c.network.beginMeasurement(condition.mibps()*MIB);
            long start=c.now(); int operation=condition.history()+1,attempts=0;
            long response=-1,recovery=-1; String outcome="CENSORED";
            if(condition.scenario().equals("NORMAL")) {
                var f=c.submit(0,operation,set(operation,condition.valueBytes()),2000); attempts++;
                long[] at={-1}; f.thenAccept(r->at[0]=c.now());
                while(!f.isDone() && c.now()-start<ProtocolCluster.ms(LIMIT_MS)) c.advance(1);
                if(f.isDone()) { outcome=f.join().status().name(); response=at[0]-start; }
            } else {
                c.crash(0);
                int target=2; long epoch=condition.variant().equals("PB")?1:0;
                CompletableFuture<Result> pending=null; long nextAttempt=start; long[] at={-1};
                while(c.now()-start<ProtocolCluster.ms(LIMIT_MS)) {
                    if(pending==null && c.now()>=nextAttempt) {
                        var context=new RequestContext(new ClientId(7),new OperationId(operation),RoutingVersion.ZERO,
                                new OwnershipEpoch(epoch),new Deadline(c.now()+ProtocolCluster.ms(2000)));
                        pending=c.nodes.get(target).execute(set(operation,condition.valueBytes()),context).toCompletableFuture(); attempts++;
                        pending.thenAccept(r->{if(r.status()==ResultStatus.OK) at[0]=c.now();});
                    }
                    if(pending!=null && pending.isDone()) {
                        Result r=pending.join();
                        if(r.status()==ResultStatus.OK) { outcome="OK"; recovery=at[0]-start; break; }
                        epoch=Math.max(epoch,r.ownershipEpoch().value());
                        if(r.status()==ResultStatus.NOT_PRIMARY) target=Math.toIntExact(r.redirect().orElseThrow().target().value());
                        else if(target==0) target=2;
                        nextAttempt=c.now()+ProtocolCluster.ms(5); pending=null;
                    }
                    c.advance(1);
                }
            }
            int preserved=0,probeCopies=0,maxView=0;
            for(int node=0;node<3;node++) {
                maxView=Math.max(maxView,Math.toIntExact(c.nodes.get(node).snapshot().ownershipEpoch().value())-(condition.variant().equals("PB")?1:0));
                if(c.dead.contains(node)) continue;
                for(int i=1;i<=condition.history();i++) {
                    Result r=local(c,node,i);
                    require(r.status()==ResultStatus.OK && r.value().orElseThrow().equals(value(i,condition.valueBytes())),"committed history changed"); preserved++;
                }
                Result probe=local(c,node,operation);
                if(probe.status()==ResultStatus.OK) {
                    require(probe.value().orElseThrow().equals(value(operation,condition.valueBytes())),"probe value mismatch"); probeCopies++;
                } else require(probe.status()==ResultStatus.MISS,"unexpected audit result");
            }
            require(!outcome.equals("OK") || probeCopies>0,"acknowledged probe missing");
            return new Measurement(condition,outcome,response,recovery,c.now()-start,attempts,preserved,probeCopies,maxView,c.network.stats());
        }
    }
}
