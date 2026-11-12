package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.replication.vr.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 같은 실행기·입력·장애로 PB 기준형과 VR을 비교한다. 시간 단위는 가상 ms다. */
public final class ProtocolComparison {
    public enum Scenario { NORMAL, SLOW_BACKUP, CRASH, PRIMARY_PARTITION, BACKUP_PARTITION, PARTIAL_CRASH, PARTIAL_PARTITION, LOST_ACK }
    public record Measurement(String variant, String scenario, int delay, int seed, long responseMs,
            long recoveryMs, int ok, int timeout, int divergent, int preserved, int messages, int logEntries, String trace) {}
    private ProtocolComparison() {}
    private static boolean ack(Object m) { return m instanceof PrimaryBackupMessage.ReplicateAck || m instanceof VrMessage.PrepareOk; }
    private static boolean data(Object m) { return m instanceof PrimaryBackupMessage.Replicate || m instanceof VrMessage.Prepare || m instanceof VrMessage.DeltaPrepare; }

    public static Measurement run(String variant, Scenario scenario, int delay, int seed) {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.valueOf(variant),delay,100,seed)) {
            c.jitter=3;
            for(int i=1;i<=2;i++) {
                var f=c.set(0,i,"marker-"+i,"saved-"+i,1000); c.advance(30);
                require(f.isDone() && f.join().status()==ResultStatus.OK,"warmup marker failed");
            }
            var initial=c.set(0,3,"x","old",1000); c.advance(30);
            require(initial.isDone() && initial.join().status()==ResultStatus.OK,"warmup x failed");
            c.advance(seed % 7);
            long start=c.nowMs(), response=-1, recovery=-1;
            int ok=0,timeouts=0; String audit="target"; List<Integer> audited=List.of(0,1,2);
            if(scenario==Scenario.NORMAL || scenario==Scenario.SLOW_BACKUP || scenario==Scenario.BACKUP_PARTITION) {
                if(scenario==Scenario.SLOW_BACKUP) c.slowAck=100;
                if(scenario==Scenario.BACKUP_PARTITION) c.isolate(2);
                var f=c.set(0,100,audit,"new",60); long[] at={-1}; f.thenAccept(r->at[0]=c.nowMs());
                c.advance(170); require(f.isDone(),"request unfinished");
                boolean expectedSuccess=!(variant.equals("PB") && scenario!=Scenario.NORMAL);
                require(f.join().status()==(expectedSuccess?ResultStatus.OK:ResultStatus.TIMEOUT),"unexpected availability outcome");
                if(expectedSuccess) ok++; else timeouts++;
                response=at[0]-start;
                // 격리된 Backup은 미확정이 아니라 지연된 복제본이다. 연결된 노드 0·1을 비교한다.
                if(scenario==Scenario.BACKUP_PARTITION) audited=List.of(0,1);
            } else {
                CompletableFuture<Result> pending=null;
                if(scenario==Scenario.PARTIAL_CRASH || scenario==Scenario.PARTIAL_PARTITION || scenario==Scenario.LOST_ACK) {
                    boolean partial=scenario!=Scenario.LOST_ACK;
                    c.drop=e->ack(e.message()) || (partial && e.to().value()==1 && data(e.message()));
                    pending=c.set(0,100,"x","pending",60);
                    c.advance(delay+5); require(!pending.isDone(),"pending request unexpectedly completed");
                }
                if(scenario==Scenario.PRIMARY_PARTITION || scenario==Scenario.PARTIAL_PARTITION) c.isolate(0); else c.crash(0);
                long fault=c.nowMs(); c.drop=e->false;
                recovery=retryAfterFault(c,200,"after","recovered",fault);
                ok=1; c.advance(40); audited=List.of(1,2);
                for(int id:audited) require(value(c,id,"after").equals("recovered"),"successful recovered write lost");
                audit=pending==null?"after":"x";
                if(pending!=null) {
                    require(pending.isDone() && pending.join().status()!=ResultStatus.OK,"uncertain write reported success");
                    if(pending.join().status()==ResultStatus.TIMEOUT) timeouts++;
                    if(!variant.equals("PB")) for(int id:audited) require(value(c,id,"x").equals("pending"),"view change lost accepted suffix");
                }
            }
            int preserved=0;
            for(int id:audited) for(int i=1;i<=2;i++) {
                require(value(c,id,"marker-"+i).equals("saved-"+i),"acknowledged marker lost"); preserved++;
            }
            Set<String> values=new HashSet<>(); for(int id:audited) values.add(value(c,id,audit));
            int divergent=values.size()>1?1:0;
            boolean expectedDivergence=variant.equals("PB") && (scenario==Scenario.SLOW_BACKUP || scenario==Scenario.BACKUP_PARTITION
                    || scenario==Scenario.PARTIAL_CRASH || scenario==Scenario.PARTIAL_PARTITION);
            require(divergent==(expectedDivergence?1:0),"unexpected replica agreement");
            for(int id:audited) c.events.add("audit node="+id+" key="+audit+" value="+value(c,id,audit));
            if(!variant.equals("PB")) for(int id=0;id<3;id++) c.events.add("state node="+id+" "+c.observation(id));
            return new Measurement(variant,scenario.name(),delay,seed,response,recovery,ok,timeouts,divergent,preserved,
                    c.messages,c.logEntries,String.join(" ; ",c.events));
        }
    }
    private static String value(ProtocolCluster c,int node,String key) {
        Result r=c.local(node,key);
        return r.status()==ResultStatus.MISS?"MISSING":new String(r.value().orElseThrow().toByteArray(),java.nio.charset.StandardCharsets.UTF_8);
    }
    /** 접속 가능 노드 목록은 알고 있지만 새 Primary/epoch는 응답에서 발견한다. 순차 재시도 간격 5ms. */
    private static long retryAfterFault(ProtocolCluster c,long operation,String key,String value,long fault) {
        int target=2; long epoch=c.variant==ProtocolCluster.Variant.PB?1:0;
        CompletableFuture<Result> f=null; long[] completed={-1};
        for(int attempt=0;attempt<400;attempt++) {
            if(f==null) {
                var context=new RequestContext(new ClientId(7),new OperationId(operation),RoutingVersion.ZERO,new OwnershipEpoch(epoch),new Deadline(c.now()+ProtocolCluster.ms(60)));
                f=c.nodes.get(target).execute(new Command.Set(Bytes.utf8(key),Bytes.utf8(value)),context).toCompletableFuture();
                int atNode=target; f.thenAccept(r->{ c.events.add(c.nowMs()+": retry node="+atNode+" status="+r.status()); if(r.status()==ResultStatus.OK) completed[0]=c.nowMs(); });
            }
            if(f.isDone()) {
                Result r=f.join(); if(r.status()==ResultStatus.OK) return completed[0]-fault;
                epoch=Math.max(epoch,r.ownershipEpoch().value());
                if(r.status()==ResultStatus.NOT_PRIMARY) target=Math.toIntExact(r.redirect().orElseThrow().target().value());
                else if(target==0 && (r.status()==ResultStatus.TIMEOUT || r.status()==ResultStatus.UNAVAILABLE || r.status()==ResultStatus.BUSY)) target=2;
                f=null;
            }
            c.advance(5);
        }
        throw new IllegalStateException("client never recovered");
    }
}
