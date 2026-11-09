package io.openkache.core.replication.experiment;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.vr.VrEngine;
import io.openkache.core.replication.vr.VrMessage;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** INCREMENTAL_CATCHUP이 같은 원인 조건에서 전체 로그 전송을 없애고 값·view를 보존하는지 INCREMENTAL과 대조한다. */
class VrCatchupTest {
    private static long count(ProtocolCluster c,int node,String name) {
        return ((VrEngine)c.nodes.get(node)).trafficCounts().get(name);
    }
    private static void allValues(ProtocolCluster c,int entries) {
        for(int n=0;n<3;n++) {
            assertEquals(0,c.observation(n).view());assertEquals(entries,c.observation(n).applied());
            for(int i=1;i<=entries;i++) assertEquals(Bytes.utf8("value"),c.local(n,"k-"+i).value().orElseThrow());
        }
    }
    @Test void delayedBackupAckNoLongerCausesCommitGapOrFullSnapshot() {
        for(var variant:new ProtocolCluster.Variant[]{ProtocolCluster.Variant.VR_INCREMENTAL,ProtocolCluster.Variant.VR_CATCHUP})
        try(var c=new ProtocolCluster(variant,1,100,1)) {
            // 7단계와 같은 재현 조건: 유실·순서 변경 없이 Backup2의 ACK만 40ms 늦춘다.
            c.slowAck=40;
            for(int i=1;i<=12;i++) {
                var f=c.set(0,i,"k-"+i,"value",1000);c.advance(6);
                assertEquals(ResultStatus.OK,f.join().status());
            }
            boolean catchup=variant==ProtocolCluster.Variant.VR_CATCHUP;
            assertEquals(catchup?0:1,Math.min(1,count(c,2,"GET_STATE_COMMIT_GAP")),variant.name());
            assertEquals(catchup?0:1,Math.min(1,count(c,0,"SNAPSHOT_SEND")),variant.name());
            if(catchup) {
                assertTrue(count(c,0,"COMMIT_BOUNDED")>0);
                assertEquals(0,count(c,2,"GET_STATE_SEND"));assertEquals(0,count(c,0,"SNAPSHOT_UPGRADE"));
            } else assertEquals(0,count(c,0,"COMMIT_BOUNDED"));
            c.slowAck=0;c.advance(1500);
            allValues(c,12);
            if(catchup) assertEquals(0,count(c,0,"SNAPSHOT_SEND")+count(c,0,"GET_STATE_RECEIVED"));
        }
    }
    @Test void boundedCommitNeverExceedsPrimaryCommitAndBackupNeverAppliesUncommitted() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_CATCHUP,1,100,3)) {
            c.slowAck=40;c.jitter=3;
            int[] maxNotice={-1};
            // drop 검사는 송신·전달 시 두 번 호출된다. Primary observation은 송신 뒤 publish되므로 전달 시점에만 대조한다.
            var sent=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<io.openkache.core.spi.PeerEnvelope,Boolean>());
            c.drop=e->{
                if(sent.add(e)) return false;
                if(e.message() instanceof VrMessage.Commit notice && e.from().value()==0) {
                    assertTrue(notice.commit()<=c.observation(0).commit(),"notice above primary commit");
                    maxNotice[0]=Math.max(maxNotice[0],notice.commit());
                }
                for(int n=1;n<3;n++) assertTrue(c.observation(n).applied()<=c.observation(0).commit(),"backup applied beyond commit");
                return false;
            };
            for(int i=1;i<=24;i++) {
                // 지연 1ms + jitter 3ms 왕복이므로 6ms 안에 끝나지 않을 수 있다. 가상 시간만 진행하므로 join으로 기다리지 않는다.
                var f=c.set(0,i,"k-"+i,"value",1000);
                for(int waited=0;!f.isDone() && waited<100;waited+=2) c.advance(2);
                assertTrue(f.isDone(),"write "+i+" did not finish");assertEquals(ResultStatus.OK,f.join().status());
            }
            c.slowAck=0;c.jitter=0;c.advance(1500);
            assertTrue(maxNotice[0]>0);allValues(c,24);
            assertEquals(0,count(c,0,"SNAPSHOT_SEND"));
        }
    }
    @Test void lostDeltaRecoversIncrementallyThroughRetryWithoutFullLog() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_CATCHUP,1,100,1)) {
            var first=c.set(0,1,"k-1","value",1000);c.advance(10);assertEquals(ResultStatus.OK,first.join().status());
            // Backup2로 가는 두 번째 DeltaPrepare만 한 번 잃는다. Commit은 전송 범위(2)까지 알리므로 gap이 생긴다.
            int[] dropped={0};
            c.drop=e->e.to().value()==2 && e.message() instanceof VrMessage.DeltaPrepare && dropped[0]++==0;
            var second=c.set(0,2,"k-2","value",1000);c.advance(10);assertEquals(ResultStatus.OK,second.join().status());
            c.advance(100);
            assertEquals(1,dropped[0]);assertEquals(1,c.observation(2).logLength());
            assertTrue(count(c,2,"GET_STATE_COMMIT_GAP")>0);assertTrue(count(c,0,"DELTA_CATCHUP_REQUEST")>0);
            assertTrue(count(c,0,"DELTA_CATCHUP_THROTTLED")>0);
            assertEquals(0,count(c,0,"SNAPSHOT_SEND"));assertEquals(0,count(c,0,"SNAPSHOT_UPGRADE"));
            c.advance(1500);
            assertTrue(count(c,0,"TRANSFER_RESEND")>0);assertEquals(0,count(c,0,"SNAPSHOT_SEND"));
            allValues(c,2);
        }
    }
    @Test void emptyAcknowledgedPrefixIsCaughtUpIncrementallyNotWithFullLog() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_CATCHUP,1,100,1)) {
            // heartbeat Commit(0)에 답한 Backup1은 길이0 prefix를 확인한 상태다. 첫 DeltaPrepare를 잃어도 전체 로그가 아닌 증분으로 따라잡는다.
            c.advance(30);assertTrue(count(c,0,"COMMIT_SEND")>0);
            c.drop=e->e.to().value()==1 && (e.message() instanceof VrMessage.DeltaPrepare || e.message() instanceof VrMessage.StartView);
            var first=c.set(0,1,"x","one",1000);c.advance(10);assertEquals(ResultStatus.OK,first.join().status());
            c.drop=e->false;
            c.nodes.get(1).onPeerMessage(new NodeId(0),new VrMessage.Commit(0,1));
            c.advance(10);
            assertTrue(count(c,1,"GET_STATE_COMMIT_GAP")>0);assertTrue(count(c,0,"DELTA_CATCHUP_REQUEST")>0);
            assertEquals(0,count(c,0,"SNAPSHOT_GET_STATE_REQUEST"));assertEquals(0,count(c,0,"SNAPSHOT_SEND"));
            c.advance(1200);assertEquals(Bytes.utf8("one"),c.local(1,"x").value().orElseThrow());
            assertEquals(0,count(c,0,"SNAPSHOT_SEND"));assertEquals(0,c.observation(1).view());
        }
    }
    @Test void stateRequestFromAnotherViewStillReceivesFullState() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_CATCHUP,1,100,1)) {
            var first=c.set(0,1,"x","one",1000);c.advance(10);assertEquals(ResultStatus.OK,first.join().status());
            // view가 다르거나 이 view에서 확인한 prefix가 없는 요청은 기존 전체 로그 경로를 유지한다.
            c.nodes.get(0).onPeerMessage(new NodeId(1),new VrMessage.GetState(0));
            c.advance(5);assertEquals(1,count(c,0,"DELTA_CATCHUP_REQUEST"));
            ((VrEngine)c.nodes.get(0)).onPeerMessage(new NodeId(2),new VrMessage.GetState(7));
            c.advance(5);assertEquals(1,count(c,0,"SNAPSHOT_GET_STATE_REQUEST"));assertEquals(1,count(c,0,"SNAPSHOT_SEND"));
        }
    }
    @Test void catchupModePassesSameProtocolScenarioMatrix() {
        for(var scenario:ProtocolComparison.Scenario.values()) for(int delay:new int[]{1,5}) for(int seed=1;seed<=5;seed++)
            ProtocolComparison.run("VR_CATCHUP",scenario,delay,seed);
    }
}
