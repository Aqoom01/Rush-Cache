package io.openkache.core.replication.experiment;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.replication.vr.VrEngine;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class VrTrafficCauseTest {
    private static long count(ProtocolCluster c,int node,String name) {
        return ((VrEngine)c.nodes.get(node)).trafficCounts().get(name);
    }
    @Test void orderedHealthySequentialWritesNeedNoSnapshot() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,100,1)) {
            for(int i=1;i<=16;i++) {
                var f=c.set(0,i,"k-"+i,"value",1000);c.advance(10);
                assertEquals(ResultStatus.OK,f.join().status());
            }
            c.advance(30);
            for(int n=0;n<3;n++) {
                assertEquals(0,count(c,n,"GET_STATE_SEND"));assertEquals(0,count(c,n,"SNAPSHOT_SEND"));
                assertEquals(0,c.observation(n).view());assertEquals(16,c.observation(n).applied());
            }
        }
    }
    @Test void delayedBackupAckAloneTriggersCommitGapAndFullSnapshot() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,100,1)) {
            // 메시지를 버리거나 순서를 바꾸지 않는다. Backup2의 ACK만 40ms 늦춘다.
            c.slowAck=40;
            for(int i=1;i<=12;i++) {
                var f=c.set(0,i,"k-"+i,"value",1000);c.advance(6);
                assertEquals(ResultStatus.OK,f.join().status());
            }
            assertTrue(count(c,2,"GET_STATE_COMMIT_GAP")>0);
            assertTrue(count(c,0,"SNAPSHOT_GET_STATE_REQUEST")>0);
            assertTrue(count(c,0,"SNAPSHOT_UPGRADE")>0);
            assertTrue(count(c,0,"SNAPSHOT_SEND")>0);
            assertEquals(0,count(c,0,"SNAPSHOT_VIEW_INSTALL_REQUEST"));
            c.slowAck=0;c.advance(1500);
            for(int n=0;n<3;n++) {
                assertEquals(0,c.observation(n).view());assertEquals(12,c.observation(n).applied());
                for(int i=1;i<=12;i++) assertEquals(ResultStatus.OK,c.local(n,"k-"+i).status());
            }
        }
    }
}
