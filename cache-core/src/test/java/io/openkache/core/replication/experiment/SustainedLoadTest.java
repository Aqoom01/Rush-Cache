package io.openkache.core.replication.experiment;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage;
import io.openkache.core.replication.vr.VrMessage;
import io.openkache.core.spi.PeerEnvelope;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SustainedLoadTest {
    @Test void delayedAckRestorationDoesNotReleaseAlreadyDelayedFramesEarly() {
        var s=new ReplicationExperiment.Scheduler(); var n=new LinkNetwork(s,1,NetworkCostExperiment.MIB);
        List<Long> operations=new ArrayList<>();
        n.send(new PeerEnvelope(new NodeId(2),new NodeId(0),new PrimaryBackupMessage.ReplicateAck(new OwnershipEpoch(1),1)),()->true,()->operations.add(1L),200);
        n.send(new PeerEnvelope(new NodeId(2),new NodeId(0),new PrimaryBackupMessage.ReplicateAck(new OwnershipEpoch(1),2)),()->true,()->operations.add(2L),0);
        assertEquals(80,n.stats().pendingBytes()); assertEquals(0,n.pendingDataBytes);
        s.drain(); assertEquals(List.of(2L,1L),operations); assertEquals(80,n.stats().deliveredBytes());
    }
    @Test void nearestRankPercentilesUseOnlyActualSuccessfulSamples() {
        assertEquals(-1,SustainedLoadExperiment.percentile(List.of(),99));
        assertEquals(2,SustainedLoadExperiment.percentile(List.of(5L,1L,2L,3L),50));
        assertEquals(5,SustainedLoadExperiment.percentile(List.of(5L,1L,2L,3L),99));
    }
    @Test void fixedOfferedScheduleIsNotReducedByBusyAndAllUniqueRejectedWritesStayAbsent() {
        var m=SustainedLoadExperiment.run(new SustainedLoadExperiment.Condition("PB","NORMAL",1,64,1),3000);
        assertEquals(3000,m.phases().stream().mapToInt(SustainedLoadExperiment.Phase::offered).sum());
        assertTrue(m.phases().stream().mapToInt(SustainedLoadExperiment.Phase::busy).sum()>0);
        assertTrue(m.converged()); assertEquals(0,m.missingOkCopies()); assertEquals(0,m.divergentKeys());
        assertTrue(m.phases().stream().allMatch(p->p.offered()==1000 && p.ok()+p.busy()+p.timeout()+p.unavailable()+p.stale()+p.redirect()+p.other()+p.pending()==p.offered()));
    }
    @Test void slowAckSeparatesQuorumProgressFromAllBackupAcknowledgmentsAndReplays() {
        for(String variant:List.of("PB","VR_INCREMENTAL")) {
            var condition=new SustainedLoadExperiment.Condition(variant,"SLOW_RECOVER",5,64,1);
            var m=SustainedLoadExperiment.run(condition,3000);
            assertEquals(m,SustainedLoadExperiment.run(condition,3000));
            assertTrue(m.converged()); assertEquals(0,m.missingOkCopies());
            if(variant.equals("PB")) assertTrue(m.phases().get(1).p50Ns()>=ProtocolCluster.ms(200));
            else assertTrue(m.phases().get(1).p50Ns()<ProtocolCluster.ms(200));
        }
    }
    @Test void snapshotReplacementRebuildsPendingRequestIndex() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,100,1)) {
            var pending=c.set(0,1,"x","uncertain",1000);
            c.nodes.get(0).onPeerMessage(new NodeId(1),new VrMessage.StartViewChange(1));
            c.nodes.get(0).onPeerMessage(new NodeId(1),new VrMessage.StartView(1,List.of(),0));
            assertEquals(ResultStatus.UNAVAILABLE,pending.join().status());
            c.nodes.get(0).onPeerMessage(new NodeId(1),new VrMessage.DoViewChange(3,1,List.of(),0));
            c.nodes.get(0).onPeerMessage(new NodeId(2),new VrMessage.StartViewChange(3));
            c.nodes.get(0).onPeerMessage(new NodeId(2),new VrMessage.PrepareOk(3,0));
            var retry=c.set(0,1,"x","replacement",1000);
            assertFalse(retry.isDone());
            c.nodes.get(0).onPeerMessage(new NodeId(1),new VrMessage.PrepareOk(3,1));
            assertEquals(ResultStatus.OK,retry.join().status());
            assertEquals(Bytes.utf8("replacement"),c.local(0,"x").value().orElseThrow());
        }
    }
}
