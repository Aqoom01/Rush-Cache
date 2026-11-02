package io.openkache.core.replication.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.replication.vr.VrMessage;
import io.openkache.core.spi.PeerEnvelope;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NetworkCostTest {
    @Test void modeledBytesIncludeActualKeyValueLogAndControlFields() {
        var set=new Command.Set(Bytes.copyOf(new byte[16]),Bytes.copyOf(new byte[64]));
        var request=new VrMessage.Request(new ClientId(7),new OperationId(1),set);
        assertEquals(129,LogicalMessageSize.of(new PrimaryBackupMessage.Replicate(new OwnershipEpoch(1),1,set)).bytes());
        assertEquals(149,LogicalMessageSize.of(new VrMessage.Prepare(0,List.of(request),0)).bytes());
        assertEquals(254,LogicalMessageSize.of(new VrMessage.StartView(1,List.of(request,request),0)).bytes());
        assertEquals(157,LogicalMessageSize.of(new VrMessage.DoViewChange(1,0,List.of(request),0)).bytes());
        assertEquals(40,LogicalMessageSize.of(new VrMessage.PrepareOk(0,1)).bytes());
        assertEquals(32,LogicalMessageSize.of(new VrMessage.StartViewChange(1)).bytes());
        assertEquals(32,LogicalMessageSize.of(new VrMessage.GetState(1)).bytes());
        assertEquals(36,LogicalMessageSize.of(new CrashOnlyPrimaryBackupEngine.Control(new OwnershipEpoch(1),CrashOnlyPrimaryBackupEngine.Kind.HEARTBEAT)).bytes());
    }
    @Test void sameDirectionQueuesAndReverseDirectionTransmitsInParallel() {
        var s=new ReplicationExperiment.Scheduler(); var n=new LinkNetwork(s,1,1000);
        var ack=new PrimaryBackupMessage.ReplicateAck(new OwnershipEpoch(1),1);
        List<Long> at=new ArrayList<>();
        n.send(new PeerEnvelope(new NodeId(0),new NodeId(1),ack),()->true,()->at.add(s.time.monotonicNanos()));
        n.send(new PeerEnvelope(new NodeId(0),new NodeId(1),ack),()->true,()->at.add(s.time.monotonicNanos()));
        n.send(new PeerEnvelope(new NodeId(1),new NodeId(0),ack),()->true,()->at.add(s.time.monotonicNanos()));
        assertEquals(120,n.stats().pendingBytes()); s.drain();
        assertEquals(List.of(ProtocolCluster.ms(41),ProtocolCluster.ms(41),ProtocolCluster.ms(81)),at);
        assertEquals(ProtocolCluster.ms(40),n.stats().maxQueueWaitNs());
        assertEquals(120,n.stats().deliveredBytes()); assertEquals(0,n.stats().pendingBytes());
        assertEquals(1,LinkNetwork.serializationNs(1,2_000_000_000L));
    }
    @Test void droppedAndPendingFramesBalanceBytesAndCannotResetNonemptyQueue() {
        var s=new ReplicationExperiment.Scheduler(); var n=new LinkNetwork(s,1,1000);
        var e=new PeerEnvelope(new NodeId(0),new NodeId(1),new VrMessage.PrepareOk(0,1));
        boolean[] connected={true};
        n.send(e,()->false,()->fail("pre-dropped delivered"));
        n.send(e,()->connected[0],()->fail("in-flight drop delivered"));
        assertThrows(IllegalStateException.class,()->n.beginMeasurement(1000));
        connected[0]=false; s.drain();
        assertEquals(80,n.stats().attemptedBytes()); assertEquals(80,n.stats().droppedBytes());
        assertEquals(40,n.stats().enqueuedBytes()); assertEquals(0,n.stats().pendingBytes());
        n.beginMeasurement(2000); assertEquals(0,n.stats().attemptedBytes());
    }
    @Test void largerFramesOrLowerBandwidthIncreaseObservedNormalWriteCost() {
        var small=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("PB","NORMAL",1,64,1,2000));
        var large=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("PB","NORMAL",1,8192,1,2000));
        var fast=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("PB","NORMAL",1,8192,100,2000));
        assertEquals("OK",small.outcome()); assertEquals("OK",large.outcome()); assertEquals("OK",fast.outcome());
        assertTrue(large.responseNs()>small.responseNs()); assertTrue(large.responseNs()>fast.responseNs());
        var shortLog=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("VR","NORMAL",1,64,10,2000));
        var longLog=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("VR","NORMAL",64,64,10,2000));
        assertEquals("OK",longLog.outcome()); assertTrue(longLog.responseNs()>shortLog.responseNs());
    }
    @Test void fixedNetworkCostModelReplaysIdenticallyAndCensoringIsNotSuccess() {
        for(String variant:List.of("PB","VR")) for(String scenario:List.of("NORMAL","CRASH")) {
            var condition=new NetworkCostExperiment.Condition(variant,scenario,64,8192,1,100);
            var m=NetworkCostExperiment.run(condition); assertEquals(m,NetworkCostExperiment.run(condition));
            var n=m.network();
            assertEquals(n.attemptedBytes(),n.deliveredBytes()+n.droppedBytes()+n.pendingBytes());
            assertEquals(n.enqueuedBytes(),n.dataBytes()+n.controlBytes());
            if(m.outcome().equals("CENSORED")) {
                assertEquals(-1,m.recoveryNs()); assertEquals(ProtocolCluster.ms(5000),m.observedNs());
            }
        }
    }
}
