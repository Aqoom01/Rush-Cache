package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage;
import io.openkache.core.replication.vr.VrEngine;
import io.openkache.core.spi.PeerEnvelope;
import java.util.*;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class LoadCrashTest {
    @TempDir Path folder;
    @Test void healthyCrashResumesUnderContinuingOfferedLoadAndPreservesPreCrashSuccesses() {
        for(String variant:List.of("PB","VR_INCREMENTAL")) {
            var condition=new LoadCrashExperiment.Condition(variant,"CRASH",20,64,1);
            var m=LoadCrashExperiment.run(condition,6000);
            assertEquals(300,m.phases().stream().mapToInt(SustainedLoadExperiment.Phase::offered).sum());
            assertTrue(m.firstPostCrashOkNs()>m.crashAtNs());assertTrue(m.firstPostCrashOkNs()<ProtocolCluster.ms(6000));
            assertEquals(m.preCrashOk(),m.retainedPreCrashAtEnd());assertEquals(m.preCrashOk()*2,m.verifiedPreCrashKvCopies());
            assertTrue(m.phases().get(1).unavailable()+m.phases().get(1).redirect()>0);
            assertTrue(m.converged());assertEquals(-1,m.samples().getLast().kv0());
            assertEquals(m,LoadCrashExperiment.run(condition,6000));
        }
    }
    @Test void slowBackupCannotBeBypassedAfterOnlyTwoSurvivorsRemain() {
        for(String variant:List.of("PB","VR_INCREMENTAL")) {
            var m=LoadCrashExperiment.run(new LoadCrashExperiment.Condition(variant,"SLOW_CRASH",20,64,1),9000);
            assertTrue(m.firstPostCrashOkNs()>m.crashAtNs());
            assertTrue(m.phases().get(1).p99Ns()>=ProtocolCluster.ms(200));
            assertEquals(m.preCrashOk(),m.retainedPreCrashAtEnd());assertTrue(m.converged());
        }
    }
    @Test void inFlightWriteMaySurviveWithoutOldClientReceivingOk() {
        for(String variant:List.of("PB","VR_INCREMENTAL")) {
            var m=LoadCrashExperiment.run(new LoadCrashExperiment.Condition(variant,"CRASH",1,64,1),12000);
            assertEquals(1,m.preCrashPending());assertEquals(1,m.inflightRetainedAtEnd());
            assertTrue(m.phases().get(0).unavailable()>0);assertTrue(m.converged());
        }
    }
    @Test void deadSenderFramesDoNotCountAsLiveReplicationBacklog() {
        var scheduler=new ReplicationExperiment.Scheduler();var network=new LinkNetwork(scheduler,1,NetworkCostExperiment.MIB);
        var message=new PrimaryBackupMessage.Replicate(new OwnershipEpoch(1),1,new Command.Set(Bytes.copyOf(new byte[16]),Bytes.copyOf(new byte[64])));
        network.send(new PeerEnvelope(new NodeId(0),new NodeId(1),message),()->true,()->{});
        network.send(new PeerEnvelope(new NodeId(1),new NodeId(2),message),()->true,()->{});
        assertEquals(258,network.pendingDataBytes);assertEquals(129,network.livePendingData(Set.of(0)));
        scheduler.drain();assertEquals(0,network.livePendingData(Set.of(0)));
    }
    @Test void diagnosticLogSnapshotIsImmutableAndDoesNotApplyPendingOperation() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,2000,1)) {
            var f=c.set(0,1,"key","value",1000);
            var snapshot=((VrEngine)c.nodes.get(0)).logSnapshot().toCompletableFuture().join();
            assertEquals(1,snapshot.size());assertFalse(f.isDone());
            assertThrows(UnsupportedOperationException.class,snapshot::clear);
            assertEquals(ResultStatus.MISS,c.local(0,"key").status());
        }
    }
    @Test void fullLengthOriginalValidatesAndRejectsLostPreCrashEvidence() throws Exception {
        var m=LoadCrashExperiment.run(new LoadCrashExperiment.Condition("PB","CRASH",20,64,1));
        LoadCrashResearchMain.validate(m);String json=LoadCrashResearchMain.encode(m);
        assertEquals(m,LoadCrashResearchMain.decode(json));
        assertThrows(IllegalStateException.class,()->LoadCrashResearchMain.decode(json.replace("a1-load-crash-json-1","bad-schema")));
        String tampered=json.replace("\"retainedPreCrashAtEnd\": "+m.preCrashOk(),"\"retainedPreCrashAtEnd\": 0");
        assertThrows(IllegalStateException.class,()->LoadCrashResearchMain.validate(LoadCrashResearchMain.decode(tampered)));
        Files.createDirectory(folder.resolve("results"));Files.writeString(folder.resolve("completion.properties"),"status=complete\n");
        assertThrows(IllegalStateException.class,()->LoadCrashResearchMain.regenerate(folder));assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
