package io.openkache.core.replication.ionia_fourth;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_fourth.IoniaFourthRegressionTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.spi.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

@Timeout(30)
class IoniaFourthBatchIntegrationTest {
    static RequestContext fresh(Cluster c,long id) { return new RequestContext(CLIENT,new OperationId(id),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(c.clock.now+10_000_000_000L)); }
    static void fast(Cluster c,int count) { for(int i=1;i<=count;i++) c.client.execute(new Command.Set(Bytes.utf8("k"+i),Bytes.utf8("v"+i)),fresh(c,i)); c.pump(); }
    @Test void V03_03_fastAckDoesNotWaitForDelayButTailEventuallyDrains() {
        try(Cluster c=new Cluster(3,new IoniaBatchSettings(10,65536,40_000_000))) {
            var write=c.client.execute(set("v"),fresh(c,1)); c.pump(); assertEquals(ResultStatus.OK,result(write).status());
            assertEquals(0,c.engines.get(A).progress().commitIndex());
            c.clock.now=39_999_999; c.tick(); c.pump(); assertEquals(0,c.engines.get(A).progress().commitIndex());
            c.clock.now=40_000_000; c.tick(); c.pump();
            for(var e:c.engines.values()) assertEquals(1,e.progress().appliedIndex());
        }
    }
    @Test void V03_06_15_pendingReadForLaterKeyForcesEveryRequiredChunkDespiteDelay() {
        try(Cluster c=new Cluster(3,new IoniaBatchSettings(2,65536,60_000_000_000L))) {
            fast(c,5); assertEquals(4,c.engines.get(A).progress().commitIndex());
            var meta=c.engines.get(A).metaQuery(request(99,new Command.Get(Bytes.utf8("k5")),0)); c.pump();
            assertEquals(Bytes.utf8("v5"),meta.toCompletableFuture().join().data().value().orElseThrow());
            var del=c.client.execute(new Command.Delete(Bytes.utf8("k5")),fresh(c,100)); c.pump(); assertEquals(ResultStatus.OK,result(del).status());
            assertEquals(6,c.engines.get(A).progress().appliedIndex());
        }
    }
    @Test void V03_06_appliedGetDoesNotForceUnrelatedFutureWrite() {
        try(Cluster c=new Cluster(3,new IoniaBatchSettings(10,65536,60_000_000_000L))) {
            c.client.execute(set("v"),fresh(c,1)); c.pump();
            var get=c.client.execute(new Command.Get(K),fresh(c,2)); c.pump(); assertEquals(Bytes.utf8("v"),result(get).value().orElseThrow());
            c.client.execute(new Command.Set(Bytes.utf8("later"),Bytes.utf8("x")),fresh(c,3)); c.pump();
            var before=c.engines.get(A).batchDiagnostics().batches().attempts();
            get=c.client.execute(new Command.Get(K),fresh(c,4)); c.pump(); assertEquals(ResultStatus.OK,result(get).status());
            assertEquals(before,c.engines.get(A).batchDiagnostics().batches().attempts()); assertEquals(1,c.engines.get(A).progress().commitIndex());
        }
    }
    @Test void V03_04_08_lostPrepareOrAckCannotSkipAChunkAndLastTailRetries() {
        for(boolean loseAck:new boolean[]{false,true}) try(Cluster c=new Cluster(3,new IoniaBatchSettings(2,65536,0))) {
            c.drop=env-> loseAck ? env.message() instanceof IoniaMessage.PrepareAck : env.message() instanceof IoniaMessage.Prepare;
            fast(c,5); assertEquals(0,c.engines.get(A).progress().commitIndex());
            assertEquals(0,c.engines.get(A).batchDiagnostics().batches().peers().get(B).matched());
            c.drop=env->false; c.clock.now=20_000_000; c.tick(); c.pump();
            for(var e:c.engines.values()) assertEquals(5,e.progress().appliedIndex());
            assertTrue(c.engines.get(A).batchDiagnostics().batches().retries()>0);
        }
    }
    @Test void V03_15_existingFlightWaitsForRetryThenUrgentBarrierContinues() {
        try(Cluster c=new Cluster(3,new IoniaBatchSettings(2,65536,60_000_000_000L))) {
            c.drop=env->env.message() instanceof IoniaMessage.PrepareAck; fast(c,5);
            var get=c.client.execute(new Command.Get(Bytes.utf8("k5")),fresh(c,90)); c.pump(); assertFalse(get.toCompletableFuture().isDone());
            c.drop=env->false; c.clock.now=19_999_999; c.tick(); c.pump(); assertFalse(get.toCompletableFuture().isDone());
            c.clock.now=20_000_000; c.tick(); c.pump(); assertEquals(Bytes.utf8("v5"),result(get).value().orElseThrow());
            assertEquals(5,c.engines.get(A).progress().appliedIndex());
        }
    }
    @Test void V03_04_16_futureAckIsRejectedAndRestoredCumulativeAckMayExceedFlight() {
        try(Cluster c=new Cluster(3,1)) {
            c.drop=env->true; fast(c,5); c.engines.get(A).onPeerMessage(B,new IoniaMessage.PrepareAck(0,6)); c.runOwners();
            assertEquals(0,c.engines.get(A).batchDiagnostics().batches().peers().get(B).matched());
            c.engines.get(A).onPeerMessage(B,new IoniaMessage.PrepareAck(0,4)); c.runOwners();
            var prepare=c.messages.stream().filter(e->e.to().equals(B)&&e.message() instanceof IoniaMessage.Prepare).map(e->(IoniaMessage.Prepare)e.message()).findFirst().orElseThrow();
            assertEquals(5,prepare.entries().getFirst().index());
        }
    }
    @Test void V03_07_oldViewAckAndDelayedLocalSendCompletionsCannotChangeNewState() {
        try(Cluster c=new Cluster(3)) {
            fast(c,5); c.engines.get(B).startViewChange(1); c.engines.get(C).startViewChange(1); c.pump();
            var before=c.engines.get(B).batchDiagnostics();
            c.engines.get(B).onPeerMessage(A,new IoniaMessage.PrepareAck(0,5)); c.pump();
            assertEquals(before,c.engines.get(B).batchDiagnostics());
        }
    }
    @Test void V03_17_overlappingTransportStagesAreNotAcknowledgementsAndOldCallbacksAreFenced() {
        Clock clock=new Clock(); Queue<Runnable> tasks=new ArrayDeque<>(); List<CompletableFuture<Void>> sends=new ArrayList<>();
        Mailbox owner=new Mailbox(){public boolean offer(Runnable r){return tasks.offer(r);} public int capacity(){return 100000;}};
        Transport transport=env->{ var f=new CompletableFuture<Void>(); sends.add(f); return f; };
        try(IoniaFourthEngine e=new IoniaFourthEngine(new IoniaConfig(A,A,List.of(A,B,C),0),new MemoryStorage(tasks),owner,transport,clock,new IoniaBatchSettings(1,65536,0))) {
            e.recordWrite(request(1,set("v"),0)); while(!tasks.isEmpty()) tasks.remove().run();
            clock.now=20_000_000; e.onTimer(new TimerEvent(IoniaEngine.TICK,clock.now)); while(!tasks.isEmpty()) tasks.remove().run();
            assertEquals(4,e.batchDiagnostics().outstandingSendStages()); assertEquals(0,e.progress().commitIndex());
            e.startRecovery(UUID.randomUUID()); while(!tasks.isEmpty()) tasks.remove().run();
            sends.forEach(f->f.complete(null)); while(!tasks.isEmpty()) tasks.remove().run();
            assertEquals(0,e.batchDiagnostics().outstandingSendStages()); assertEquals(0,e.progress().commitIndex());
        }
    }
    @Test void V03_02_everyReplicaRejectsOversizedWriteBeforeRecordOrAck() {
        Bytes value=Bytes.copyOf(new byte[(int)IoniaPrepareSizer.MAX_KV_PAYLOAD-18]);
        Command.Set oversized=new Command.Set(Bytes.utf8("xx"),value);
        try(Cluster c=new Cluster(3)) {
            for(NodeId node:c.nodes) {
                var response=c.engines.get(node).recordWrite(request(1,oversized,0)); c.pump();
                assertEquals(ResultStatus.CAPACITY_EXCEEDED,response.toCompletableFuture().join().status());
                assertEquals(0,c.engines.get(node).progress().recordedCount());
            }
            var response=c.engines.get(A).execute(oversized,fresh(c,1)); c.pump(); assertEquals(ResultStatus.CAPACITY_EXCEEDED,result(response).status());
        }
    }
    @Test void V03_05_midChunkLeaderCrashPreservesAllCompletedWritesForThreeAndFiveNodes() {
        for(int count:new int[]{3,5}) try(Cluster c=new Cluster(count,new IoniaBatchSettings(2,65536,60_000_000_000L))) {
            c.drop=env->env.message() instanceof IoniaMessage.PrepareAck;
            fast(c,7); assertEquals(0,c.engines.get(A).progress().commitIndex());
            assertTrue(c.engines.get(B).progress().lastLogIndex()<=2);
            c.crash(A); if(count==5)c.crash(new NodeId(5)); c.drop=env->false;
            for(NodeId node:c.nodes)if(!c.offline.contains(node))c.engines.get(node).startViewChange(1);
            c.pump(); c.client.updateView(1,B);
            for(int i=1;i<=7;i++) {
                var get=c.client.readAtFollower(C,new Command.Get(Bytes.utf8("k"+i)),fresh(c,100+i));c.pump();
                assertEquals(Bytes.utf8("v"+i),result(get).value().orElseThrow());
            }
            assertEquals(7,c.engines.get(B).progress().lastLogIndex());
        }
    }
    @Test void V03_13_duplicateRecordingAndContinuousArrivalCannotResetEngineAge() {
        try(Cluster c=new Cluster(3,new IoniaBatchSettings(128,65536,40_000_000))) {
            c.client.execute(set("v"),fresh(c,1));c.pump();
            for(int i=1;i<4;i++) {
                c.clock.now=i*10_000_000L;
                c.engines.get(A).recordWrite(request(1,set("v"),0));
                c.client.execute(new Command.Set(Bytes.utf8("other"+i),Bytes.utf8("x")),fresh(c,i+1)); c.pump();
                c.tick();c.pump();assertEquals(0,c.engines.get(A).progress().commitIndex());
            }
            c.clock.now=40_000_000;c.tick();c.pump();assertEquals(4,c.engines.get(A).progress().appliedIndex());
        }
    }
    @Test void V03_16_reorderedAndDuplicatedPeerChunksCannotPublishAGap() {
        try(Cluster c=new Cluster(3)) {
            c.engines.get(B).onPeerMessage(A,new IoniaMessage.Prepare(0,List.of(entry(2))));c.runOwners();
            assertEquals(0,c.engines.get(B).progress().lastLogIndex());assertTrue(c.messages.isEmpty());
            c.engines.get(B).onPeerMessage(A,new IoniaMessage.Prepare(0,List.of(entry(1))));c.runOwners();
            c.engines.get(B).onPeerMessage(A,new IoniaMessage.Prepare(0,List.of(entry(2))));c.runOwners();
            c.engines.get(B).onPeerMessage(A,new IoniaMessage.Prepare(0,List.of(entry(1))));c.runOwners();
            assertEquals(2,c.engines.get(B).progress().lastLogIndex());assertEquals(2,c.engines.get(B).progress().recordedCount());
        }
    }
    @Test void V03_07_queuedFlushFromBeforeViewTransitionIsFenced() {
        try(Cluster c=new Cluster(3)) {
            c.engines.get(A).recordWrite(request(1,set("v"),0));
            c.engines.get(A).startViewChange(1);c.runOwners();
            assertEquals(IoniaEngine.Status.VIEW_CHANGE,c.engines.get(A).viewInfo().status());
            assertEquals(0,c.engines.get(A).batchDiagnostics().batches().attempts());
            assertTrue(c.engines.get(A).failureReason().isEmpty());
        }
    }
    @Test void V03_08_oneSlowFollowerDoesNotPreventOtherQuorumProgress() {
        try(Cluster c=new Cluster(3,new IoniaBatchSettings(2,65536,0))) {
            c.drop=env->env.to().equals(C)||env.from().equals(C);fast(c,7);
            assertEquals(7,c.engines.get(A).progress().appliedIndex());
            assertEquals(7,c.engines.get(A).batchDiagnostics().batches().peers().get(B).matched());
            assertEquals(0,c.engines.get(A).batchDiagnostics().batches().peers().get(C).matched());
            c.drop=env->false;c.clock.now=20_000_000;c.tick();c.pump();assertEquals(7,c.engines.get(C).progress().appliedIndex());
        }
    }
    @Test void V03_04_failedLocalStagesAndSynchronousThrowAreRetriedWithoutAck() {
        for(boolean throwsImmediately:new boolean[]{false,true}) {
            Clock clock=new Clock();Queue<Runnable> tasks=new ArrayDeque<>();
            Mailbox owner=new Mailbox(){public boolean offer(Runnable r){return tasks.offer(r);}public int capacity(){return 100000;}};
            Transport transport=env->{
                if(env.message() instanceof IoniaMessage.Prepare) {
                    if(throwsImmediately)throw new IllegalStateException("fixture send failure");
                    return CompletableFuture.failedFuture(new IllegalStateException("fixture stage failure"));
                }
                return CompletableFuture.completedFuture(null);
            };
            try(var engine=new IoniaFourthEngine(new IoniaConfig(A,A,List.of(A,B,C),0),new MemoryStorage(tasks),owner,transport,clock,new IoniaBatchSettings(1,65536,0))) {
                engine.recordWrite(request(1,set("v"),0));while(!tasks.isEmpty())tasks.remove().run();
                assertEquals(2,engine.batchDiagnostics().locallyFailedStages());assertEquals(0,engine.batchDiagnostics().outstandingSendStages());
                clock.now=20_000_000;engine.onTimer(new TimerEvent(IoniaEngine.TICK,clock.now));while(!tasks.isEmpty())tasks.remove().run();
                assertEquals(4,engine.batchDiagnostics().locallyFailedStages());assertEquals(0,engine.progress().commitIndex());
                assertTrue(engine.failureReason().isEmpty());
                var flight=engine.batchDiagnostics().batches().peers().get(B).flight().orElseThrow();
                assertEquals(0,flight.firstSendNanos());assertEquals(20_000_000,flight.lastSendNanos());
            }
        }
    }

}
