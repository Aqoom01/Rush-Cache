package io.openkache.core.replication.ionia_fifth;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_fifth.IoniaFifthRegressionTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

@Timeout(30)
class IoniaFifthApplyTest {
    static RequestContext fresh(Cluster c,long id){return new RequestContext(CLIENT,new OperationId(id),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(c.clock.now+10_000_000_000L));}
    static Command.Set put(Bytes key,String value){return new Command.Set(key,Bytes.utf8(value));}
    static void fill(Cluster c,int count){for(int i=1;i<=count;i++)c.client.execute(put(Bytes.utf8("k"+i),"v"+i),fresh(c,i));c.pump();}
    static void healthy(Cluster c){for(var engine:c.engines.values())assertTrue(engine.failureReason().isEmpty(),engine.failureReason().toString());}
    @Test void V04_02_distinctKeysActuallyOverlapOnlyUpToConfiguredLimit() {
        for(int cap:new int[]{1,2,4,65536})try(Cluster c=new Cluster(3,cap)) {
            for(int i=1;i<=5;i++)c.stores.get(A).held.add(Bytes.utf8("k"+i));fill(c,5);
            assertEquals(Math.min(cap,5),c.stores.get(A).active);assertEquals(Math.min(cap,5),c.engines.get(A).applyDiagnostics().gate().activeKeys());
            assertEquals(5-Math.min(cap,5),c.engines.get(A).applyDiagnostics().gate().readyKeys());
            while(!c.stores.get(A).held.isEmpty()){for(Bytes key:List.copyOf(c.stores.get(A).held))c.stores.get(A).release(key);c.pump();}
            assertEquals(5,c.engines.get(A).progress().appliedIndex());assertTrue(c.stores.get(A).peak<=cap);
            assertEquals(0,c.engines.get(A).applyDiagnostics().gate().activeKeys());
        }
    }
    @Test void V04_01_sameKeySetDeleteAndRetryKeepConsensusOrder() {
        for(int cap:new int[]{1,2,4})try(Cluster c=new Cluster(3,cap)) {
            c.stores.get(A).held.add(K);c.client.execute(set("one"),fresh(c,1));
            var deleted=c.client.execute(new Command.Delete(K),fresh(c,2));c.client.execute(set("three"),fresh(c,3));c.pump();
            assertEquals(1,c.stores.get(A).active);assertEquals(1,c.stores.get(A).startedKeys.size());
            c.stores.get(A).release(K);c.pump();assertEquals(ResultStatus.OK,result(deleted).status());
            assertEquals(Bytes.utf8("three"),c.stores.get(A).values.get(K));assertEquals(List.of(K,K,K),c.stores.get(A).startedKeys);
            var retry=c.client.execute(new Command.Delete(K),fresh(c,2));c.pump();assertEquals(ResultStatus.OK,result(retry).status());
            assertEquals(Bytes.utf8("three"),c.stores.get(A).values.get(K));
        }
    }
    @Test void V04_03_07_hashCollisionKeysCanOverlapButAppliedPrefixNeverSkips() {
        try(Cluster c=new Cluster(3,2)) {
            Bytes a=Bytes.utf8("Aa"),b=Bytes.utf8("BB");assertEquals(a.hashCode(),b.hashCode());c.stores.get(A).held.add(a);
            c.client.execute(put(a,"a"),fresh(c,1));c.client.execute(put(b,"b"),fresh(c,2));c.pump();
            assertEquals(Bytes.utf8("b"),c.stores.get(A).values.get(b));assertEquals(0,c.engines.get(A).progress().appliedIndex());
            assertEquals(1,c.engines.get(A).applyDiagnostics().completedBeyondPrefix());
            c.stores.get(A).release(a);c.pump();assertEquals(2,c.engines.get(A).progress().appliedIndex());
        }
    }
    @Test void V04_02_hotKeyCannotKeepPermitAheadOfAlreadyWaitingColdKeys() {
        try(Cluster c=new Cluster(3,1)) {
            Bytes b=Bytes.utf8("cold-b"),d=Bytes.utf8("cold-c");c.stores.get(A).held.add(K);
            c.client.execute(set("first"),fresh(c,1));c.client.execute(set("second"),fresh(c,2));
            c.client.execute(put(b,"b"),fresh(c,3));c.client.execute(put(d,"c"),fresh(c,4));c.pump();
            c.stores.get(A).release(K);c.pump();assertEquals(List.of(K,b,d,K),c.stores.get(A).startedKeys);
            assertEquals(4,c.engines.get(A).progress().appliedIndex());
        }
    }
    @Test void V04_04_clientTimeoutOrCancellationDoesNotReleasePhysicalApplyEarly() {
        for(boolean cancel:new boolean[]{false,true})try(Cluster c=new Cluster(3,1)) {
            Bytes other=Bytes.utf8("other");c.stores.get(A).held.add(K);
            var write=c.engines.get(A).execute(set("first"),fresh(c,1));c.pump();
            c.engines.get(A).execute(put(other,"next"),fresh(c,2));c.pump();
            if(cancel)write.toCompletableFuture().cancel(false);
            else{c.clock.now=11_000_000_000L;c.tick();c.pump();assertEquals(ResultStatus.TIMEOUT,result(write).status());}
            assertEquals(1,c.stores.get(A).active);assertFalse(c.stores.get(A).values.containsKey(other));
            c.stores.get(A).release(K);c.pump();assertEquals(2,c.engines.get(A).progress().appliedIndex());
            assertEquals(0,c.engines.get(A).applyDiagnostics().gate().activeKeys());
        }
    }
    @Test void V04_04_exceptionBusyAndCancelledStorageCannotFabricateSuccess() {
        for(int mode=0;mode<5;mode++)try(Cluster c=new Cluster(3,1)) {
            var store=c.stores.get(A);store.throwApply=mode==0;store.exceptionalApply=mode==1;store.cancelApply=mode==2;
            store.rejectApply=mode==3?ResultStatus.BUSY:mode==4?ResultStatus.CAPACITY_EXCEEDED:null;
            var request=c.engines.get(A).execute(set("rejected"),fresh(c,1));c.runOwners();
            int limit=1000;while((!c.tasks.isEmpty()||!c.messages.isEmpty())&&limit-->0){
                c.runOwners();if(!c.messages.isEmpty()){var env=c.messages.removeFirst();c.engines.get(env.to()).onPeerMessage(env.from(),env.message());}
            }
            assertTrue(c.engines.get(A).failureReason().isPresent());assertEquals(ResultStatus.UNAVAILABLE,result(request).status());
            assertEquals(0,c.engines.get(A).applyDiagnostics().gate().activeKeys());
        }
    }
    @Test void V04_04_closeKeepsOtherKeyUnstartedAndPhysicalCompletionReleasesPermit() {
        try(Cluster c=new Cluster(3,1)) {
            c.stores.get(A).held.add(K);c.client.execute(set("held"),fresh(c,1));c.client.execute(put(Bytes.utf8("later"),"x"),fresh(c,2));c.pump();
            c.engines.get(A).close();c.stores.get(A).release(K);c.runOwners();
            assertEquals(1,c.stores.get(A).startedKeys.size());assertEquals(0,c.engines.get(A).applyDiagnostics().gate().activeKeys());
            assertEquals(IoniaEngine.Status.RECOVERING,c.engines.get(A).viewInfo().status());
        }
    }
    @Test void V04_05_viewInstallationWaitsForOldPhysicalApplyAndDiscardsOldReadyQueue() {
        try(Cluster c=new Cluster(3,1)) {
            c.stores.get(B).held.add(K);c.client.execute(set("latest"),fresh(c,1));c.client.execute(put(Bytes.utf8("other"),"v"),fresh(c,2));c.pump();
            assertEquals(1,c.stores.get(B).startedKeys.size());
            c.engines.get(B).startViewChange(1);c.engines.get(C).startViewChange(1);c.pump();
            assertTrue(c.engines.get(B).applyDiagnostics().installing());assertFalse(c.engines.get(B).applyDiagnostics().replaying());
            assertEquals(0,c.stores.get(B).resets);c.stores.get(B).release(K);c.pump();
            assertEquals(1,c.stores.get(B).resets);assertEquals(Bytes.utf8("latest"),c.stores.get(B).values.get(K));
            assertEquals(2,c.engines.get(B).progress().appliedIndex());assertTrue(c.stores.get(B).peak<=1);
            var gate=c.engines.get(B).applyDiagnostics().gate();assertEquals(gate.starts(),gate.released()+gate.resetDiscarded()+gate.activeKeys());
        }
    }
    @Test void V04_05_resetFailureFailsClosedAfterOldPermitsAreDrained() {
        try(Cluster c=new Cluster(3,1)) {
            c.stores.get(B).held.add(K);c.client.execute(set("held"),fresh(c,1));c.pump();
            c.stores.get(B).failReset=true;c.engines.get(B).startViewChange(1);c.engines.get(C).startViewChange(1);
            int limit=10000;while((!c.tasks.isEmpty()||!c.messages.isEmpty())&&limit-->0){c.runOwners();if(!c.messages.isEmpty()){var env=c.messages.removeFirst();c.engines.get(env.to()).onPeerMessage(env.from(),env.message());}}
            assertEquals(0,c.stores.get(B).resets);assertEquals(1,c.engines.get(B).applyDiagnostics().gate().activeKeys());
            c.stores.get(B).release(K);driveWithExpectedFailure(c);
            assertTrue(c.engines.get(B).failureReason().isPresent());assertEquals(0,c.engines.get(B).applyDiagnostics().gate().activeKeys());
        }
    }
    @Test void V04_04_rejectedOwnerMailboxDoesNotAcquireAnyApplyPermit() {
        Clock clock=new Clock();Queue<Runnable> tasks=new ArrayDeque<>();
        Mailbox full=new Mailbox(){public boolean offer(Runnable r){return false;}public int capacity(){return 1;}};
        try(var engine=new IoniaFifthEngine(new IoniaConfig(A,A,List.of(A,B,C),0),new MemoryStorage(tasks),full,
                envelope->CompletableFuture.completedFuture(null),clock,new IoniaApplySettings(1,2))) {
            assertEquals(ResultStatus.BUSY,result(engine.execute(set("v"),context(1))).status());
            assertEquals(0,engine.applyDiagnostics().gate().starts());
        }
    }
    static void driveWithExpectedFailure(Cluster c) {
        int limit=10000;
        while(!c.tasks.isEmpty()||!c.messages.isEmpty()){
            assertTrue(limit-->0);c.runOwners();if(!c.messages.isEmpty()){var e=c.messages.removeFirst();c.engines.get(e.to()).onPeerMessage(e.from(),e.message());}
        }
    }
    @Test void V04_04_oneFailedKeyDoesNotReleaseAnotherKeysPhysicalPermitEarly() {
        try(Cluster c=new Cluster(3,2)) {
            Bytes held=Bytes.utf8("held");var store=c.stores.get(A);store.held.add(held);store.exceptionalApply=true;store.failOnlyKey=K;
            var failed=c.engines.get(A).execute(set("bad"),fresh(c,1));var other=c.engines.get(A).execute(put(held,"v"),fresh(c,2));
            driveWithExpectedFailure(c);assertEquals(ResultStatus.UNAVAILABLE,result(failed).status());assertEquals(ResultStatus.UNAVAILABLE,result(other).status());
            assertEquals(1,store.active);assertEquals(1,c.engines.get(A).applyDiagnostics().gate().activeKeys());
            store.release(held);c.runOwners();assertEquals(0,c.engines.get(A).applyDiagnostics().gate().activeKeys());assertEquals(0,store.active);
        }
    }

}
