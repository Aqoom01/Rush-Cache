package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.rose.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RoseTest {
    @Test void ackAtExactDeadlineCannotAdmitExpiredDeferredRequest() {
        try(var c=new RoseCluster(1,50,200,0)) {
            c.set(1,"first","v",1000); var second=c.set(2,"expired","v",100);
            c.advance(101); assertEquals(ResultStatus.TIMEOUT,second.join().status());
            assertEquals(1,c.nodes.get(0).observation().logLength()); assertEquals(ResultStatus.MISS,c.local(0,"expired").status());
        }
    }
    @Test void finiteWindowWaitsAndExpiredDeferredWriteIsNeverAdmitted() {
        try(var c=new RoseCluster(3,1,200,0)) {
            c.isolate(0);
            for(int i=1;i<=3;i++) assertEquals(ResultStatus.OK,c.set(i,"k"+i,"v",100).join().status());
            var blocked=c.set(4,"not-admitted","v",100); assertFalse(blocked.isDone());
            assertEquals(ResultStatus.BUSY,c.set(5,"busy","v",100).join().status());
            assertEquals(3,c.nodes.get(0).observation().logLength()); c.advance(120);
            assertEquals(ResultStatus.TIMEOUT,blocked.join().status());
            c.heal(); c.advance(2500); assertTrue(c.converged());
            assertEquals(ResultStatus.MISS,c.local(0,"not-admitted").status()); assertEquals(3,c.nodes.get(0).observation().peakLag());
        }
    }
    @Test void zeroWindowWaitsForEveryBackupAndPreservesSuccessfulWrite() {
        try(var c=new RoseCluster(0,1,200,0)) {
            c.slowData=200; var f=c.set(1,"a","v",1000); c.advance(20); assertFalse(f.isDone());
            c.advance(200); assertEquals(ResultStatus.OK,f.join().status());
            c.crashPrimary(); c.slowData=0; c.advance(1500);
            assertEquals(Bytes.utf8("v"),c.local(c.primary,"a").value().orElseThrow()); assertTrue(c.converged());
        }
    }
    @Test void immediateCrashLossIsExactlyWindowAndUnlimitedIsNotBounded() {
        for(int limit:new int[]{1,10,100,-1}) try(var c=new RoseCluster(limit,1,200,0)) {
            int successes=limit<0?200:limit;
            for(int i=1;i<=successes;i++) assertEquals(ResultStatus.OK,c.set(i,"k"+i,"v",100).join().status());
            c.crashPrimary(); c.advance(1500);
            for(int i=1;i<=successes;i++) assertEquals(ResultStatus.MISS,c.local(c.primary,"k"+i).status());
            assertTrue(c.converged()); assertEquals(successes,c.nodes.get(0).observation().peakLag());
        }
    }
    @Test void mostAdvancedBackupIsSelectedAndRepairsSlowerPeer() {
        try(var c=new RoseCluster(10,1,200,1_048_576)) {
            c.drop=e->e.to().value()==1 && e.message() instanceof RoseMessage.Append;
            for(int i=1;i<=5;i++) assertEquals(ResultStatus.OK,c.set(i,"k"+i,"v",100).join().status());
            c.advance(30); assertEquals(0,c.nodes.get(1).observation().applied()); assertEquals(5,c.nodes.get(2).observation().applied());
            c.crashPrimary(); c.drop=e->false; c.advance(2000);
            assertEquals(2,c.primary); assertTrue(c.converged());
            for(int i=1;i<=5;i++) assertEquals(Bytes.utf8("v"),c.local(1,"k"+i).value().orElseThrow());
        }
    }
    @Test void duplicateAndDeleteResultsSurvivePromotion() {
        try(var c=new RoseCluster(10,1,200,0)) {
            c.duplicate=true; c.set(1,"a","v",100); c.advance(20);
            long version=c.local(0,"a").valueVersion(); assertEquals(ResultStatus.OK,c.set(1,"a","v",100).join().status());
            assertEquals(version,c.local(0,"a").valueVersion());
            assertEquals(ResultStatus.BAD_REQUEST,c.set(1,"a","other",100).join().status());
            var deleted=c.execute(0,2,new Command.Delete(Bytes.utf8("a")),100); assertEquals(ResultStatus.OK,deleted.join().status());
            c.advance(20); c.crashPrimary(); c.advance(1500);
            assertEquals(ResultStatus.OK,c.execute(c.primary,2,new Command.Delete(Bytes.utf8("a")),100).join().status());
            assertEquals(ResultStatus.MISS,c.local(c.primary,"a").status());
        }
    }
    @Test void staleAndNonPrimaryMessagesCannotChangeState() {
        try(var c=new RoseCluster(1,1,200,0)) {
            var r=new RoseMessage.Request(new ClientId(7),new OperationId(1),new Command.Set(Bytes.utf8("bad"),Bytes.utf8("v")));
            c.nodes.get(1).onPeerMessage(new NodeId(2),new RoseMessage.Append(1,1,r));
            c.nodes.get(1).onPeerMessage(new NodeId(0),new RoseMessage.Append(2,1,r));
            assertEquals(ResultStatus.MISS,c.local(1,"bad").status());
            c.nodes.get(0).onPeerMessage(new NodeId(1),new RoseMessage.Applied(1,100));
            assertEquals(0,c.nodes.get(0).observation().minAck());
        }
    }
    @Test void lostAckOnlyAppliesOnceAndRetryMaySucceedAfterTimeout() {
        try(var c=new RoseCluster(0,1,200,0)) {
            c.drop=e->e.message() instanceof RoseMessage.Applied; var f=c.set(1,"a","v",100); c.advance(120);
            assertEquals(ResultStatus.TIMEOUT,f.join().status()); long version=c.local(1,"a").valueVersion();
            c.drop=e->false; c.advance(2200); assertEquals(ResultStatus.OK,c.set(1,"a","v",100).join().status());
            assertEquals(version,c.local(1,"a").valueVersion()); assertTrue(c.converged());
        }
    }
    @Test void noPartitionPromotionAndOldEpochRejectedAfterCrash() {
        try(var c=new RoseCluster(1,1,200,0)) {
            c.isolate(0); c.set(1,"a","v",100); c.advance(500); assertEquals(-1,c.detectedAt);
            assertEquals(1,c.nodes.get(1).observation().epoch()); c.heal(); c.advance(1500);
            c.crashPrimary(); c.advance(1500);
            var stale=c.nodes.get(c.primary).execute(new Command.Count(),new RequestContext(new ClientId(4),new OperationId(8),RoutingVersion.ZERO,
                    new OwnershipEpoch(1),new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
            assertEquals(ResultStatus.STALE_EPOCH,stale.status());
            assertThrows(Exception.class,()->c.nodes.get(c.primary).openAfterRepair(2,1).toCompletableFuture().join());
        }
    }
    @Test void completePrefixMatchesBackupKvAndReplayIsDeterministic() {
        assertEquals(trace(),trace());
    }
    private List<RoseEngine.Observation> trace() {
        try(var c=new RoseCluster(10,2,200,1_048_576)) {
            for(int i=1;i<=8;i++) c.set(i,"same","v"+i,100); c.advance(100);
            for(int node=1;node<3;node++) {
                int applied=c.nodes.get(node).observation().applied();
                assertEquals(Bytes.utf8("v"+applied),c.local(node,"same").value().orElseThrow());
            }
            c.crashPrimary(); c.advance(1000); return c.nodes.stream().map(RoseEngine::observation).toList();
        }
    }
}
