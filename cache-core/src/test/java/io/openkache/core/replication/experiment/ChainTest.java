package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.chain.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ChainTest {
    @Test void configurationWaitsForOutstandingKvApplication() {
        var clock=new io.openkache.core.replication.VirtualTimeSource();
        var mailbox=new io.openkache.core.replication.DirectMailbox();
        try(var kv=new io.openkache.core.kv.KvEngine(new NodeId(0),List.of(mailbox),clock)) {
            var gate=new java.util.concurrent.CompletableFuture<Result>();
            var actual=new java.util.concurrent.atomic.AtomicReference<Result>();
            CacheEngine delayed=new CacheEngine() {
                @Override public java.util.concurrent.CompletionStage<Result> execute(Command c,RequestContext context) {
                    actual.set(kv.execute(c,context).toCompletableFuture().join()); return gate;
                }
                @Override public void onPeerMessage(NodeId from,io.openkache.core.spi.PeerMessage message) {}
                @Override public void onTimer(io.openkache.core.runtime.TimerEvent timer) {}
                @Override public EngineSnapshot snapshot() { return kv.snapshot(); }
                @Override public void close() {}
            };
            List<io.openkache.core.spi.PeerEnvelope> messages=new ArrayList<>();
            try(var node=new ChainEngine(new NodeId(0),List.of(new NodeId(0),new NodeId(1),new NodeId(2)),1,delayed,mailbox,
                    e->{ messages.add(e); return java.util.concurrent.CompletableFuture.completedFuture(null); },clock,java.time.Duration.ofSeconds(1))) {
                var write=node.execute(new Command.Set(Bytes.utf8("a"),Bytes.utf8("v")),new RequestContext(new ClientId(7),new OperationId(1),
                        RoutingVersion.ZERO,new OwnershipEpoch(1),new Deadline(Long.MAX_VALUE))).toCompletableFuture();
                var frozen=node.installConfiguration(2,List.of(new NodeId(0),new NodeId(2))).toCompletableFuture();
                assertEquals(ResultStatus.UNAVAILABLE,write.join().status()); assertFalse(frozen.isDone());
                assertEquals(0,node.observation().applied());
                gate.complete(actual.get()); assertEquals(1,frozen.join());
                assertEquals(ChainEngine.State.FROZEN,node.observation().state()); assertTrue(messages.isEmpty());
                node.beginRepair(2).toCompletableFuture().join();
                assertThrows(Exception.class,()->node.openConfiguration(2,1).toCompletableFuture().join());
                node.onPeerMessage(new NodeId(2),new ChainMessage.Progress(2,0));
                assertTrue(messages.getLast().message() instanceof ChainMessage.Forward f && f.epoch()==2 && f.index()==1);
            }
        }
    }
    @Test void tailConfirmsBeforeReverseAckAndReadsRedirectToTail() {
        try(var c=new ChainCluster(1,200,0)) {
            c.drop=e->e.message() instanceof ChainMessage.Ack;
            var f=c.set(1,"a","v",100); c.advance(20); assertEquals(ResultStatus.OK,f.join().status());
            assertEquals(1,c.nodes.get(0).observation().sent());
            assertEquals(Bytes.utf8("v"),c.read(2,"a").value().orElseThrow());
            assertEquals(new NodeId(2),c.read(0,"a").redirect().orElseThrow().target());
            assertEquals(new NodeId(0),c.execute(1,2,new Command.Set(Bytes.utf8("b"),Bytes.utf8("v")),100).join().redirect().orElseThrow().target());
            c.drop=e->false; c.advance(1200); assertEquals(0,c.nodes.get(0).observation().sent()); c.assertPrefix();
        }
    }
    @Test void tailNeverReadsUnforwardedValueAndDuplicateDoesNotReapply() {
        try(var c=new ChainCluster(1,200,0)) {
            var first=c.set(1,"a","old",200); c.advance(20); assertEquals(ResultStatus.OK,first.join().status());
            c.drop=e->e.from().value()==1 && e.to().value()==2 && e.message() instanceof ChainMessage.Forward;
            var pending=c.set(2,"a","new",100); c.advance(30);
            assertEquals(Bytes.utf8("new"),c.local(0,"a").value().orElseThrow());
            assertEquals(Bytes.utf8("old"),c.read(2,"a").value().orElseThrow());
            c.advance(100); assertEquals(ResultStatus.TIMEOUT,pending.join().status());
            c.drop=e->false; c.duplicate=true; c.advance(2300);
            long version=c.read(2,"a").valueVersion(); assertEquals(Bytes.utf8("new"),c.read(2,"a").value().orElseThrow());
            assertEquals(ResultStatus.OK,c.set(2,"a","new",100).join().status());
            assertEquals(version,c.read(2,"a").valueVersion());
            assertEquals(ResultStatus.BAD_REQUEST,c.set(2,"a","different",100).join().status()); c.assertPrefix();
        }
    }
    @Test void headMiddleAndTailCrashPreserveSuccessfulWrites() {
        for(int failed=0;failed<3;failed++) try(var c=new ChainCluster(2,200,1_048_576)) {
            for(int i=1;i<=5;i++) { var f=c.set(i,"key"+i,"v",1000); c.advance(20); assertEquals(ResultStatus.OK,f.join().status()); }
            c.crash(failed); c.advance(1500); assertTrue(c.readyHead()>=0);
            var f=c.set(10,"after","v",1000); c.advance(50); assertEquals(ResultStatus.OK,f.join().status());
            for(int n=0;n<3;n++) if(n!=failed) for(int i=1;i<=5;i++) assertEquals(Bytes.utf8("v"),c.local(n,"key"+i).value().orElseThrow());
            c.assertPrefix(); assertTrue(c.readyAt>=c.detectedAt);
        }
    }
    @Test void sentSuffixReplaysAroundCrashedMiddleThroughPeerLink() {
        try(var c=new ChainCluster(1,200,1_048_576)) {
            c.drop=e->e.from().value()==1 && e.to().value()==2 && e.message() instanceof ChainMessage.Forward;
            var pending=c.set(1,"pending","v",1000); c.advance(10);
            assertEquals(ResultStatus.MISS,c.local(2,"pending").status());
            assertEquals(1,c.nodes.get(0).observation().sent()); long before=c.network.dataBytes;
            c.crash(1); c.drop=e->false; c.advance(1500);
            assertEquals(ResultStatus.UNAVAILABLE,pending.join().status());
            assertEquals(Bytes.utf8("v"),c.local(2,"pending").value().orElseThrow());
            assertTrue(c.network.dataBytes>before); assertEquals(0,c.nodes.get(0).observation().sent());
            assertEquals(ResultStatus.OK,c.set(1,"pending","v",100).join().status()); c.assertPrefix();
        }
    }
    @Test void twoSequentialCrashesCanLeaveOneNode() {
        try(var c=new ChainCluster(1,200,0)) {
            var f=c.set(1,"before","v",500); c.advance(20); assertEquals(ResultStatus.OK,f.join().status());
            c.crash(0); c.advance(1000); c.crash(1); c.advance(1000);
            assertEquals(2,c.head()); assertEquals(2,c.tail());
            var next=c.set(2,"after","new",500); assertEquals(ResultStatus.OK,next.join().status());
            assertEquals(Bytes.utf8("v"),c.read(2,"before").value().orElseThrow());
        }
    }
    @Test void partitionDoesNotAuthorizeMasterRemovalAndHeals() {
        try(var c=new ChainCluster(1,200,0)) {
            c.isolate(0); var f=c.set(1,"isolated","v",500); c.advance(700);
            assertEquals(ResultStatus.TIMEOUT,f.join().status());
            assertEquals(ResultStatus.MISS,c.read(2,"isolated").status());
            assertEquals(1,c.masterEpoch); assertEquals(-1,c.detectedAt);
            c.heal(); c.advance(2200); assertEquals(Bytes.utf8("v"),c.read(2,"isolated").value().orElseThrow()); c.assertPrefix();
        }
    }
    @Test void progressIsNotACommitAckAndWrongNeighborsOrEpochsAreIgnored() {
        try(var c=new ChainCluster(1,200,0)) {
            c.drop=e->e.message() instanceof ChainMessage.Forward && e.to().value()==2;
            var f=c.set(1,"a","v",100); c.advance(30);
            c.nodes.get(0).onPeerMessage(new NodeId(1),new ChainMessage.Progress(1,1));
            assertFalse(f.isDone()); assertEquals(1,c.nodes.get(0).observation().sent());
            var r=new ChainMessage.Request(new ClientId(7),new OperationId(8),new Command.Set(Bytes.utf8("bad"),Bytes.utf8("v")));
            c.nodes.get(2).onPeerMessage(new NodeId(0),new ChainMessage.Forward(1,1,r));
            c.nodes.get(2).onPeerMessage(new NodeId(1),new ChainMessage.Forward(2,1,r));
            assertEquals(ResultStatus.MISS,c.local(2,"bad").status()); c.advance(100); assertEquals(ResultStatus.TIMEOUT,f.join().status());
        }
    }
    @Test void deletionAndDuplicateResultsSurviveTailChange() {
        try(var c=new ChainCluster(1,200,0)) {
            c.set(1,"a","v",100); c.advance(20);
            var deleted=c.execute(0,2,new Command.Delete(Bytes.utf8("a")),500); c.advance(20); assertEquals(ResultStatus.OK,deleted.join().status());
            c.crash(2); c.advance(1000);
            assertEquals(ResultStatus.OK,c.execute(0,2,new Command.Delete(Bytes.utf8("a")),500).join().status());
            assertEquals(ResultStatus.MISS,c.read(1,"a").status());
        }
    }
    @Test void masterCannotOpenBeforeRepairOrAddNewMembers() {
        try(var c=new ChainCluster(1,200,0)) {
            assertThrows(Exception.class,()->c.nodes.get(0).openConfiguration(1,0).toCompletableFuture().join());
            assertThrows(Exception.class,()->c.nodes.get(0).installConfiguration(2,List.of(new NodeId(0),new NodeId(3))).toCompletableFuture().join());
            assertEquals(ChainEngine.State.RUNNING,c.nodes.get(0).observation().state());
        }
    }
    @Test void sameFaultScheduleReplaysExactly() {
        assertEquals(replay(),replay());
    }
    private List<ChainEngine.Observation> replay() {
        try(var c=new ChainCluster(2,200,1_048_576)) {
            c.duplicate=true; c.set(1,"a","v",500); c.advance(20); c.crash(2); c.advance(1500);
            c.set(2,"b","v",500); c.advance(20); c.assertPrefix(); return c.nodes.stream().map(ChainEngine::observation).toList();
        }
    }
}
