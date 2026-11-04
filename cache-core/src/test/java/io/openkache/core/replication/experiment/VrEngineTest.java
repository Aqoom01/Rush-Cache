package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.vr.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class VrEngineTest {
    @Test void viewChangePrefersLastNormalViewBeforeLength() {
        try(var c=cluster()) {
            var shortLog=java.util.List.of(new VrMessage.Request(new ClientId(8),new OperationId(1),new Command.Set(Bytes.utf8("x"),Bytes.utf8("fresh"))));
            var longLog=java.util.stream.IntStream.range(0,5).mapToObj(i->new VrMessage.Request(new ClientId(9),new OperationId(i),new Command.Set(Bytes.utf8("old-"+i),Bytes.utf8("old")))).toList();
            c.nodes.get(0).onPeerMessage(new NodeId(1),new VrMessage.DoViewChange(3,1,longLog,0));
            c.nodes.get(0).onPeerMessage(new NodeId(2),new VrMessage.DoViewChange(3,2,shortLog,0));
            assertEquals(1,c.observation(0).logLength()); assertEquals(VrEngine.State.INSTALLING,c.observation(0).state());
        }
    }
    @Test void duplicateViewChangeReportIsNotAQuorum() {
        try(var c=cluster()) {
            var report=new VrMessage.DoViewChange(3,0,java.util.List.of(),0);
            for(int i=0;i<5;i++) c.nodes.get(0).onPeerMessage(new NodeId(1),report);
            assertEquals(VrEngine.State.VIEW_CHANGE,c.observation(0).state());
        }
    }
    @Test void droppedStartViewIsRetransmittedBeforeWritesResume() {
        try(var c=cluster()) {
            c.crash(0); c.drop=e->e.message() instanceof VrMessage.StartView; c.advance(150);
            assertNotEquals(VrEngine.State.NORMAL,c.observation(1).state());
            c.drop=e->false; c.advance(catchupMs()); int leader=c.leader(); assertTrue(leader>=0);
            ok(c,leader,1,"x","resumed");
        }
    }
    @Test void asynchronousCommittedApplySurvivesViewChangeWithoutLateReply() {
        var time=new io.openkache.core.replication.VirtualTimeSource();
        var applied=new java.util.concurrent.CompletableFuture<Result>();
        CacheEngine kv=new CacheEngine() {
            @Override public java.util.concurrent.CompletionStage<Result> execute(Command command,RequestContext context) { return applied; }
            @Override public void onPeerMessage(NodeId from,io.openkache.core.spi.PeerMessage message) {}
            @Override public void onTimer(io.openkache.core.runtime.TimerEvent event) {}
            @Override public EngineSnapshot snapshot() { return new EngineSnapshot(new NodeId(0),0,RoutingVersion.ZERO,OwnershipEpoch.ZERO); }
            @Override public void close() {}
        };
        var engine=new VrEngine(new NodeId(0),java.util.List.of(new NodeId(0),new NodeId(1),new NodeId(2)),kv,
                new io.openkache.core.replication.DirectMailbox(),e->java.util.concurrent.CompletableFuture.completedFuture(null),time,
                java.time.Duration.ofMillis(10),java.time.Duration.ofMillis(100));
        try {
            Command command=new Command.Set(Bytes.utf8("x"),Bytes.utf8("v"));
            var context=new RequestContext(new ClientId(1),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(ProtocolCluster.ms(1000)));
            var f=engine.execute(command,context).toCompletableFuture();
            engine.onPeerMessage(new NodeId(1),new VrMessage.PrepareOk(0,1)); assertFalse(f.isDone());
            engine.onPeerMessage(new NodeId(1),new VrMessage.StartViewChange(1));
            engine.onPeerMessage(new NodeId(1),new VrMessage.StartView(1,java.util.List.of(new VrMessage.Request(context.clientId(),context.operationId(),command)),1));
            applied.complete(Result.of(ResultStatus.OK,RoutingVersion.ZERO,OwnershipEpoch.ZERO));
            assertEquals(ResultStatus.UNAVAILABLE,f.join().status()); assertEquals(1,engine.observation().applied());
        } finally { engine.close(); }
    }
    protected ProtocolCluster cluster(long seed) { return new ProtocolCluster(ProtocolCluster.Variant.VR,1,100,seed); }
    private ProtocolCluster cluster() { return cluster(1); }
    protected int catchupMs() { return 300; }
    protected int electionMs() { return 350; }
    protected int requestWaitMs() { return 150; }
    protected void settleAfterHeal(ProtocolCluster cluster,String expected) { cluster.advance(catchupMs()); }
    protected void waitForElection(ProtocolCluster cluster,int isolated) { cluster.advance(electionMs()); }
    protected void writeRound(ProtocolCluster c,int primary,int round,int isolated) {
        var f=c.set(primary,round,"shared","value-"+round,1000); c.advance(requestWaitMs());
        assertTrue(f.isDone()); assertEquals(ResultStatus.OK,f.join().status());
    }
    private boolean prepare(io.openkache.core.spi.PeerMessage message) {
        return message instanceof VrMessage.Prepare || message instanceof VrMessage.DeltaPrepare;
    }
    private void ok(ProtocolCluster c,int node,long op,String key,String value) {
        var f=c.set(node,op,key,value,1000); c.advance(15); assertEquals(ResultStatus.OK,f.join().status());
    }
    private void value(ProtocolCluster c,int node,String key,String value) { assertEquals(Bytes.utf8(value),c.local(node,key).value().orElseThrow()); }

    @Test void quorumIgnoresSlowBackupAndCommitAppliesInOrder() {
        try(var c=cluster()) {
            c.slowAck=200;
            var f=c.set(0,1,"x","one",1000); c.advance(3); assertEquals(ResultStatus.OK,f.join().status());
            ok(c,0,2,"x","two");
            // 과반수 OK는 느린 Backup의 즉시 따라잡기까지 뜻하지 않는다.
            c.advance(catchupMs());
            for(int i=0;i<3;i++) value(c,i,"x","two");
        }
    }
    @Test void noQuorumDoesNotApplyAndReadsCannotBypassQuorum() {
        try(var c=cluster()) {
            ok(c,0,1,"x","old"); c.isolate(0);
            var write=c.set(0,2,"x","new",30); c.advance(31);
            assertEquals(ResultStatus.TIMEOUT,write.join().status()); value(c,0,"x","old");
            assertEquals(ResultStatus.BUSY,c.submit(0,3,new Command.Get(Bytes.utf8("x")),30).join().status());
        }
        try(var c=cluster()) {
            ok(c,0,1,"x","old"); c.isolate(0);
            var read=c.submit(0,2,new Command.Get(Bytes.utf8("x")),30); c.advance(31);
            assertEquals(ResultStatus.TIMEOUT,read.join().status());
        }
    }
    @Test void partitionMajorityWritesMinorityCannotAndHealingConverges() {
        try(var c=cluster()) {
            ok(c,0,1,"x","old"); c.isolate(0); c.advance(130);
            assertEquals(1,c.observation(1).view());
            var minority=c.set(0,2,"x","minority",40);
            ok(c,1,3,"x","majority"); c.advance(40);
            assertEquals(ResultStatus.TIMEOUT,minority.join().status());
            c.heal(); c.advance(catchupMs());
            for(int i=0;i<3;i++) value(c,i,"x","majority");
            assertEquals(c.observation(1).logLength(),c.observation(0).logLength());
        }
    }
    @Test void acknowledgedWriteSurvivesCandidateMissingLogAndLostCommitNotification() {
        try(var c=cluster()) {
            c.drop=e -> (e.to().value()==1 && prepare(e.message()))
                    || (e.message() instanceof VrMessage.Prepare p && p.commit()>0)
                    || (e.message() instanceof VrMessage.DeltaPrepare p && p.commit()>0)
                    || (e.message() instanceof VrMessage.Commit p && p.commit()>0);
            var f=c.set(0,1,"x","saved",1000); c.advance(3); assertEquals(ResultStatus.OK,f.join().status());
            assertEquals(ResultStatus.MISS,c.local(1,"x").status()); assertEquals(ResultStatus.MISS,c.local(2,"x").status());
            c.crash(0); c.drop=e->false; c.advance(140);
            value(c,1,"x","saved"); value(c,2,"x","saved");
        }
    }
    @Test void receivedButUnacknowledgedOperationIsRecoveredAndDeduplicated() {
        try(var c=cluster()) {
            c.drop=e->e.message() instanceof VrMessage.PrepareOk;
            var f=c.set(0,1,"x","pending",1000); c.advance(3);
            assertFalse(f.isDone()); assertEquals(ResultStatus.MISS,c.local(1,"x").status());
            c.crash(0); c.drop=e->false; c.advance(140);
            value(c,1,"x","pending"); int length=c.observation(1).logLength();
            ok(c,1,1,"x","pending"); assertEquals(length,c.observation(1).logLength());
        }
    }
    @Test void primaryOnlySuffixIsDiscardedAndSameRequestCanBeRetried() {
        try(var c=cluster()) {
            c.drop=e->prepare(e.message());
            var f=c.set(0,1,"x","pending",1000); c.advance(2); c.crash(0);
            c.drop=e->false; c.advance(140);
            assertEquals(ResultStatus.UNAVAILABLE,f.join().status()); assertEquals(ResultStatus.MISS,c.local(1,"x").status());
            ok(c,1,1,"x","pending"); value(c,2,"x","pending");
        }
    }
    @Test void timeoutCanLaterCommitWithoutSecondReply() {
        try(var c=cluster()) {
            c.slowAck=60; c.drop=e->e.from().value()==1 && e.message() instanceof VrMessage.PrepareOk;
            var f=c.set(0,1,"x","late",20); c.advance(21); assertEquals(ResultStatus.TIMEOUT,f.join().status());
            c.advance(70); value(c,0,"x","late"); assertEquals(ResultStatus.TIMEOUT,f.join().status());
            int length=c.observation(0).logLength(); ok(c,0,1,"x","late"); assertEquals(length,c.observation(0).logLength());
        }
    }
    @Test void duplicateRequestDifferentCommandIsRejectedIncludingPendingRequest() {
        try(var c=cluster()) {
            var f=c.set(0,1,"x","one",1000);
            assertEquals(ResultStatus.BAD_REQUEST,c.set(0,1,"x","two",1000).join().status());
            c.advance(10); assertEquals(ResultStatus.OK,f.join().status());
            assertEquals(ResultStatus.BAD_REQUEST,c.set(0,1,"x","two",1000).join().status());
        }
    }
    @Test void duplicateReorderedMessagesAndLossOfSecondReplicaStopsProgress() {
        try(var c=cluster()) {
            c.duplicate=true; c.jitter=5;
            ok(c,0,1,"x","one"); c.crash(0); c.advance(200);
            ok(c,1,2,"x","two"); c.isolate(1); c.advance(250);
            // 2개 영구 crash는 허용 f=1을 넘는다. 여기서는 quorum 부재가 진행을 막는지 확인한다.
            var f=c.set(1,3,"x","unsafe",30); c.advance(40);
            assertNotEquals(ResultStatus.OK,f.join().status()); value(c,2,"x","two");
        }
    }
    @Test void repeatedPartitionsLossDuplicationAndReorderingPreserveOverwriteHistory() {
        for(int seed=1;seed<=10;seed++) try(var c=cluster(seed)) {
            c.jitter=7; c.duplicate=true; c.drop=e->c.random.nextInt(12)==0;
            int primary=0;
            for(int round=1;round<=6;round++) {
                int isolated=primary; c.isolate(isolated); waitForElection(c,isolated);
                primary=-1;
                for(int i=0;i<3;i++) if(i!=isolated) {
                    var o=c.observation(i);
                    if(o.state()==VrEngine.State.NORMAL && o.view()%3==i) primary=i;
                }
                assertTrue(primary>=0,"majority did not establish view, seed="+seed);
                writeRound(c,primary,round,isolated);
                c.heal(); settleAfterHeal(c,"value-"+round);
                for(int i=0;i<3;i++) {
                    assertEquals(ResultStatus.OK,c.local(i,"shared").status(),"seed="+seed+" round="+round+" node="+i+" state="+c.observation(i));
                    assertEquals(Bytes.utf8("value-"+round),c.local(i,"shared").value().orElseThrow(),
                            "seed="+seed+" round="+round+" node="+i+" state="+c.observation(i)+" peer0="+c.observation(0)+" peer1="+c.observation(1)+" peer2="+c.observation(2));
                }
                primary=c.leader(); assertTrue(primary>=0);
            }
        }
    }
    @Test void isolatedBackupCannotFormQuorumAndHealedViewsEventuallySettle() {
        try(var c=cluster()) {
            c.isolate(2); ok(c,0,1,"x","one"); c.advance(350);
            assertEquals(VrEngine.State.VIEW_CHANGE,c.observation(2).state());
            ok(c,0,2,"x","two"); c.heal(); c.advance(250);
            int leader=c.leader(); assertTrue(leader>=0); ok(c,leader,3,"x","three");
            for(int i=0;i<3;i++) value(c,i,"x","three");
        }
    }
    @Test void readsDeleteAndCountShareOrderedLogAndDuplicateReply() {
        try(var c=cluster()) {
            ok(c,0,1,"x","one");
            var read=c.submit(0,2,new Command.Get(Bytes.utf8("x")),1000); c.advance(10); assertEquals(Bytes.utf8("one"),read.join().value().orElseThrow());
            ok(c,0,3,"x","two");
            var repeated=c.submit(0,2,new Command.Get(Bytes.utf8("x")),1000); assertEquals(read.join(),repeated.join());
            var delete=c.submit(0,4,new Command.Delete(Bytes.utf8("x")),1000); c.advance(10); assertEquals(ResultStatus.OK,delete.join().status());
            var count=c.submit(0,5,new Command.Count(),1000); c.advance(10); assertEquals(ResultStatus.OK,count.join().status());
            assertEquals(ResultStatus.MISS,c.local(2,"x").status());
        }
    }
}
