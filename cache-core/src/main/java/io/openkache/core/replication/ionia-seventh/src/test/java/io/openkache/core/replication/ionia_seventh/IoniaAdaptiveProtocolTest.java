package io.openkache.core.replication.ionia_seventh;
import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_seventh.IoniaAdaptiveTestSupport.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaAdaptiveProtocolTest {
    @Test void V05_13_isolatedOldLeaderCannotCertifyAReadAfterNewView(){
        for(int count:new int[]{3,5})try(var c=new Cluster(count)){
            c.client.execute(set("old"),context(1));c.pump();c.drop=e->e.from().equals(A)||e.to().equals(A);
            c.engines.get(B).startViewChange(1);c.engines.get(C).startViewChange(1);c.pump();
            try(var old=new IoniaAdaptiveReadClient(CLIENT,c.nodes,A,0,c,c.clock,IoniaReadSelectionTest.settings(IoniaReadSelectionSettings.Mode.ADAPTIVE))){
                var read=old.execute(new Command.Get(K),context(2));c.pump();assertEquals(ResultStatus.UNAVAILABLE,result(read).status());
            }
        }
    }
    @Test void V05_10_pendingMetaDataAndDeleteMissUseOriginalEngine(){
        try(var c=new Cluster(3)){
            c.stores.get(A).held.add(K);c.client.execute(set("v"),context(1));c.runOwners();
            var read=c.client.execute(new Command.Get(K),context(2));c.pump();assertFalse(read.toCompletableFuture().isDone());
            c.stores.get(A).release(K);c.pump();assertEquals(Bytes.utf8("v"),result(read).value().orElseThrow());assertTrue(c.client.diagnostics().metaDataReplies()>0);
            var deleted=c.client.execute(new Command.Delete(K),context(3));c.pump();assertEquals(ResultStatus.OK,result(deleted).status());
            var absent=c.client.execute(new Command.Get(K),context(4));c.pump();assertEquals(ResultStatus.MISS,result(absent).status());
        }
    }

    @Test void V05_09_10_capturedOldValueOrMissCannotPassLaterTrimmedMeta()throws Exception{
        for(boolean miss:new boolean[]{false,true})try(var c=new Cluster(3)){
            long op=1;if(!miss){c.client.execute(set("old"),context(op++));c.pump();}
            c.stores.get(B).holdReadReplies=true;var gate=new Gate(c);
            try(var reader=new IoniaAdaptiveReadClient(CLIENT,c.nodes,A,0,gate,c.clock,IoniaReadSelectionTest.settings(IoniaReadSelectionSettings.Mode.ADAPTIVE))){
                var ctx=new RequestContext(CLIENT,new OperationId(op++),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(10_000_000_000L));
                var read=reader.execute(new Command.Get(K),ctx);c.pump();
                c.client.execute(set("new"),context(op++));c.client.execute(new Command.Set(Bytes.utf8("other1"),Bytes.utf8("x")),context(op++));
                c.client.execute(new Command.Set(Bytes.utf8("other2"),Bytes.utf8("x")),context(op++));c.pump();c.clock.now=1_000_000_000L;c.tick();c.pump();
                c.stores.get(B).delayedReadReplies.forEach(Runnable::run);c.pump();assertFalse(read.toCompletableFuture().isDone());
                var field=IoniaEngine.class.getDeclaredField("history");field.setAccessible(true);IoniaHistory history=(IoniaHistory)field.get(c.engines.get(A));
                assertTrue(history.lastTrimmedIndex()>0);gate.release();c.pump();
                assertEquals(Bytes.utf8("new"),result(read).value().orElseThrow());assertEquals(miss?ResultStatus.MISS:ResultStatus.OK,gate.captured.result().status());
                assertTrue(gate.captured.appliedIndex()<gate.meta.index());assertEquals(1,reader.diagnostics().leaderGetCalls());
            }
        }
    }
    private static final class Gate implements IoniaClientTransport {
        final Cluster c;final java.util.concurrent.CompletableFuture<MetaReply> held=new java.util.concurrent.CompletableFuture<>();Request request;NodeId to;ReadReply captured;MetaReply meta;
        Gate(Cluster c){this.c=c;}
        public java.util.concurrent.CompletionStage<WriteAck> record(NodeId n,Request r){return c.record(n,r);}
        public java.util.concurrent.CompletionStage<Reply> execute(NodeId n,Request r){return c.execute(n,r);}
        public java.util.concurrent.CompletionStage<ReadReply> followerRead(NodeId n,Request r){return c.followerRead(n,r).thenApply(value->{captured=value;return value;});}
        public java.util.concurrent.CompletionStage<MetaReply> metaQuery(NodeId n,Request r){to=n;request=r;return held;}
        public java.util.concurrent.CompletionStage<IoniaEngine.ViewInfo> probe(NodeId n){return c.probe(n);}
        void release(){c.metaQuery(to,request).whenComplete((v,e)->{meta=v;if(e!=null)held.completeExceptionally(e);else held.complete(v);});}
    }
}
