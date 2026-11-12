package io.openkache.core.replication.ionia_eighth;
import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_eighth.IoniaMetaBatchTestSupport.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaMetaBatchProtocolTest {
    @Test void V06_13_isolatedOldLeaderCannotCertifyAReadAfterNewView(){
        for(int count:new int[]{3,5})try(var c=new Cluster(count)){
            c.client.execute(set("old"),context(1));c.pump();c.drop=e->e.from().equals(A)||e.to().equals(A);
            c.engines.get(B).startViewChange(1);c.engines.get(C).startViewChange(1);c.pump();
            try(var old=new IoniaMetaBatchClient(CLIENT,c.nodes,A,0,c,c.clock,new IoniaMetaBatchSettings(1,65536,0,256,4))){
                var read=old.execute(new Command.Get(K),context(2));c.pump();assertEquals(ResultStatus.UNAVAILABLE,result(read).status());
            }
        }
    }
    @Test void V06_10_pendingMetaDataAndDeleteMissUseOriginalEngine(){
        try(var c=new Cluster(3)){
            c.stores.get(A).held.add(K);c.client.execute(set("v"),context(1));c.runOwners();
            var read=c.client.execute(new Command.Get(K),context(2));c.pump();assertFalse(read.toCompletableFuture().isDone());
            c.stores.get(A).release(K);c.pump();assertEquals(Bytes.utf8("v"),result(read).value().orElseThrow());
            var deleted=c.client.execute(new Command.Delete(K),context(3));c.pump();assertEquals(ResultStatus.OK,result(deleted).status());
            var absent=c.client.execute(new Command.Get(K),context(4));c.pump();assertEquals(ResultStatus.MISS,result(absent).status());
        }
    }

    @Test void V06_09_10_capturedOldValueOrMissCannotPassLaterTrimmedMeta()throws Exception{
        for(boolean miss:new boolean[]{false,true})try(var c=new Cluster(3)){
            long op=1;if(!miss){c.client.execute(set("old"),context(op++));c.pump();}
            c.stores.get(B).holdReadReplies=true;var gate=new Gate(c);
            try(var reader=new IoniaMetaBatchClient(CLIENT,c.nodes,A,0,gate,c.clock,new IoniaMetaBatchSettings(1,65536,0,256,4))){
                var ctx=new RequestContext(CLIENT,new OperationId(op++),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(10_000_000_000L));
                var read=reader.execute(new Command.Get(K),ctx);c.pump();
                c.client.execute(set("new"),context(op++));c.client.execute(new Command.Set(Bytes.utf8("other1"),Bytes.utf8("x")),context(op++));
                c.client.execute(new Command.Set(Bytes.utf8("other2"),Bytes.utf8("x")),context(op++));c.pump();c.clock.now=1_000_000_000L;c.tick();c.pump();
                c.stores.get(B).delayedReadReplies.forEach(Runnable::run);c.pump();assertFalse(read.toCompletableFuture().isDone());
                var field=IoniaEngine.class.getDeclaredField("history");field.setAccessible(true);IoniaHistory history=(IoniaHistory)field.get(c.engines.get(A));
                assertTrue(history.lastTrimmedIndex()>0);gate.release();c.pump();
                assertEquals(Bytes.utf8("new"),result(read).value().orElseThrow());assertEquals(miss?ResultStatus.MISS:ResultStatus.OK,gate.captured.result().status());
                assertTrue(gate.captured.appliedIndex()<gate.meta.index());assertTrue(c.executed.contains(A));
            }
        }
    }
    private static final class Gate implements IoniaMetaBatchTransport {
        final Cluster c;final java.util.concurrent.CompletableFuture<List<Answer>> held=new java.util.concurrent.CompletableFuture<>();Request request;NodeId to;ReadReply captured;MetaReply meta;long token;
        Gate(Cluster c){this.c=c;}
        public java.util.concurrent.CompletionStage<WriteAck> record(NodeId n,Request r){return c.record(n,r);}
        public java.util.concurrent.CompletionStage<Reply> execute(NodeId n,Request r){return c.execute(n,r);}
        public java.util.concurrent.CompletionStage<ReadReply> followerRead(NodeId n,Request r){return c.followerRead(n,r).thenApply(value->{captured=value;return value;});}
        public java.util.concurrent.CompletionStage<List<Answer>> metaBatch(NodeId n,List<Item> items){assertEquals(1,items.size());to=n;request=items.getFirst().request();token=items.getFirst().token();return held;}
        public java.util.concurrent.CompletionStage<IoniaEngine.ViewInfo> probe(NodeId n){return c.probe(n);}
        void release(){c.metaQuery(to,request).whenComplete((v,e)->{meta=v;if(e!=null)held.completeExceptionally(e);else held.complete(List.of(new Answer(token,v)));});}
    }
}
