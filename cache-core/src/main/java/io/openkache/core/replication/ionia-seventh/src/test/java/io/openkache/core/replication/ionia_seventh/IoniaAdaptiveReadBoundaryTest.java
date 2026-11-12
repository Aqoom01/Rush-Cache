package io.openkache.core.replication.ionia_seventh;
import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_seventh.IoniaReadSelectionTest.*;
import static io.openkache.core.replication.ionia_seventh.IoniaAdaptiveReadClientTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;

@Timeout(15)
class IoniaAdaptiveReadBoundaryTest {
    @Test void V05_04_rawRttClampEwmaAndReceiptAge(){
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,64,32)){
            var first=get(c,t,1);w.reply(0,0,value("zero-clock"));first.join();assertEquals(1,node(c,w.reads.getFirst().node()).ewmaNanos());
            var second=get(c,t,2);t.now=100;w.reply(1,0,value("sample"));second.join();assertEquals(13,node(c,w.reads.getFirst().node()).ewmaNanos());
            var third=get(c,t,3);t.now=40_000_000_100L;w.reply(2,0,value("late"));third.join();
            assertTrue(node(c,w.reads.get(2).node()).ewmaNanos()<=IoniaReadSelectionSettings.SAMPLE_MAX_NANOS);accounting(c);
        }
    }
    @Test void V05_12_dispatchSequenceExhaustionClosesWithoutNewWireCalls()throws Exception{
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,4,2)){
            var field=IoniaAdaptiveReadClient.class.getDeclaredField("selector");field.setAccessible(true);((ReplicaSelector)field.get(c)).nextSequence=Long.MAX_VALUE;
            assertEquals(ResultStatus.UNAVAILABLE,get(c,t,1).join().status());assertTrue(c.diagnostics().closed());assertTrue(w.reads.isEmpty());accounting(c);
            assertEquals(ResultStatus.UNAVAILABLE,get(c,t,2).join().status());assertEquals(0,w.metas);assertEquals(0,w.probes);
        }
    }
    @SuppressWarnings("try") // close 도중 미완료 refresh를 검증하기 위한 의도적 조기 종료
    @Test void V05_11_oldRefreshCannotRollBackNewViewAndCloseCompletesPendingRefresh(){
        Clock t=new Clock();Wire w=new Wire();List<CompletableFuture<IoniaEngine.ViewInfo>> replies=new ArrayList<>();
        var borrowed=new IoniaClientTransport(){
            public CompletionStage<WriteAck> record(NodeId n,Request r){return w.record(n,r);}public CompletionStage<Reply> execute(NodeId n,Request r){return w.execute(n,r);}
            public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId n){var f=new CompletableFuture<IoniaEngine.ViewInfo>();replies.add(f);return f;}
        };
        try(var c=new IoniaAdaptiveReadClient(ID,IDS,IDS.getFirst(),0,borrowed,t,settings(IoniaReadSelectionSettings.Mode.ADAPTIVE))){
            var old=c.refreshView().toCompletableFuture();c.updateView(1,IDS.get(1));for(var f:replies)f.complete(new IoniaEngine.ViewInfo(0,IDS.getFirst(),IoniaEngine.Status.NORMAL));
            assertThrows(CompletionException.class,old::join);assertEquals(1,c.diagnostics().view());
            var pending=c.refreshView().toCompletableFuture();c.close();assertThrows(CompletionException.class,pending::join);
            assertEquals(1,c.diagnostics().view());
        }
    }
    @Test void V05_14_synchronousTransportReentryDefersPublicCallbacksOutsideDispatchLock()throws Exception{
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,4,2);var worker=Executors.newSingleThreadExecutor()){
            var old=get(c,t,1);AtomicBoolean called=new AtomicBoolean();
            var callback=old.thenRun(()->{try{worker.submit(c::diagnostics).get(2,TimeUnit.SECONDS);called.set(true);}catch(Exception e){throw new CompletionException(e);}});
            w.readAction=call->{w.reply(0,0,value("old"));return call.future();};get(c,t,2);callback.get(3,TimeUnit.SECONDS);assertTrue(called.get());accounting(c);
        }
    }
    @Test void V05_03_06_rawFailureCooldownExpiresWithoutExtraProbeTraffic(){
        Wire w=new Wire();Clock t=new Clock();w.data=value("meta");try(var c=client(w,t,64,32)){
            get(c,t,1).join();NodeId first=w.reads.getFirst().node();w.reads.getFirst().future().completeExceptionally(new IllegalStateException("failure"));
            for(int i=2;i<5;i++){get(c,t,i).join();assertNotEquals(first,w.reads.getLast().node());w.reply(w.reads.size()-1,0,value("ok"));}
            int probesBefore=w.probes;t.now=250_000_001;boolean revisited=false;
            for(int i=5;i<22;i++){get(c,t,i).join();revisited|=w.reads.getLast().node().equals(first);w.reply(w.reads.size()-1,0,value("recovered"));}
            assertTrue(revisited);assertEquals(17*IDS.size(),w.probes-probesBefore);accounting(c);
        }
    }

    @Test void V05_10_followerReplyBeforeMetaAndForgedMetaStillUsesOriginalValidation(){
        for(boolean forged:new boolean[]{false,true}){
            Clock t=new Clock();CompletableFuture<IoniaClientTransport.MetaReply> meta=new CompletableFuture<>();
            Wire w=new Wire(){@Override public CompletionStage<MetaReply> metaQuery(NodeId n,Request r){metas++;return meta;}};
            try(var c=client(w,t,4,2)){
                var read=get(c,t,1);w.reply(0,0,value("follower"));assertFalse(read.isDone());var r=w.reads.getFirst().request();
                meta.complete(new IoniaClientTransport.MetaReply(IDS.getFirst(),0,forged?new ClientId(99):r.clientId(),r.operationId(),0,null));
                assertEquals(forged?ResultStatus.UNAVAILABLE:ResultStatus.OK,read.join().status());accounting(c);
            }
        }
    }
}
