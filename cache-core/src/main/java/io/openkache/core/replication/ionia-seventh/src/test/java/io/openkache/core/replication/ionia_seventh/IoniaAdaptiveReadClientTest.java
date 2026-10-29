package io.openkache.core.replication.ionia_seventh;
import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_seventh.IoniaReadSelectionTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import org.junit.jupiter.api.*;

@Timeout(15)
class IoniaAdaptiveReadClientTest {
    static final ClientId ID=new ClientId(0);static final Bytes K=Bytes.utf8("k");
    static class Clock implements TimeSource {long now;public long monotonicNanos(){return now;}public long epochMillis(){return 0;}}
    record Call(NodeId node,IoniaClientTransport.Request request,CompletableFuture<IoniaClientTransport.ReadReply> future) { }
    static class Wire implements IoniaClientTransport {
        final List<Call> reads=new ArrayList<>();int metas,probes,records,executes;long view,metaIndex;boolean failMeta,failProbes;
        Result data;Function<Call,CompletionStage<ReadReply>> readAction=c->c.future();
        public CompletionStage<WriteAck> record(NodeId n,Request r){records++;return CompletableFuture.completedFuture(new WriteAck(n,r.view(),r.clientId(),r.operationId(),ResultStatus.OK));}
        public CompletionStage<Reply> execute(NodeId n,Request r){executes++;return CompletableFuture.completedFuture(new Reply(n,r.view(),r.clientId(),r.operationId(),value("leader")));}
        public CompletionStage<ReadReply> followerRead(NodeId n,Request r){Call c=new Call(n,r,new CompletableFuture<>());reads.add(c);return readAction.apply(c);}
        public CompletionStage<MetaReply> metaQuery(NodeId n,Request r){metas++;return failMeta?CompletableFuture.failedFuture(new IOException("meta")):CompletableFuture.completedFuture(new MetaReply(n,r.view(),r.clientId(),r.operationId(),metaIndex,data));}
        public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId n){probes++;return failProbes&&!n.equals(IDS.getFirst())?CompletableFuture.failedFuture(new IOException("probe")):CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(view,IDS.get((int)(view%3)),IoniaEngine.Status.NORMAL));}
        void reply(int index,long applied,Result value){Call c=reads.get(index);c.future().complete(new ReadReply(c.node(),c.request().view(),c.request().clientId(),c.request().operationId(),applied,value));}
    }
    static Result value(String v){return Result.ok(Bytes.utf8(v),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
    static RequestContext context(Clock t,long id){return new RequestContext(ID,new OperationId(id),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(t.now+1_000_000_000));}
    static IoniaAdaptiveReadClient client(Wire w,Clock t,int l,int p){return new IoniaAdaptiveReadClient(ID,IDS,IDS.getFirst(),0,w,t,new IoniaReadSelectionSettings(IoniaReadSelectionSettings.Mode.ADAPTIVE,l,p,1,1000,250,16));}
    static CompletableFuture<Result> get(IoniaAdaptiveReadClient c,Clock t,long id){return c.execute(new Command.Get(K),context(t,id)).toCompletableFuture();}
    static IoniaAdaptiveReadClient.NodeDiagnostics node(IoniaAdaptiveReadClient c,NodeId n){return c.diagnostics().nodes().stream().filter(s->s.node().equals(n)).findFirst().orElseThrow();}
    static void accounting(IoniaAdaptiveReadClient c){var d=c.diagnostics();assertEquals(d.logicalStarts(),d.logicalTerminal()+d.activeLogical());for(var s:d.nodes())assertEquals(s.rawStarts(),s.rawTerminal()+s.rawPending());}
    @Test void V05_03_metaDataDoesNotTrainIncompleteFollowerOrFreeItsSlot(){
        Wire w=new Wire();Clock t=new Clock();w.data=value("meta");
        try(var c=client(w,t,4,1)){
            assertEquals(Bytes.utf8("meta"),get(c,t,1).join().value().orElseThrow());assertEquals(0,c.diagnostics().activeLogical());
            assertEquals(1,node(c,w.reads.getFirst().node()).rawPending());assertFalse(node(c,w.reads.getFirst().node()).fresh());
            get(c,t,2).join();int calls=w.reads.size();assertEquals(ResultStatus.BUSY,get(c,t,3).join().status());assertEquals(calls,w.reads.size());
            t.now+=10_000_000;w.reply(0,0,value("raw"));assertEquals(10_000_000,node(c,w.reads.getFirst().node()).ewmaNanos());accounting(c);
        }
    }
    @Test void V05_08_publicCancellationDoesNotFreeLogicalOrRawAdmission(){
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,1,1)){
            var read=get(c,t,1);assertTrue(read.cancel(false));assertFalse(w.reads.getFirst().future().isCancelled());
            assertEquals(ResultStatus.BUSY,get(c,t,2).join().status());assertEquals(1,c.diagnostics().activeLogical());
            w.reply(0,0,value("done"));assertEquals(0,c.diagnostics().activeLogical());assertEquals(1,c.diagnostics().publicCancels());accounting(c);
        }
    }
    @Test void V05_08_timeoutReturnsLogicalButKeepsRawStagePending(){
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,1,1)){
            var read=get(c,t,1);t.now=1_000_000_001;c.onTimer(new TimerEvent(IoniaEngine.TICK,t.now));assertEquals(ResultStatus.TIMEOUT,read.join().status());
            assertEquals(0,c.diagnostics().activeLogical());assertEquals(1,node(c,w.reads.getFirst().node()).rawPending());
            w.reply(0,0,value("late"));accounting(c);assertEquals(ResultStatus.TIMEOUT,read.join().status());
        }
    }
    @Test void V05_09_fastStaleValueAndMissBothRequireFreshMetaFallback(){
        for(Result raw:List.of(value("old"),Result.of(ResultStatus.MISS,RoutingVersion.ZERO,OwnershipEpoch.ZERO))){Wire w=new Wire();Clock t=new Clock();w.metaIndex=1;
            try(var c=client(w,t,4,2)){var read=get(c,t,1);w.reply(0,0,raw);assertEquals(Bytes.utf8("leader"),read.join().value().orElseThrow());assertEquals(1,w.executes);assertEquals(1,c.diagnostics().leaderGetCalls());}
        }
    }
    @Test void V05_10_equalAndGreaterAppliedIndexesAcceptIncludingMiss(){
        for(long index:new long[]{3,4})for(boolean miss:new boolean[]{false,true}){Wire w=new Wire();Clock t=new Clock();w.metaIndex=3;
            try(var c=client(w,t,4,2)){var read=get(c,t,1);Result raw=miss?Result.of(ResultStatus.MISS,RoutingVersion.ZERO,OwnershipEpoch.ZERO):value("fresh");w.reply(0,index,raw);assertEquals(raw,read.join());assertEquals(0,w.executes);}
        }
    }
    @Test void V05_07_throwNullExceptionCancelAndBadRepliesNeverTrainSuccess(){
        for(String fault:List.of("throw","null","exception","cancel","BUSY","UNAVAILABLE","TIMEOUT","node","view","client","operation")){
            Wire w=new Wire();Clock t=new Clock();w.readAction=call->{
                if(fault.equals("throw"))throw new IllegalStateException("injected");if(fault.equals("null"))return null;
                if(fault.equals("exception"))return CompletableFuture.failedFuture(new IOException("injected"));
                if(fault.equals("cancel")){call.future().cancel(false);return call.future();}
                var r=call.request();Result raw=List.of("BUSY","UNAVAILABLE","TIMEOUT").contains(fault)?Result.of(ResultStatus.valueOf(fault),RoutingVersion.ZERO,OwnershipEpoch.ZERO):value("bad");
                return CompletableFuture.completedFuture(new IoniaClientTransport.ReadReply(fault.equals("node")?IDS.getFirst():call.node(),fault.equals("view")?1:r.view(),
                        fault.equals("client")?new ClientId(99):r.clientId(),fault.equals("operation")?new OperationId(99):r.operationId(),0,raw));};
            try(var c=client(w,t,4,2)){get(c,t,1).join();var n=node(c,w.reads.getFirst().node());assertFalse(n.fresh(),fault);assertEquals(1,n.rawFailure(),fault);assertEquals(0,n.rawPending(),fault);assertTrue(n.cooldownRemainingNanos()>0,fault);accounting(c);}
        }
    }
    @Test void V05_11_oldViewCallbacksOnlyReleaseRawSlots(){
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,4,1)){
            var read=get(c,t,1);NodeId target=w.reads.getFirst().node();w.view=1;c.updateView(1,IDS.get(1));w.reply(0,0,value("old"));
            assertEquals(ResultStatus.UNAVAILABLE,read.join().status());var n=node(c,target);assertEquals(0,n.rawPending());assertEquals(1,n.ignoredGeneration());assertFalse(n.fresh());assertEquals(0,n.cooldownRemainingNanos());accounting(c);
        }
    }
    @Test void V05_11_sameViewRefreshKeepsLearningAndWrongLeaderIsRejected(){
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,4,2)){
            var r=get(c,t,1);t.now=5;w.reply(0,0,value("ok"));r.join();long before=node(c,w.reads.getFirst().node()).ewmaNanos();
            c.refreshView().toCompletableFuture().join();assertEquals(before,node(c,w.reads.getFirst().node()).ewmaNanos());assertEquals(0,c.diagnostics().generation());
            assertThrows(IllegalArgumentException.class,()->c.updateView(1,IDS.get(2)));assertThrows(IllegalArgumentException.class,()->c.updateView(-1,IDS.getFirst()));
        }
    }
    @Test void V05_12_olderSuccessCannotEraseNewFailureCooldown(){
        Wire w=new Wire();Clock t=new Clock();w.data=value("meta");try(var c=client(w,t,64,32)){
            for(int i=1;i<=3;i++)get(c,t,i).join();assertEquals(w.reads.get(0).node(),w.reads.get(2).node());
            w.reads.get(2).future().completeExceptionally(new IOException("new failure"));w.reply(0,0,value("older success"));
            var n=node(c,w.reads.getFirst().node());assertFalse(n.fresh());assertEquals(1,n.ignoredSequence());assertTrue(n.cooldownRemainingNanos()>0);accounting(c);
        }
    }
    @Test void V05_13_metaOrViewCertificateFailureNeverReturnsFollowerValue(){
        for(boolean meta:new boolean[]{false,true}){Wire w=new Wire();Clock t=new Clock();w.failMeta=meta;w.failProbes=!meta;
            try(var c=client(w,t,4,2)){var read=get(c,t,1);w.reply(0,0,value("follower"));assertEquals(ResultStatus.UNAVAILABLE,read.join().status());accounting(c);}
        }
    }
    @Test void V05_14_writeFastPathDeleteAndCloseKeepOriginalContract(){
        Wire w=new Wire();Clock t=new Clock();var c=client(w,t,4,2);
        assertEquals(ResultStatus.OK,c.execute(new Command.Set(K,Bytes.utf8("v")),context(t,1)).toCompletableFuture().join().status());assertEquals(3,w.records);assertTrue(w.reads.isEmpty());
        c.execute(new Command.Delete(K),context(t,2)).toCompletableFuture().join();assertEquals(1,w.executes);
        var read=get(c,t,3);c.close();assertEquals(ResultStatus.UNAVAILABLE,read.join().status());assertEquals(1,node(c,w.reads.getFirst().node()).rawPending());
        assertEquals(ResultStatus.UNAVAILABLE,get(c,t,4).join().status());w.reply(0,0,value("late"));assertEquals(0,node(c,w.reads.getFirst().node()).rawPending());accounting(c);c.close();
    }
    @Test void V05_05_concurrentCallsCannotExceedRawOrLogicalCaps()throws Exception{
        Wire w=new Wire();Clock t=new Clock();w.data=value("meta");try(var c=client(w,t,4,1);var threads=Executors.newVirtualThreadPerTaskExecutor()){
            List<Future<Result>> calls=new ArrayList<>();for(int i=0;i<32;i++){long id=i+1;calls.add(threads.submit(()->get(c,t,id).join()));}
            int ok=0,busy=0;for(var f:calls){ResultStatus s=f.get(5,TimeUnit.SECONDS).status();if(s==ResultStatus.OK)ok++;else{assertEquals(ResultStatus.BUSY,s);busy++;}}
            assertEquals(2,ok);assertEquals(30,busy);assertEquals(2,w.reads.size());for(var n:c.diagnostics().nodes())assertTrue(n.rawPending()<=1);accounting(c);
        }
    }
    @Test void V05_05_sameLogicalIdentityDoesNotOverwriteRawCompletionTokens(){
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,4,2)){
            var first=get(c,t,1);var second=get(c,t,1);w.reply(1,0,value("two"));w.reply(0,0,value("one"));assertTrue(first.isDone());assertTrue(second.isDone());accounting(c);
        }
    }
    @Test void V05_14_invalidContextAndClosedClientDispatchNoRpc(){
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,4,2)){
            var invalid=new RequestContext(new ClientId(9),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(10));
            assertEquals(ResultStatus.BAD_REQUEST,c.execute(new Command.Get(K),invalid).toCompletableFuture().join().status());
            var expired=new RequestContext(ID,new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(0));assertEquals(ResultStatus.TIMEOUT,c.execute(new Command.Get(K),expired).toCompletableFuture().join().status());
            assertTrue(w.reads.isEmpty());assertEquals(0,w.probes);
        }
    }
    @Test void V05_14_publicCompletionCallbackRunsOutsideControlLock()throws Exception{
        Wire w=new Wire();Clock t=new Clock();try(var c=client(w,t,4,2);var executor=Executors.newSingleThreadExecutor()){
            var read=get(c,t,1);var callback=read.thenRun(()->{try{executor.submit(c::diagnostics).get(2,TimeUnit.SECONDS);}catch(Exception e){throw new CompletionException(e);}});
            w.reply(0,0,value("ok"));callback.get(3,TimeUnit.SECONDS);
        }
    }
}
