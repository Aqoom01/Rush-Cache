package io.openkache.core.replication.ionia_eighth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

@Timeout(15)
class IoniaMetaBatchTest {
    static final NodeId A=new NodeId(1),B=new NodeId(2);
    static final ClientId C=new ClientId(7);
    static class Clock implements TimeSource{long now;public long monotonicNanos(){return now;}public long epochMillis(){return 0;}}
    static class Raw implements IoniaMetaBatchTransport{
        record Call(NodeId to,List<Item> items,CompletableFuture<List<Answer>> future){}
        final List<Call> calls=new ArrayList<>(); int failure;
        public CompletionStage<WriteAck> record(NodeId to,Request r){return CompletableFuture.failedFuture(new UnsupportedOperationException());}
        public CompletionStage<Reply> execute(NodeId to,Request r){return CompletableFuture.failedFuture(new UnsupportedOperationException());}
        public CompletionStage<List<Answer>> metaBatch(NodeId to,List<Item> items){
            if(failure==1)throw new IllegalStateException("fixture");if(failure==2)return null;
            var f=new CompletableFuture<List<Answer>>();calls.add(new Call(to,items,f));if(failure==3)f.completeExceptionally(new IllegalStateException("fixture"));return f;
        }
        List<Answer> answers(int index){var call=calls.get(index);return call.items().stream().map(i->new Answer(i.token(),new MetaReply(call.to(),i.request().view(),i.request().clientId(),i.request().operationId(),i.request().operationId().value(),null))).toList();}
        void done(int index){calls.get(index).future().complete(answers(index));}
    }
    static IoniaMetaBatchSettings settings(int e,int bytes,long delay,int p,int w){return new IoniaMetaBatchSettings(e,bytes,delay,p,w);}
    static IoniaClientTransport.Request request(long id,long view,long timeout,String key){return new IoniaClientTransport.Request(view,new Command.Get(Bytes.utf8(key)),C,new OperationId(id),RoutingVersion.ZERO,OwnershipEpoch.ZERO,timeout);}
    static IoniaClientTransport.Request request(long id){return request(id,0,1000,"k");}
    static ResultStatus status(CompletionStage<IoniaClientTransport.MetaReply> f){return f.toCompletableFuture().join().data().status();}

    @Test void countAndTokensKeepDuplicateLogicalIdsSeparate(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(2,1000,10,8,2))){
            var first=batch.metaQuery(A,request(1));assertTrue(raw.calls.isEmpty());
            var second=batch.metaQuery(A,request(1));assertEquals(1,raw.calls.size());assertNotEquals(raw.calls.getFirst().items().getFirst().token(),raw.calls.getFirst().items().getLast().token());
            raw.done(0);assertNull(first.toCompletableFuture().join().data());assertNull(second.toCompletableFuture().join().data());assertEquals(0,batch.diagnostics().outstanding());
        }
    }
    @Test void byteBoundaryFlushesPrefixAndRejectsOversize(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(4,128,10,8,2))){
            var a=batch.metaQuery(A,request(1));var b=batch.metaQuery(A,request(2));var c=batch.metaQuery(A,request(3));
            assertEquals(1,raw.calls.size());assertEquals(2,raw.calls.getFirst().items().size());raw.done(0);
            clock.now=10;batch.onTimer();assertEquals(1,raw.calls.getLast().items().size());raw.done(1);
            assertTrue(a.toCompletableFuture().isDone()&&b.toCompletableFuture().isDone()&&c.toCompletableFuture().isDone());
            assertEquals(ResultStatus.BAD_REQUEST,status(batch.metaQuery(A,request(4,0,1000,"x".repeat(64)))));
        }
    }
    @Test void timerAndNearDeadlineFlushWithoutExtendingBudget(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(8,1000,10,8,2))){
            batch.metaQuery(A,request(1));clock.now=9;batch.onTimer();assertTrue(raw.calls.isEmpty());
            clock.now=10;batch.onTimer();assertEquals(990,raw.calls.getFirst().items().getFirst().request().timeoutNanos());raw.done(0);
            batch.metaQuery(A,request(2,0,5,"k"));assertEquals(2,raw.calls.size());raw.done(1);
        }
    }
    @Test void inFlightAndPendingLimitsHoldAcrossCancelAndExpiry(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,2,1))){
            var a=batch.metaQuery(A,request(1));a.toCompletableFuture().cancel(false);var b=batch.metaQuery(A,request(2));
            assertEquals(ResultStatus.BUSY,status(batch.metaQuery(A,request(3))));assertEquals(1,raw.calls.size());
            clock.now=1000;batch.onTimer();assertEquals(ResultStatus.TIMEOUT,status(b));assertEquals(1,batch.diagnostics().outstanding());assertEquals(1,batch.diagnostics().inFlight());
            raw.done(0);assertEquals(0,batch.diagnostics().outstanding());assertEquals(0,batch.diagnostics().inFlight());
        }
    }
    @Test void rawTimeoutKeepsPermitUntilActualTerminal(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,1,1))){
            var a=batch.metaQuery(A,request(1));clock.now=1000;batch.onTimer();assertEquals(ResultStatus.TIMEOUT,status(a));
            assertEquals(1,batch.diagnostics().outstanding());assertEquals(ResultStatus.BUSY,status(batch.metaQuery(A,request(2))));
            raw.done(0);assertEquals(0,batch.diagnostics().outstanding());
        }
    }
    @Test void viewAdvanceFencesQueuedAndLateInFlightWithoutMixingGroups(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(2,1000,10,8,2))){
            var a=batch.metaQuery(A,request(1));var b=batch.metaQuery(A,request(2));var queued=batch.metaQuery(A,request(3));
            batch.advanceView(1);assertEquals(ResultStatus.UNAVAILABLE,status(a));assertEquals(ResultStatus.UNAVAILABLE,status(b));assertEquals(ResultStatus.UNAVAILABLE,status(queued));
            assertEquals(2,batch.diagnostics().outstanding());var fresh=batch.metaQuery(B,request(4,1,1000,"k"));batch.advanceView(1);
            assertFalse(fresh.toCompletableFuture().isDone());clock.now=10;batch.onTimer();raw.done(0);raw.done(1);
            assertEquals(B,fresh.toCompletableFuture().join().nodeId());assertEquals(0,batch.diagnostics().outstanding());
            assertThrows(IllegalArgumentException.class,()->batch.advanceView(0));
            assertEquals(ResultStatus.UNAVAILABLE,status(batch.metaQuery(A,request(5))));
        }
    }
    @Test void reorderedAnswersMapByOccurrenceToken(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(2,1000,10,8,1))){
            var a=batch.metaQuery(A,request(10));var b=batch.metaQuery(A,request(20));
            var answers=new ArrayList<>(raw.answers(0));Collections.reverse(answers);raw.calls.getFirst().future().complete(answers);
            assertEquals(10,a.toCompletableFuture().join().index());assertEquals(20,b.toCompletableFuture().join().index());
        }
    }
    @Test void malformedEnvelopeCannotPartiallyPublishValidPrefix(){
        for(int kind=0;kind<8;kind++){
            Raw raw=new Raw();Clock clock=new Clock();
            try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(2,1000,10,8,1))){
                var a=batch.metaQuery(A,request(10));var b=batch.metaQuery(A,request(20));var answers=new ArrayList<>(raw.answers(0));var r=answers.getLast().reply();long token=answers.getLast().token();
                switch(kind){
                    case 0->answers.removeLast();case 1->answers.set(1,answers.getFirst());case 2->answers.set(1,new IoniaMetaBatchTransport.Answer(999,r));
                    case 3->answers.set(1,new IoniaMetaBatchTransport.Answer(token,new IoniaClientTransport.MetaReply(B,0,C,r.operationId(),0,null)));
                    case 4->answers.set(1,new IoniaMetaBatchTransport.Answer(token,new IoniaClientTransport.MetaReply(A,1,C,r.operationId(),0,null)));
                    case 5->answers.set(1,new IoniaMetaBatchTransport.Answer(token,new IoniaClientTransport.MetaReply(A,0,new ClientId(99),r.operationId(),0,null)));
                    case 6->answers.set(1,new IoniaMetaBatchTransport.Answer(token,new IoniaClientTransport.MetaReply(A,0,C,new OperationId(99),0,null)));
                    case 7->answers.set(1,null);
                }
                raw.calls.getFirst().future().complete(answers);assertEquals(ResultStatus.UNAVAILABLE,status(a));assertEquals(ResultStatus.UNAVAILABLE,status(b));assertEquals(1,batch.diagnostics().malformedBatches());
            }
        }
    }
    @Test void throwingNullExceptionalAndCancelledStagesReleaseExactlyOnce(){
        for(int failure=1;failure<=5;failure++){
            Raw raw=new Raw();raw.failure=Math.min(failure,3);if(failure>3)raw.failure=0;Clock clock=new Clock();
            try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,1,1))){
                var a=batch.metaQuery(A,request(1));if(failure==4)raw.calls.getFirst().future().cancel(false);if(failure==5)raw.calls.getFirst().future().complete(null);
                assertEquals(ResultStatus.UNAVAILABLE,status(a));assertEquals(0,batch.diagnostics().outstanding());assertEquals(0,batch.diagnostics().inFlight());
            }
        }
    }
    @Test void closePublishesUnavailableButRetainsRawPermits(){
        Raw raw=new Raw();Clock clock=new Clock();var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,3,1));
        var a=batch.metaQuery(A,request(1));var b=batch.metaQuery(A,request(2));batch.close();batch.close();
        assertEquals(ResultStatus.UNAVAILABLE,status(a));assertEquals(ResultStatus.UNAVAILABLE,status(b));assertEquals(1,batch.diagnostics().outstanding());
        assertEquals(ResultStatus.UNAVAILABLE,status(batch.metaQuery(A,request(3))));raw.done(0);assertEquals(0,batch.diagnostics().outstanding());
    }
    @Test void callbacksAndSynchronousReentryRunOutsideControlLock() throws Exception{
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,3,1));var executor=Executors.newVirtualThreadPerTaskExecutor()){
            var a=batch.metaQuery(A,request(1));var callback=a.thenRun(()->{
                try{assertEquals(0,executor.submit(()->batch.diagnostics().inFlight()).get(2,TimeUnit.SECONDS));}
                catch(Exception error){throw new AssertionError(error);}
            });raw.done(0);callback.toCompletableFuture().get(2,TimeUnit.SECONDS);
        }
    }
    @Test void concurrentCallersNeverExceedAdmissionAndPhysicalCaps() throws Exception{
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,16,1));var executor=Executors.newVirtualThreadPerTaskExecutor()){
            List<Future<CompletionStage<IoniaClientTransport.MetaReply>>> calls=new ArrayList<>();
            for(int i=0;i<32;i++){int id=i+1;calls.add(executor.submit(()->batch.metaQuery(A,request(id))));}
            List<CompletionStage<IoniaClientTransport.MetaReply>> results=new ArrayList<>();for(var f:calls)results.add(f.get(2,TimeUnit.SECONDS));
            assertEquals(16,batch.diagnostics().outstanding());assertEquals(16,batch.diagnostics().busy());assertEquals(1,raw.calls.size());
            for(int i=0;i<16;i++)raw.done(i);assertEquals(16,raw.calls.size());assertEquals(0,batch.diagnostics().outstanding());
            assertEquals(16,results.stream().filter(f->f.toCompletableFuture().join().data()==null).count());
        }
    }
    @Test void policyValidationAndStableProtocolAreIndependentOfClientTuning(){
        assertThrows(IllegalArgumentException.class,()->settings(0,1000,0,1,1));assertThrows(IllegalArgumentException.class,()->settings(65,1000,0,65,1));
        assertThrows(IllegalArgumentException.class,()->settings(1,64,0,1,1));assertThrows(IllegalArgumentException.class,()->settings(1,1000,-1,1,1));
        assertThrows(IllegalArgumentException.class,()->settings(2,1000,0,1,1));assertThrows(IllegalArgumentException.class,()->settings(1,1000,0,1,257));
        Properties p=new Properties();assertThrows(IllegalArgumentException.class,()->IoniaMetaBatchSettings.parse(p));
        assertNotEquals(settings(1,1000,0,1,1).profileId(),settings(2,1000,2,8,2).profileId());
        assertEquals("members|ionia-meta-batch-v1",IoniaMetaBatchSettings.configuration("members"));
    }

    @Test void shortDeadlineBehindLongRequestFlushesBothWithoutWaitingForHeadAge(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(8,1000,10,8,1))){
            batch.metaQuery(A,request(1));batch.metaQuery(A,request(2,0,5,"k"));assertEquals(1,raw.calls.size());assertEquals(2,raw.calls.getFirst().items().size());raw.done(0);
        }
    }
    @Test void synchronousBorrowedCompletionDrainsThousandsOfQueuedBatchesWithoutRecursion(){
        Clock clock=new Clock();
        Raw raw=new Raw(){
            @Override public CompletionStage<List<Answer>> metaBatch(NodeId to,List<Item> items){
                var stage=super.metaBatch(to,items);if(calls.size()>1)done(calls.size()-1);return stage;
            }
        };
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,6000,1))){
            for(int i=0;i<5000;i++)batch.metaQuery(A,request(i+1));
            assertEquals(1,raw.calls.size());raw.done(0);assertEquals(5000,raw.calls.size());assertEquals(0,batch.diagnostics().outstanding());
        }
    }
    @Test void successiveIdenticalLogicalRequestsNeverReuseACompletedMetaAnswer(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,4,2))){
            var a=batch.metaQuery(A,request(1));raw.done(0);assertEquals(1,a.toCompletableFuture().join().index());
            var b=batch.metaQuery(A,request(1));assertFalse(b.toCompletableFuture().isDone());assertEquals(2,raw.calls.size());
            var item=raw.calls.getLast().items().getFirst();assertNotEquals(raw.calls.getFirst().items().getFirst().token(),item.token());
            raw.calls.getLast().future().complete(List.of(new IoniaMetaBatchTransport.Answer(item.token(),new IoniaClientTransport.MetaReply(A,0,C,new OperationId(1),99,null))));
            assertEquals(99,b.toCompletableFuture().join().index());
        }
    }
    @Test void leaderGroupsAndOutOfOrderPhysicalBatchesStayIndependent(){
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(2,1000,10,8,2))){
            var a=batch.metaQuery(A,request(1));var b=batch.metaQuery(B,request(2));batch.metaQuery(A,request(3));batch.metaQuery(B,request(4));
            assertEquals(2,raw.calls.size());assertTrue(raw.calls.getFirst().items().stream().allMatch(i->i.request().operationId().value()%2==1));
            raw.done(1);assertFalse(a.toCompletableFuture().isDone());assertEquals(B,b.toCompletableFuture().join().nodeId());raw.done(0);assertEquals(A,a.toCompletableFuture().join().nodeId());
        }
    }
    @Test void tokenExhaustionFailsClosedWithoutWrappingOrOverwriting() throws Exception{
        Raw raw=new Raw();Clock clock=new Clock();
        try(var batch=new IoniaBatchedMetaTransport(raw,clock,settings(1,1000,0,4,2))){
            var field=IoniaBatchedMetaTransport.class.getDeclaredField("sequence");field.setAccessible(true);field.setLong(batch,Long.MAX_VALUE);
            assertEquals(ResultStatus.UNAVAILABLE,status(batch.metaQuery(A,request(1))));assertEquals(0,batch.diagnostics().admitted());assertTrue(raw.calls.isEmpty());
        }
    }
}
