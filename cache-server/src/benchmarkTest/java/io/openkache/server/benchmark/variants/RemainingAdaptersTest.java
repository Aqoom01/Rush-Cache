package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.benchmark.variants.RemainingFixtures.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_seventh.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.server.benchmark.ionia_seventh.V06BenchmarkClient;
import io.openkache.server.benchmark.ionia_eighth.V07BenchmarkClient;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Timeout(15)
class RemainingAdaptersTest {
    @ParameterizedTest @EnumSource(Variant.class)
    void actualReadPoliciesReuseOneLifecycleAndCaptureDistinctProfiles(Variant v)throws Exception{
        Fake raw=new Fake();try(var c=create(v,settings(3,2),raw,new Clock())){
            for(int i=0;i<3;i++){var o=get(c.submit(request(i,read())));assertEquals(ClientOutcome.Category.SUCCESS,o.category());assertEquals(Bytes.utf8("value"),o.result().value().orElseThrow());assertEquals(i+1,o.wireOperationId());}
            assertEquals(3,c.snapshot().success());assertEquals(3,c.snapshot().attempts());assertEquals(1,c.diagnostics().clientSessions());assertEquals(3,raw.reads.get());assertTrue(raw.probes.get()>=3);
            assertFalse(raw.readTargets.contains(new NodeId(1)));assertEquals(v==Variant.V07?0:3,raw.metas.get());
            assertEquals(v==Variant.V07?3:0,raw.batches.get());assertEquals(v.name(),c.metadata("local","clock",null,null,Set.of("M01")).variant());
            assertEquals(c.adapterConfiguration(),c.metadata("local","clock",null,null,Set.of("M01")).adapterConfiguration());
            assertTrue(c.adapterConfiguration().get("profile_sha256").matches("[0-9a-f]{64}"));
            assertThrows(UnsupportedOperationException.class,()->c.adapterConfiguration().put("bad","change"));
            if(v.ordinal()<4){assertNotEquals(VariantBenchmarkClient.membership(settings(3,1)).configurationId(),c.configuration());}
            if(v==Variant.V06){assertEquals(VariantBenchmarkClient.membership(settings(3,1)).configurationId(),c.configuration());}
        }assertEquals(1,raw.closes.get());
    }
    @ParameterizedTest @EnumSource(Variant.class)
    void setFanoutDeleteAndBoundedRetryKeepTheSameIdentityAndDeadline(Variant v)throws Exception{
        Fake raw=new Fake();Clock clock=new Clock();AtomicInteger calls=new AtomicInteger();
        raw.execute=(to,r)->{var status=calls.getAndIncrement()==0?ResultStatus.UNAVAILABLE:ResultStatus.OK;clock.now.addAndGet(500);return CompletableFuture.completedFuture(new IoniaClientTransport.Reply(to,r.view(),r.clientId(),r.operationId(),result(status)));};
        try(var c=create(v,settings(5,2),raw,clock)){
            assertEquals(ClientOutcome.Category.SUCCESS,get(c.submit(request(0,new Command.Set(Bytes.utf8("key"),Bytes.utf8("value"))))).category());assertEquals(5,raw.records.get());
            var o=get(c.submit(request(1,delete())));assertEquals(ClientOutcome.Category.SUCCESS,o.category());assertEquals(2,o.attempts().size());
            assertEquals(raw.executed.get(0).clientId(),raw.executed.get(1).clientId());assertEquals(raw.executed.get(0).operationId(),raw.executed.get(1).operationId());
            assertEquals(raw.executed.get(0).command(),raw.executed.get(1).command());assertTrue(raw.executed.get(1).timeoutNanos()<raw.executed.get(0).timeoutNanos());assertEquals(1,c.snapshot().retriedLogical());
        }
    }
    @ParameterizedTest @EnumSource(Variant.class)
    void malformedReplyIsAnErrorRatherThanARecordedSuccess(Variant v)throws Exception{
        Fake raw=new Fake();raw.execute=(to,r)->CompletableFuture.completedFuture(new IoniaClientTransport.Reply(new NodeId(99),r.view(),r.clientId(),r.operationId(),result(ResultStatus.OK)));
        try(var c=create(v,settings(3,3),raw,new Clock())){
            var o=get(c.submit(request(0,delete())));assertEquals(ClientOutcome.Category.ERROR_RESPONSE,o.category());assertEquals(1,o.attempts().size());assertEquals(0,c.snapshot().success());
        }
    }
    @ParameterizedTest @EnumSource(Variant.class)
    void hungDiscoveryDeadlineAndCloseNeverInventAnExecuteAttempt(Variant v)throws Exception{
        Fake raw=new Fake();Clock clock=new Clock();var held=new CompletableFuture<IoniaEngine.ViewInfo>();raw.probe=()->held;
        var c=create(v,settings(3,1),raw,clock);
        var stage=c.submit(request(0,read()));clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2));
        var o=get(stage);assertEquals(ClientOutcome.Category.NO_RESPONSE,o.category());assertEquals(0,o.attempts().size());assertEquals(3,c.diagnostics().rpc().outstanding());
        c.close();c.close();assertEquals(1,raw.closes.get());assertEquals(3,c.diagnostics().rpc().outstanding());
        held.complete(new IoniaEngine.ViewInfo(0,new NodeId(1),IoniaEngine.Status.NORMAL));await(()->c.diagnostics().rpc().outstanding()==0);
    }
    @ParameterizedTest @EnumSource(Variant.class)
    void cancelAndClosePublishOnceWithoutCancellingRawWork(Variant v)throws Exception{
        Fake raw=new Fake();var held=new CompletableFuture<IoniaClientTransport.Reply>();raw.execute=(to,r)->held;
        var c=create(v,settings(3,1),raw,new Clock());var r=request(0,delete());var stage=c.submit(r);
        assertTrue(c.cancel(r.id("remaining","slot")));assertEquals(ClientOutcome.Reason.CANCELLED,get(stage).reason());
        assertFalse(c.cancel(r.id("remaining","slot")));c.close();assertEquals(1,c.snapshot().noResponse());assertEquals(1,c.diagnostics().rpc().outstanding());
        held.completeExceptionally(new IllegalStateException("late"));await(()->c.diagnostics().rpc().outstanding()==0);assertEquals(0,c.snapshot().success());
    }
    @Test void adaptiveAndRoundRobinAreRealClientPoliciesAndNotServerWireProfiles()throws Exception{
        for(var mode:IoniaReadSelectionSettings.Mode.values()){
            Fake raw=new Fake();try(var c=new V06BenchmarkClient(settings(3,1),selection(mode),raw,new Clock())){
                for(int i=0;i<8;i++){assertEquals(ClientOutcome.Category.SUCCESS,get(c.submit(request(i,read()))).category());}
                assertEquals(Set.of(new NodeId(2),new NodeId(3)),Set.copyOf(raw.readTargets));
                assertEquals(8,c.adaptiveDiagnostics().values().stream().mapToLong(d->d.logicalStarts()).sum());
                assertEquals(mode.name(),c.adapterConfiguration().get("read.selection"));assertTrue(c.protocolPending().values().stream().allMatch(n->n==0));
            }
        }
    }
    @Test void metaBatchCombinesFourCallsFromOneActualSessionAndPreservesFourLogicalResults()throws Exception{
        Fake raw=new Fake();try(var c=new V07BenchmarkClient(settings(3,1),new IoniaMetaBatchSettings(4,65536,100_000_000L,16,2),raw,new Clock())){
            List<CompletionStage<ClientOutcome>> stages=new ArrayList<>();for(int i=0;i<4;i++){stages.add(c.submit(request(i,read())));}
            for(var f:stages){assertEquals(ClientOutcome.Category.SUCCESS,get(f).category());}
            assertEquals(4,raw.reads.get());assertEquals(0,raw.metas.get());assertEquals(1,raw.batches.get());assertEquals(4,raw.batchesSent.getFirst().size());
            assertEquals(4,c.snapshot().success());assertEquals(1,c.batchFrames().dispatched());assertEquals(4,c.metaDiagnostics().values().stream().mapToLong(d->d.admitted()).sum());
        }
    }
    @Test void timedOutMetaBatchRemainsInDrainUntilTheActualRawStageFinishes()throws Exception{
        Fake raw=new Fake();Clock clock=new Clock();var held=new CompletableFuture<List<IoniaMetaBatchTransport.Answer>>();raw.batch=(to,items)->held;
        var c=new V07BenchmarkClient(settings(3,1),new IoniaMetaBatchSettings(1,65536,0,16,1),raw,clock);
        var stage=c.submit(request(0,read()));assertEquals(1,c.batchFrames().outstanding());clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2));
        assertEquals(ClientOutcome.Category.NO_RESPONSE,get(stage).category());assertEquals(1,c.protocolPending().get("batch_raw"));
        c.close();assertEquals(1,c.batchFrames().outstanding());assertEquals(1,raw.closes.get());
        held.completeExceptionally(new IllegalStateException("late"));await(()->c.batchFrames().outstanding()==0);assertEquals(0,c.snapshot().success());
    }
    @Test void batchProfilesAreClientMetadataButProtocolIsMandatory(){
        var p1=new IoniaMetaBatchSettings(1,65536,0,16,1);var p2=new IoniaMetaBatchSettings(4,65536,0,16,2);
        try(var c1=new V07BenchmarkClient(settings(3,1),p1,new Fake(),new Clock());var c2=new V07BenchmarkClient(settings(3,1),p2,new Fake(),new Clock())){
            assertNotEquals(c1.adapterConfiguration().get("profile_sha256"),c2.adapterConfiguration().get("profile_sha256"));assertEquals(c1.configuration(),c2.configuration());
        }
        assertThrows(IllegalArgumentException.class,()->new ObservedMetaBatchFrames(new IoniaClientTransport(){
            public CompletionStage<WriteAck> record(NodeId to,Request r){throw new UnsupportedOperationException();}public CompletionStage<Reply> execute(NodeId to,Request r){throw new UnsupportedOperationException();}
            public CompletionStage<ReadReply> followerRead(NodeId to,Request r){throw new UnsupportedOperationException();}public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){throw new UnsupportedOperationException();}
            public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){throw new UnsupportedOperationException();}
        },1));
    }
    @Test void malformedBatchEnvelopeCannotYieldAPartialOrFabricatedSuccess()throws Exception{
        Fake raw=new Fake();raw.batch=(to,items)->CompletableFuture.completedFuture(List.of());
        try(var c=new V07BenchmarkClient(settings(3,1),new IoniaMetaBatchSettings(1,65536,0,16,1),raw,new Clock())){
            assertNotEquals(ClientOutcome.Category.SUCCESS,get(c.submit(request(0,read()))).category());
            assertEquals(1,c.metaDiagnostics().values().stream().mapToLong(d->d.malformedBatches()).sum());assertEquals(0,c.snapshot().success());
        }
    }
    @ParameterizedTest @EnumSource(value=Variant.class,names={"V06","V07"})
    void actualClientRefreshesItsGenerationAndFollowsAChangedLeader(Variant v)throws Exception{
        Fake raw=new Fake();Clock clock=new Clock();AtomicInteger calls=new AtomicInteger();
        raw.execute=(to,r)->{if(calls.getAndIncrement()==0){raw.view=1;raw.leader=new NodeId(2);clock.now.addAndGet(100);return CompletableFuture.completedFuture(new IoniaClientTransport.Reply(to,r.view(),r.clientId(),r.operationId(),result(ResultStatus.UNAVAILABLE)));}
            return CompletableFuture.completedFuture(new IoniaClientTransport.Reply(to,r.view(),r.clientId(),r.operationId(),result(ResultStatus.OK)));};
        try(var c=create(v,settings(3,2),raw,clock)){
            assertEquals(ClientOutcome.Category.SUCCESS,get(c.submit(request(0,delete()))).category());assertEquals(List.of(new NodeId(1),new NodeId(2)),raw.executeTargets);
            if(c instanceof V06BenchmarkClient adaptive){assertTrue(adaptive.adaptiveDiagnostics().values().stream().allMatch(d->d.view()==1));}
            if(c instanceof V07BenchmarkClient batching){assertTrue(batching.metaDiagnostics().values().stream().allMatch(d->d.view()==1));}
        }
    }
}
