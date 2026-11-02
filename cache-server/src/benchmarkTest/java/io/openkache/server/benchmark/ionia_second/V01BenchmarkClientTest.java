package io.openkache.server.benchmark.ionia_second;

import static io.openkache.server.benchmark.ionia_second.V01AdapterFixtures.*;
import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.server.benchmark.ionia.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(15)
class V01BenchmarkClientTest {
    @ParameterizedTest @ValueSource(ints={3,5})
    void leaderDataReadsRetainViewProbesAndTheSameSessionAcrossRequests(int n) throws Exception {
        Fake raw=new Fake();
        try(var client=new V01BenchmarkClient(settings(n,2,8,128),raw,new Clock())){
            for(int i=0;i<3;i++){var o=get(client.submit(request(i,read())));assertEquals(ClientOutcome.Category.SUCCESS,o.category());assertEquals(Bytes.utf8("value"),o.result().value().orElseThrow());assertEquals(i+1,o.wireOperationId());}
            assertEquals(List.of(new NodeId(1),new NodeId(1),new NodeId(1)),raw.executeTargets);
            assertEquals(0,raw.reads.get());assertEquals(0,raw.metas.get());
            assertEquals(4*n,raw.probeTargets.size());assertEquals(Set.copyOf(ids(n)),Set.copyOf(raw.probeTargets));
            assertEquals(1,client.diagnostics().clientSessions());assertEquals(3,client.snapshot().attempts());
        }
        assertEquals(1,raw.closes.get());
    }
    @Test void retryAfterViewChangeFollowsTheNewLeaderWithSameIdAndTotalDeadline() throws Exception {
        Fake raw=new Fake();Clock clock=new Clock();AtomicInteger calls=new AtomicInteger();
        raw.execute=(to,r)->{
            if(calls.getAndIncrement()==0){clock.now.addAndGet(500);raw.view=1;raw.leader=new NodeId(2);return CompletableFuture.completedFuture(new IoniaClientTransport.Reply(to,r.view(),r.clientId(),r.operationId(),result(ResultStatus.UNAVAILABLE)));}
            return CompletableFuture.completedFuture(new IoniaClientTransport.Reply(to,r.view(),r.clientId(),r.operationId(),Result.ok(Bytes.utf8("new"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO)));
        };
        try(var client=new V01BenchmarkClient(settings(3,3,8,128),raw,clock)){
            var o=get(client.submit(request(0,read())));assertEquals(ClientOutcome.Category.SUCCESS,o.category());
            assertEquals(2,o.attempts().size());assertEquals(1,client.snapshot().retriedLogical());
            assertEquals(List.of(new NodeId(1),new NodeId(2)),raw.executeTargets);
            assertEquals(raw.executed.get(0).clientId(),raw.executed.get(1).clientId());
            assertEquals(raw.executed.get(0).operationId(),raw.executed.get(1).operationId());
            assertEquals(raw.executed.get(0).command(),raw.executed.get(1).command());
            assertTrue(raw.executed.get(1).timeoutNanos()<raw.executed.get(0).timeoutNanos());
            assertEquals(0,raw.reads.get());assertEquals(0,raw.metas.get());
        }
    }
    @ParameterizedTest @ValueSource(ints={3,5})
    void setFanoutAndDeleteOutcomeUseOriginalWritePaths(int n) throws Exception {
        Fake raw=new Fake();
        try(var client=new V01BenchmarkClient(settings(n,2,8,128),raw,new Clock())){
            assertEquals(ClientOutcome.Category.SUCCESS,get(client.submit(request(0,new Command.Set(Bytes.utf8("key"),Bytes.utf8("payload"))))).category());
            assertEquals(Set.copyOf(ids(n)),Set.copyOf(raw.recordTargets));assertEquals(n,raw.recordTargets.size());
            assertEquals(ClientOutcome.Category.SUCCESS,get(client.submit(request(1,delete()))).category());
            assertEquals(List.of(new NodeId(1)),raw.executeTargets);assertEquals(2,client.snapshot().admitted());
        }
    }
    @ParameterizedTest @ValueSource(strings={"BAD_REQUEST","INTERNAL_ERROR","NOT_IMPLEMENTED"})
    void permanentErrorsAreNotRetried(String status) throws Exception {
        Fake raw=new Fake();raw.execute=(to,r)->CompletableFuture.completedFuture(new IoniaClientTransport.Reply(to,r.view(),r.clientId(),r.operationId(),result(ResultStatus.valueOf(status))));
        try(var client=new V01BenchmarkClient(settings(3,3,8,128),raw,new Clock())){
            var o=get(client.submit(request(0,read())));assertEquals(ClientOutcome.Category.ERROR_RESPONSE,o.category());assertEquals(1,o.attempts().size());
        }
    }
    @ParameterizedTest @ValueSource(strings={"BUSY","NOT_PRIMARY"})
    void transientErrorsHaveBoundedSameLogicalAttempts(String status) throws Exception {
        Fake raw=new Fake();raw.execute=(to,r)->CompletableFuture.completedFuture(new IoniaClientTransport.Reply(to,r.view(),r.clientId(),r.operationId(),result(ResultStatus.valueOf(status))));
        try(var client=new V01BenchmarkClient(settings(3,2,8,128),raw,new Clock())){
            var o=get(client.submit(request(0,read())));assertEquals(ClientOutcome.Category.ERROR_RESPONSE,o.category());assertEquals(2,o.attempts().size());assertEquals(1,client.snapshot().admitted());
        }
    }
    @Test void discoveryDeadlineHasNoFabricatedAttemptAndDoesNotReleaseRawPermits() throws Exception {
        Fake raw=new Fake();Clock clock=new Clock();var held=new CompletableFuture<IoniaEngine.ViewInfo>();raw.probe=()->held;
        try(var client=new V01BenchmarkClient(settings(3,1,8,2),raw,clock)){
            var stage=client.submit(request(0,read()));assertEquals(2,raw.probeTargets.size());
            clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2));var o=get(stage);
            assertEquals(ClientOutcome.Category.NO_RESPONSE,o.category());assertEquals(ClientOutcome.Reason.DEADLINE,o.reason());assertEquals(0,o.attempts().size());
            assertEquals(2,client.diagnostics().rpc().outstanding());
            held.complete(new IoniaEngine.ViewInfo(0,new NodeId(1),IoniaEngine.Status.NORMAL));
            await(()->client.diagnostics().rpc().outstanding()==0);
        }
    }
    @Test void lateDataSuccessCannotReplaceAnExpiredRead() throws Exception {
        Fake raw=new Fake();Clock clock=new Clock();var held=new CompletableFuture<IoniaClientTransport.Reply>();raw.execute=(to,r)->held;
        try(var client=new V01BenchmarkClient(settings(3,1,8,128),raw,clock)){
            var stage=client.submit(request(0,read()));clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2));
            var o=get(stage);assertEquals(ClientOutcome.Category.NO_RESPONSE,o.category());assertEquals(1,o.attempts().size());
            var r=raw.executed.getFirst();held.complete(new IoniaClientTransport.Reply(new NodeId(1),r.view(),r.clientId(),r.operationId(),Result.ok(Bytes.utf8("late"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO)));
            await(()->client.diagnostics().rpc().outstanding()==0);assertEquals(0,client.snapshot().success());assertEquals(1,client.snapshot().noResponse());
        }
    }
    @Test void cancelAndClosePublishOnceAndKeepRawRpcOwnershipUntilRealCompletion() throws Exception {
        Fake raw=new Fake();var held=new CompletableFuture<IoniaClientTransport.Reply>();raw.execute=(to,r)->held;
        var client=new V01BenchmarkClient(settings(3,1,8,128),raw,new Clock());
        var request=request(0,read());var stage=client.submit(request);assertTrue(client.cancel(request.id("run","slot")));
        assertEquals(ClientOutcome.Reason.CANCELLED,get(stage).reason());assertFalse(client.cancel(request.id("run","slot")));
        client.close();client.close();assertEquals(1,raw.closes.get());assertEquals(1,client.snapshot().noResponse());assertEquals(1,client.diagnostics().rpc().outstanding());
        held.completeExceptionally(new IllegalStateException("late"));await(()->client.diagnostics().rpc().outstanding()==0);
    }
    @Test void admissionAndDuplicateProtectionShareTheO00Contract() throws Exception {
        Fake raw=new Fake();var held=new CompletableFuture<IoniaClientTransport.Reply>();raw.execute=(to,r)->held;
        try(var client=new V01BenchmarkClient(settings(3,1,1,128),raw,new Clock())){
            var request=request(0,read());var first=client.submit(request);var duplicate=client.submit(request);
            assertThrows(ExecutionException.class,()->get(client.submit(request(1,read()))));assertEquals(1,raw.executed.size());
            var r=raw.executed.getFirst();held.complete(new IoniaClientTransport.Reply(new NodeId(1),r.view(),r.clientId(),r.operationId(),Result.ok(Bytes.utf8("value"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO)));
            assertEquals(get(first),get(duplicate));await(()->client.diagnostics().occupiedSlots()==0);
            assertThrows(ExecutionException.class,()->get(client.submit(request)));assertEquals(1,client.snapshot().duplicateSubmissions());
        }
    }
    @Test void closeCompletesPendingReadAndRejectsNewWork() throws Exception {
        Fake raw=new Fake();var held=new CompletableFuture<IoniaClientTransport.Reply>();raw.execute=(to,r)->held;
        var client=new V01BenchmarkClient(settings(3,1,8,128),raw,new Clock());
        try {
            var pending=client.submit(request(0,read()));client.close();
            assertEquals(ClientOutcome.Reason.CLOSED,get(pending).reason());
            assertEquals(1,client.snapshot().noResponse());assertEquals(0,client.snapshot().success());
            assertThrows(ExecutionException.class,()->get(client.submit(request(1,read()))));
            held.completeExceptionally(new IllegalStateException("closed"));await(()->client.diagnostics().rpc().outstanding()==0);
        } finally {client.close();}
    }
    @Test void invalidReplyIdentityAndTraceNamespaceCannotBecomeSuccess() throws Exception {
        Fake raw=new Fake();raw.execute=(to,r)->CompletableFuture.completedFuture(new IoniaClientTransport.Reply(new NodeId(2),r.view(),r.clientId(),r.operationId(),Result.ok(Bytes.utf8("bad"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO)));
        try(var client=new V01BenchmarkClient(settings(3,3,8,128),raw,new Clock())){
            var o=get(client.submit(request(0,read())));assertEquals(ClientOutcome.Category.ERROR_RESPONSE,o.category());assertEquals(ResultStatus.BAD_REQUEST,o.result().status());assertEquals(1,o.attempts().size());
            var malformed=new LogicalRequest(TracePhase.MEASURE,1,0,99,1,read());
            assertThrows(ExecutionException.class,()->get(client.submit(malformed)));assertEquals(1,client.snapshot().admitted());
        }
    }
    @Test void originalFollowerReadRemainsAnActualControlPath() throws Exception {
        Fake raw=new Fake();var ids=ids(3);
        var s=new O00BenchmarkClient.Settings("run","slot",1,20_000,ids,ids.getFirst(),ids.get(1),TimeUnit.SECONDS.toNanos(1),1,0,8,128,TimeUnit.MILLISECONDS.toNanos(5));
        try(var original=new O00BenchmarkClient(s,raw,new Clock())){
            assertEquals(ClientOutcome.Category.SUCCESS,get(original.submit(request(0,read()))).category());
            assertEquals(1,raw.reads.get());assertEquals(1,raw.metas.get());assertTrue(raw.executeTargets.isEmpty());assertTrue(raw.probeTargets.size()>=3);
        }
    }
    @Test void invalidMembershipAndTcpAddressesAreRejectedBeforeConnection() {
        assertThrows(IllegalArgumentException.class,()->settings(4,1,8,128));
        assertThrows(IllegalArgumentException.class,()->V01TcpClients.open(settings(3,1,8,128),Map.of(),1000,new Clock()));
        assertTrue(Arrays.stream(V01BenchmarkClient.Settings.class.getRecordComponents()).noneMatch(c->c.getName().equals("readReplica")));
    }
}
