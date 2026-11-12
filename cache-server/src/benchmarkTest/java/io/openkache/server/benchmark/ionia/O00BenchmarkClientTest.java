package io.openkache.server.benchmark.ionia;

import static io.openkache.server.benchmark.ionia.AdapterFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class O00BenchmarkClientTest {
    @Test void retainedSessionDiscoversOnceAndPreservesMonotonicOperationIds() throws Exception {
        Fake raw = new Fake();
        try (var client = new O00BenchmarkClient(settings(32, 128, 2), raw, new Clock(), false)) {
            for (int i = 0; i < 3; i++) {
                var outcome = get(client.submit(request(i, delete())));
                assertEquals(ClientOutcome.Category.SUCCESS, outcome.category());
                assertEquals(10_002, outcome.wireClientId());
                assertEquals(i + 1, outcome.wireOperationId());
            }
            assertEquals(1, client.diagnostics().clientSessions());
            assertEquals(3, raw.probes.get());
            assertEquals(3, client.snapshot().admitted());
            assertEquals(3, client.snapshot().attempts());
        }
        assertEquals(1, raw.closes.get());
    }

    @Test void retryKeepsCommandWireIdAndOneTotalDeadlineAcrossViewChange() throws Exception {
        Fake raw = new Fake(); Clock clock = new Clock(); AtomicInteger calls = new AtomicInteger();
        raw.execute = r -> {
            if (calls.getAndIncrement() == 0) {
                clock.now.addAndGet(500);
                raw.view = 1; raw.leader = new NodeId(2);
                return CompletableFuture.completedFuture(result(ResultStatus.UNAVAILABLE));
            }
            return CompletableFuture.completedFuture(result(ResultStatus.OK));
        };
        try (var client = new O00BenchmarkClient(settings(32, 128, 3), raw, clock, false)) {
            var outcome = get(client.submit(request(0, delete())));
            assertEquals(ClientOutcome.Category.SUCCESS, outcome.category());
            assertEquals(2, outcome.attempts().size());
            assertEquals(2, outcome.rounds());
            assertEquals(List.of(new NodeId(1), new NodeId(2)), raw.executeTargets);
            assertEquals(raw.executed.get(0).command(), raw.executed.get(1).command());
            assertEquals(raw.executed.get(0).clientId(), raw.executed.get(1).clientId());
            assertEquals(raw.executed.get(0).operationId(), raw.executed.get(1).operationId());
            assertTrue(raw.executed.get(1).timeoutNanos() < raw.executed.get(0).timeoutNanos());
            assertEquals(1, client.snapshot().admitted());
            assertEquals(2, client.snapshot().attempts());
            assertEquals(1, client.snapshot().retriedLogical());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"BAD_REQUEST", "MOVED", "STALE_EPOCH", "CAPACITY_EXCEEDED", "NOT_IMPLEMENTED", "INTERNAL_ERROR"})
    void permanentClientErrorsAreNotRetried(String name) throws Exception {
        Fake raw = new Fake(); raw.execute = r -> CompletableFuture.completedFuture(result(ResultStatus.valueOf(name)));
        try (var client = new O00BenchmarkClient(settings(32, 128, 3), raw, new Clock(), false)) {
            var outcome = get(client.submit(request(0, delete())));
            assertEquals(ClientOutcome.Category.ERROR_RESPONSE, outcome.category());
            assertEquals(1, outcome.attempts().size());
            assertEquals(1, outcome.rounds());
            assertEquals(ResultStatus.valueOf(name), outcome.result().status());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNAVAILABLE", "NOT_PRIMARY", "BUSY"})
    void retryableResultsAreBounded(String name) throws Exception {
        Fake raw = new Fake(); raw.execute = r -> CompletableFuture.completedFuture(result(ResultStatus.valueOf(name)));
        try (var client = new O00BenchmarkClient(settings(32, 128, 2), raw, new Clock(), false)) {
            var outcome = get(client.submit(request(0, delete())));
            assertEquals(ClientOutcome.Category.ERROR_RESPONSE, outcome.category());
            assertEquals(2, outcome.attempts().size());
            assertEquals(2, outcome.rounds());
            assertEquals(1, client.snapshot().errorResponse());
        }
    }

    @Test void discoveryFailureHasNoFabricatedExecuteAttempt() throws Exception {
        Fake raw = new Fake(); raw.probe = () -> CompletableFuture.failedFuture(new IllegalStateException("offline"));
        try (var client = new O00BenchmarkClient(settings(32, 128, 2), raw, new Clock(), false)) {
            var outcome = get(client.submit(request(0, delete())));
            assertEquals(ClientOutcome.Reason.DISCOVERY_FAILED, outcome.reason());
            assertTrue(outcome.attempts().isEmpty());
            assertEquals(0, client.snapshot().attempts());
            assertEquals(1, client.snapshot().noResponse());
            assertFalse(outcome.executionUnknown());
        }
    }

    @Test void watchdogCoversHungDiscoveryAndRawPermitsSurviveDeadlineAndClose() throws Exception {
        Fake raw = new Fake(); Clock clock = new Clock();
        CompletableFuture<IoniaEngine.ViewInfo> held = new CompletableFuture<>();
        raw.probe = () -> held;
        var client = new O00BenchmarkClient(settings(32, 2, 1), raw, clock, false);
        var future = client.submit(request(0, delete()));
        assertEquals(2, client.diagnostics().rpc().outstanding());
        clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2)); client.tick();
        var outcome = get(future);
        assertEquals(ClientOutcome.Reason.DEADLINE, outcome.reason());
        assertEquals(0, outcome.attempts().size());
        assertEquals(2, client.diagnostics().rpc().outstanding());
        get(client.submit(request(1, delete())));
        assertEquals(2, raw.probes.get());
        assertEquals(2, client.diagnostics().rpc().peak());
        client.close(); client.close();
        assertEquals(1, raw.closes.get());
        assertEquals(2, client.diagnostics().rpc().outstanding());
        held.complete(new IoniaEngine.ViewInfo(0, IDS.getFirst(), IoniaEngine.Status.NORMAL));
        await(() -> client.diagnostics().rpc().outstanding() == 0);
        assertEquals(2, client.snapshot().noResponse());
    }

    @Test void deadlineDuringExecuteCannotBecomeALateSuccess() throws Exception {
        Fake raw = new Fake(); Clock clock = new Clock();
        CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, clock, false)) {
            var future = client.submit(request(0, delete()));
            clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2)); client.tick();
            var outcome = get(future);
            assertEquals(ClientOutcome.Reason.DEADLINE, outcome.reason());
            assertTrue(outcome.executionUnknown());
            assertEquals(1, client.diagnostics().rpc().outstanding());
            held.complete(result(ResultStatus.OK));
            assertSame(outcome, get(future));
            assertEquals(0, client.snapshot().success());
            assertEquals(1, client.snapshot().noResponse());
            assertEquals(0, client.diagnostics().rpc().outstanding());
        }
    }

    @Test void explicitCancelDoesNotReleaseRawPermitOrReclassifySuccess() throws Exception {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false)) {
            var req = request(0, delete());
            var future = client.submit(req);
            assertTrue(client.cancel(req.id("run", "load-1")));
            var outcome = get(future);
            assertEquals(ClientOutcome.Reason.CANCELLED, outcome.reason());
            assertFalse(client.cancel(req.id("run", "load-1")));
            assertEquals(1, client.diagnostics().rpc().outstanding());
            held.complete(result(ResultStatus.OK));
            assertEquals(1, client.snapshot().byOperation().get(ClientOutcome.Operation.DELETE).lateClientCompletions());
            assertEquals(0, client.snapshot().success());
            assertSame(outcome, get(future));
        }
    }

    @Test void publicFutureCopyCannotCancelOrFabricateInternalCompletion() throws Exception {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false)) {
            var req = request(0, delete());
            var first = client.submit(req);
            assertTrue(first.toCompletableFuture().cancel(true));
            var duplicate = client.submit(req);
            held.complete(result(ResultStatus.OK));
            assertEquals(ClientOutcome.Category.SUCCESS, get(duplicate).category());
            assertEquals(1, client.snapshot().admitted());
            assertEquals(1, client.snapshot().duplicateSubmissions());
            assertEquals(1, raw.executed.size());
        }
    }

    @Test void sameIdDifferentCommandAndCompletedIdReplayAreRejected() throws Exception {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false)) {
            var original = request(0, delete()); var future = client.submit(original);
            var changed = client.submit(request(0, new Command.Get(Bytes.utf8("other"))));
            assertThrows(ExecutionException.class, () -> get(changed));
            held.complete(result(ResultStatus.OK)); get(future);
            assertThrows(ExecutionException.class, () -> get(client.submit(original)));
            assertEquals(1, client.snapshot().admitted());
            assertEquals(1, client.snapshot().attempts());
        }
    }

    @Test void fullAdmissionDoesNotAdvanceTheRejectedSequence() throws Exception {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        try (var client = new O00BenchmarkClient(settings(1, 128, 1), raw, new Clock(), false)) {
            var first = client.submit(request(0, delete()));
            assertThrows(ExecutionException.class, () -> get(client.submit(request(1, delete()))));
            held.complete(result(ResultStatus.OK)); get(first);
            await(() -> client.diagnostics().occupiedSlots() == 0);
            assertEquals(ClientOutcome.Category.SUCCESS, get(client.submit(request(1, delete()))).category());
            assertEquals(2, client.snapshot().admitted());
            assertEquals(1, client.snapshot().rejected());
        }
    }

    @Test void closeCompletesPendingOnceAndRejectsNewSubmissions() throws Exception {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false);
        var future = client.submit(request(0, delete()));
        client.close(); client.close();
        var outcome = get(future);
        assertEquals(ClientOutcome.Reason.CLOSED, outcome.reason());
        assertEquals(1, raw.closes.get());
        assertThrows(ExecutionException.class, () -> get(client.submit(request(1, delete()))));
        held.complete(result(ResultStatus.OK));
        assertEquals(1, client.snapshot().noResponse());
        assertEquals(0, client.snapshot().pending());
    }

    @Test void setFastAckIsOneClientAttemptAndThreeRecordRpcCalls() throws Exception {
        Fake raw = new Fake();
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false)) {
            var outcome = get(client.submit(request(0, new Command.Set(Bytes.utf8("key"), Bytes.utf8("value")))));
            assertEquals(ClientOutcome.Category.SUCCESS, outcome.category());
            assertEquals(1, outcome.attempts().size());
            assertEquals(3, raw.records.get());
            assertEquals(0, raw.executed.size());
            assertEquals(3, client.diagnostics().rpc().byKind().get(ObservedRpc.Kind.RECORD).dispatched());
        }
    }

    @Test void getAndDeleteMissAreSuccessButMissingGetPayloadIsMalformed() throws Exception {
        Fake raw = new Fake(); raw.execute = r -> CompletableFuture.completedFuture(result(ResultStatus.MISS));
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false)) {
            assertEquals(ClientOutcome.Category.SUCCESS, get(client.submit(request(0, new Command.Get(Bytes.utf8("key"))))).category());
            assertEquals(ClientOutcome.Category.SUCCESS, get(client.submit(request(1, delete()))).category());
            raw.execute = r -> CompletableFuture.completedFuture(result(ResultStatus.OK));
            var invalid = get(client.submit(request(2, new Command.Get(Bytes.utf8("key")))));
            assertEquals(ClientOutcome.Reason.MALFORMED_RESULT, invalid.reason());
            assertEquals(2, client.snapshot().success());
            assertEquals(1, client.snapshot().errorResponse());
        }
    }

    @Test void callbacksRunOutsideLocksAndControlTimer() throws Exception {
        Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>(); raw.execute = r -> held;
        CountDownLatch entered = new CountDownLatch(1); CountDownLatch release = new CountDownLatch(1);
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false)) {
            client.submit(request(0, delete())).thenAccept(outcome -> {
                entered.countDown();
                try { release.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            held.complete(result(ResultStatus.OK));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            try (var executor = Executors.newSingleThreadExecutor()) {
                assertEquals(1, executor.submit(() -> client.diagnostics().logical().success()).get(1, TimeUnit.SECONDS));
            } finally { release.countDown(); }
        }
    }

    @Test void publicationQueueDelayIsIncludedAndCannotPublishLateSuccess() throws Exception {
        Fake raw = new Fake(); Clock clock = new Clock();
        var held = new ConcurrentHashMap<Long, CompletableFuture<Result>>();
        raw.execute = r -> held.computeIfAbsent(r.operationId().value(), ignored -> new CompletableFuture<>());
        CountDownLatch entered = new CountDownLatch(2); CountDownLatch release = new CountDownLatch(1);
        try (var client = new O00BenchmarkClient(settings(8, 128, 1), raw, clock, false)) {
            for (int i = 0; i < 2; i++) {
                client.submit(request(i, delete())).thenAccept(outcome -> {
                    entered.countDown();
                    try { release.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException error) { Thread.currentThread().interrupt(); }
                });
                held.get((long) i + 1).complete(result(ResultStatus.OK));
            }
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var queued = client.submit(request(2, delete()));
            held.get(3L).complete(result(ResultStatus.OK));
            assertEquals(1, client.snapshot().pending());
            clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2));
            release.countDown();
            var outcome = get(queued);
            assertEquals(ClientOutcome.Category.NO_RESPONSE, outcome.category());
            assertEquals(ClientOutcome.Reason.DEADLINE, outcome.reason());
            assertEquals(0, outcome.observedElapsedNanos());
            assertTrue(outcome.elapsedNanos() >= TimeUnit.SECONDS.toNanos(2));
            assertEquals(1, client.diagnostics().latePublications());
            assertEquals(2, client.snapshot().success());
        } finally { release.countDown(); }
    }

    @Test void cancelAndResponseRacesConserveOneTerminalOutcome() throws Exception {
        try (var pool = Executors.newFixedThreadPool(2)) {
            for (int seed = 0; seed < 16; seed++) {
                Fake raw = new Fake(); CompletableFuture<Result> held = new CompletableFuture<>();
                raw.execute = r -> held;
                try (var client = new O00BenchmarkClient(settings(8, 128, 1), raw, new Clock(), false)) {
                    var req = request(0, delete()); var result = client.submit(req);
                    CountDownLatch start = new CountDownLatch(1);
                    Future<?> cancel = pool.submit(() -> {
                        try { start.await(); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
                        client.cancel(req.id("run", "load-1"));
                    });
                    Future<?> response = pool.submit(() -> {
                        try { start.await(); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
                        held.complete(result(ResultStatus.OK));
                    });
                    start.countDown(); cancel.get(5, TimeUnit.SECONDS); response.get(5, TimeUnit.SECONDS);
                    var outcome = get(result);
                    assertTrue(outcome.category() == ClientOutcome.Category.SUCCESS
                            || outcome.reason() == ClientOutcome.Reason.CANCELLED);
                    var s = client.snapshot();
                    assertEquals(1, s.admitted()); assertEquals(0, s.pending());
                    assertEquals(1, s.success() + s.errorResponse() + s.noResponse());
                    assertEquals(0, client.diagnostics().rpc().outstanding());
                }
            }
        }
    }

    @Test void invalidStreamAndWireOverflowDoNotAdmit() throws Exception {
        Fake raw = new Fake();
        try (var client = new O00BenchmarkClient(settings(32, 128, 1), raw, new Clock(), false)) {
            var invalid = new LogicalRequest(TracePhase.MEASURE, 0, 0, 7, 0, delete());
            assertThrows(ExecutionException.class, () -> get(client.submit(invalid)));
            assertThrows(ExecutionException.class, () -> get(client.submit(request(Long.MAX_VALUE, delete()))));
            assertEquals(0, client.snapshot().admitted());
            assertEquals(0, raw.probes.get());
        }
    }
}

