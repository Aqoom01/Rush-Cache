package io.openkache.runner.benchmark.execution;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Timeout(10)
class OpenLoopRunnerTest {
    private static long ms(long n) { return TimeUnit.MILLISECONDS.toNanos(n); }
    private TraceGenerator generator(boolean writes) {
        return new TraceGenerator(new TraceConfig(101, 4, 24, 100,
                writes ? 0 : 100, writes ? 100 : 0, 0, 2, TraceConfig.Distribution.UNIFORM, 0));
    }
    private OpenLoopPlan plan(int inFlight, long measured, long rate) {
        return new OpenLoopPlan("run", "load-1", 3, measured, 100, 100, rate,
                inFlight, ms(2000), 0, 0,
                new OpenLoopPlan.Limits(ms(5000), ms(1000), ms(500), ms(500), ms(1)));
    }
    private DrainProbe ready() {
        return expected -> new DrainProbe.Observation(DrainProbe.Status.READY, "fixture_ready", Map.of("writes", expected));
    }
    private static long sum(OpenLoopResult.Phase p, java.util.function.ToLongFunction<OpenLoopResult.Counts> f) {
        return p.byOperation().values().stream().mapToLong(f).sum();
    }

    @Test void healthyPhasesPreserveTheAbsoluteScheduleAndRequestIds() {
        Fake client = new Fake();
        List<OpenLoopResult.Decision> decisions = new CopyOnWriteArrayList<>();
        List<OpenLoopResult.ScheduledOutcome> outcomes = new CopyOnWriteArrayList<>();
        var observer = new OpenLoopRunner.Observer() {
            @Override public void decision(OpenLoopResult.Decision d) { decisions.add(d); }
            @Override public void outcome(OpenLoopResult.ScheduledOutcome o) { outcomes.add(o); }
        };
        var result = new OpenLoopRunner(generator(false), plan(32, 11, 100), client, ready(), observer).run();
        assertTrue(result.passed(), result.toString());
        assertEquals(18, result.finalClient().admitted());
        assertEquals(18, decisions.size()); assertEquals(18, outcomes.size());
        assertEquals(18, new HashSet<>(client.accepted.keySet()).size());
        for (var d : decisions) {
            var p = result.phases().get(d.phase());
            assertEquals(p.schedule().offsetNanos(d.index()), d.dueNanos() - p.startedNanos());
            assertTrue(d.decisionNanos() - d.dueNanos() >= 0);
        }
        for (var p : result.phases().values()) {
            assertEquals(p.schedule().requests(), p.considered());
            assertEquals(0, p.unissued()); assertEquals(0, p.unresolved());
            assertEquals(p.considered(), p.decisionLag().count());
            assertEquals(p.submissionCalls(), p.submissionLag().count());
            assertEquals(p.success(), p.admittedLag().count());
            assertTrue(p.emissionEndedNanos() - p.startedNanos() >= p.schedule().spanNanos());
        }
        assertEquals(1, client.closes.get());
    }

    @Test void heldResponsesDoNotSlowTheScheduleAndCapacityDropsAreCounted() throws Exception {
        Fake client = new Fake(); client.holdMeasure = true;
        CountDownLatch lastDecision = new CountDownLatch(1);
        var observer = new OpenLoopRunner.Observer() {
            @Override public void decision(OpenLoopResult.Decision d) {
                if (d.phase() == TracePhase.MEASURE && d.index() == 19) { lastDecision.countDown(); }
            }
        };
        try (var executor = Executors.newSingleThreadExecutor()) {
            var future = executor.submit(() -> new OpenLoopRunner(generator(false), plan(3, 20, 1000), client, ready(), observer).run());
            assertTrue(lastDecision.await(5, TimeUnit.SECONDS));
            assertEquals(3, client.measurePending());
            client.releaseMeasure();
            var result = future.get(5, TimeUnit.SECONDS);
            assertTrue(result.completed(), result.toString());
            assertTrue(result.collectionValid()); assertTrue(result.scheduleQualified());
            assertFalse(result.serviceHealthy()); assertFalse(result.passed());
            var p = result.phases().get(TracePhase.MEASURE);
            assertEquals(20, p.considered()); assertEquals(3, p.success());
            assertEquals(17, p.capacityDrops()); assertEquals(0, p.unissued());
            assertEquals(3, p.peakInFlight()); assertEquals(3, p.submissionCalls());
        }
    }

    @Test void backendAdmissionRejectionsAreNeverRetriedOrHidden() {
        Fake client = new Fake(); client.rejectOddMeasure = true;
        var result = new OpenLoopRunner(generator(false), plan(32, 10, 100), client, ready()).run();
        assertTrue(result.completed(), result.toString()); assertFalse(result.serviceHealthy());
        var p = result.phases().get(TracePhase.MEASURE);
        assertEquals(5, sum(p, OpenLoopResult.Counts::clientRejected));
        assertEquals(10, p.submissionCalls()); assertEquals(5, p.success());
        assertEquals(5, result.finalClient().rejected());
        assertEquals(12, result.finalClient().admitted()); assertEquals(12, result.finalClient().attempts());
    }

    @ParameterizedTest
    @EnumSource(value = ClientOutcome.Category.class, names = {"ERROR_RESPONSE", "NO_RESPONSE"})
    void failedReadOutcomesDoNotReshapeTheRemainingArrivalSchedule(ClientOutcome.Category category) {
        Fake client = new Fake(); client.measureCategory = category;
        var result = new OpenLoopRunner(generator(false), plan(32, 10, 100), client, ready()).run();
        assertTrue(result.completed(), result.toString()); assertTrue(result.scheduleQualified());
        assertFalse(result.serviceHealthy());
        var p = result.phases().get(TracePhase.MEASURE);
        assertEquals(10, p.considered()); assertEquals(10, p.submissionCalls());
        assertEquals(10, sum(p, OpenLoopResult.Counts::outcomes));
        assertEquals(17, result.finalClient().admitted());
        assertEquals(17, result.finalClient().attempts());
    }

    @Test void writeTimeoutCannotBeQualifiedAsAReplicaDrainProof() {
        Fake client = new Fake(); client.measureCategory = ClientOutcome.Category.NO_RESPONSE;
        var result = new OpenLoopRunner(generator(true), plan(32, 3, 100), client, ready()).run();
        assertTrue(result.allArrivalsConsidered()); assertFalse(result.collectionValid());
        assertEquals("WRITE_EFFECTS_UNCERTAIN", result.failure());
        assertEquals(3, result.phases().get(TracePhase.MEASURE).uncertainWrites());
    }

    @Test void driverPauseProducesTimingMissesNotFakeBackendCapacityDrops() {
        Fake client = new Fake();
        var p = new OpenLoopPlan("run", "load-1", 2, 10, 50, 50, 1000, 32, ms(50), 0, 0,
                new OpenLoopPlan.Limits(ms(5000), ms(1000), ms(500), ms(500), ms(1)));
        var observer = new OpenLoopRunner.Observer() {
            @Override public void decision(OpenLoopResult.Decision d) {
                if (d.phase() == TracePhase.MEASURE && d.index() == 0) {
                    try { Thread.sleep(200); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
                }
            }
        };
        var result = new OpenLoopRunner(generator(false), p, client, ready(), observer).run();
        assertTrue(result.collectionValid(), result.toString());
        assertFalse(result.scheduleQualified());
        var phase = result.phases().get(TracePhase.MEASURE);
        assertTrue(phase.timingMisses() > 0); assertEquals(0, phase.capacityDrops());
        assertEquals(10, phase.decisionLag().count());
        assertTrue(phase.decisionLag().maximumNanos() >= ms(50));
    }

    @Test void setupLossPreventsMeasurementRatherThanUsingAnIncompleteDataset() {
        Fake client = new Fake(); client.rejectFirstLoad = true;
        var result = new OpenLoopRunner(generator(false), plan(32, 10, 100), client, ready()).run();
        assertFalse(result.collectionValid()); assertEquals("SETUP_NOT_HEALTHY_LOAD", result.failure());
        assertFalse(result.phases().containsKey(TracePhase.WARMUP));
        assertFalse(result.phases().containsKey(TracePhase.MEASURE));
    }

    @Test void fatalObserverFailureKeepsFutureAndAlreadyDueUnissuedCounts() {
        Fake client = new Fake();
        var observer = new OpenLoopRunner.Observer() {
            @Override public void decision(OpenLoopResult.Decision d) {
                if (d.phase() == TracePhase.MEASURE) { throw new IllegalStateException("controlled observer failure"); }
            }
        };
        var result = new OpenLoopRunner(generator(false), plan(32, 10, 5), client, ready(), observer).run();
        assertFalse(result.collectionValid()); assertEquals("DECISION_OBSERVER_FAILED", result.failure());
        var p = result.phases().get(TracePhase.MEASURE);
        assertEquals(1, p.considered()); assertEquals(9, p.unissued());
        assertEquals(p.unissued(), p.dueButUnissued() + p.futureCancelled());
        assertEquals(0, result.outstandingCallbacks());
    }

    @Test void brokenOutcomeAndCollectorExceptionsDoNotPass() {
        for (boolean malformed : new boolean[]{true, false}) {
            Fake client = new Fake(); client.badMeasureId = malformed;
            var observer = new OpenLoopRunner.Observer() {
                @Override public void outcome(OpenLoopResult.ScheduledOutcome o) {
                    if (!malformed && o.request().phase() == TracePhase.MEASURE) { throw new IllegalStateException("collector"); }
                }
            };
            var result = new OpenLoopRunner(generator(false), plan(32, 5, 100), client, ready(), observer).run();
            assertFalse(result.collectionValid()); assertNotNull(result.failure()); assertEquals(1, client.closes.get());
        }
    }

    @Test void uncooperativeOutcomesRemainUnresolvedAfterTheBoundedGrace() {
        Fake client = new Fake(); client.holdMeasure = true; client.ignoreMeasureClose = true;
        var p = new OpenLoopPlan("run", "load-1", 0, 5, 50, 1000, 1000, 2, ms(1000), 0, 0,
                new OpenLoopPlan.Limits(ms(2000), ms(20), ms(50), ms(30), ms(1)));
        var result = new OpenLoopRunner(generator(false), p, client, ready()).run();
        assertFalse(result.collectionValid()); assertEquals("OUTCOME_DRAIN_TIMEOUT", result.failure());
        var phase = result.phases().get(TracePhase.MEASURE);
        assertTrue(phase.unresolved() > 0); assertEquals(phase.unresolved(), result.finalClient().pending());
        assertEquals(0, sum(phase, OpenLoopResult.Counts::noResponse));
    }

    @Test void interruptionPreservesUnissuedAndRestoresCallerInterrupt() throws Exception {
        Fake client = new Fake(); client.holdMeasure = true;
        AtomicReference<OpenLoopResult> result = new AtomicReference<>();
        AtomicBoolean restored = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            result.set(new OpenLoopRunner(generator(false), plan(3, 10, 5), client, ready()).run());
            restored.set(Thread.currentThread().isInterrupted());
        });
        caller.start(); assertTrue(client.measureAdmitted.await(5, TimeUnit.SECONDS));
        caller.interrupt(); caller.join(2000);
        assertFalse(caller.isAlive()); assertTrue(restored.get()); assertFalse(result.get().collectionValid());
        assertTrue(result.get().phases().get(TracePhase.MEASURE).unissued() > 0);
        assertEquals(0, result.get().finalClient().pending()); assertEquals(1, client.closes.get());
    }

    @Test void invalidMinimumDurationAndOversizedScheduleFailBeforeSending() {
        assertThrows(IllegalArgumentException.class, () -> new OpenLoopPlan("run", "load-1", 0, 10,
                1000, 1000, 1000, 2, ms(100), 0, ms(1000),
                new OpenLoopPlan.Limits(ms(5000), ms(100), ms(100), ms(100), ms(1))));
        Fake client = new Fake();
        var tooLong = plan(2, 10, 1);
        assertThrows(IllegalArgumentException.class, () -> new OpenLoopRunner(generator(false), tooLong, client, ready()));
        assertEquals(0, client.snapshot().admitted()); client.close();
    }

    @Test void workCutoffCountsAlreadyDueRequestsThatWereNeverConsidered() {
        Fake client = new Fake();
        var p = new OpenLoopPlan("run", "load-1", 0, 10, 100, 100, 100,
                32, ms(400), 0, 0,
                new OpenLoopPlan.Limits(ms(500), ms(1000), ms(100), ms(500), ms(1)));
        var observer = new OpenLoopRunner.Observer() {
            @Override public void decision(OpenLoopResult.Decision d) {
                if (d.phase() == TracePhase.MEASURE && d.index() == 0) {
                    try { Thread.sleep(700); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
                }
            }
        };
        var result = new OpenLoopRunner(generator(false), p, client, ready(), observer).run();
        assertFalse(result.collectionValid()); assertEquals("WORK_LIMIT", result.failure());
        var phase = result.phases().get(TracePhase.MEASURE);
        assertEquals(1, phase.considered()); assertEquals(9, phase.unissued());
        assertEquals(9, phase.dueButUnissued()); assertEquals(0, phase.futureCancelled());
        assertEquals(0, result.outstandingCallbacks()); assertEquals(0, result.finalClient().pending());
    }

    private static final class Fake implements BenchmarkClient {
        final ClientAccounting ledger = new ClientAccounting();
        final ConcurrentMap<LogicalRequest.RequestId, LogicalRequest> accepted = new ConcurrentHashMap<>();
        final ConcurrentMap<LogicalRequest.RequestId, Pending> pending = new ConcurrentHashMap<>();
        final AtomicInteger closes = new AtomicInteger();
        final CountDownLatch measureAdmitted = new CountDownLatch(1);
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        volatile boolean holdMeasure, rejectOddMeasure, rejectFirstLoad, ignoreMeasureClose, badMeasureId;
        volatile ClientOutcome.Category measureCategory = ClientOutcome.Category.SUCCESS;
        private boolean closed;
        private record Pending(LogicalRequest request, long invoked, CompletableFuture<ClientOutcome> future, AtomicBoolean done) {}

        @Override public CompletionStage<ClientOutcome> submit(LogicalRequest request) {
            Pending p;
            synchronized (this) {
                if (closed || request.phase() == TracePhase.MEASURE && rejectOddMeasure && request.globalIndex() % 2 == 1
                        || request.phase() == TracePhase.LOAD && rejectFirstLoad && request.globalIndex() == 0) {
                    ledger.rejected(); return CompletableFuture.failedFuture(new RejectedExecutionException("fixture"));
                }
                var id = request.id("run", "load-1");
                if (accepted.putIfAbsent(id, request) != null) { return CompletableFuture.failedFuture(new IllegalArgumentException("duplicate")); }
                var op = ClientOutcome.Operation.of(request.command());
                ledger.admitted(op); ledger.attempt(op);
                p = new Pending(request, System.nanoTime(), new CompletableFuture<>(), new AtomicBoolean());
                pending.put(id, p);
            }
            if (request.phase() == TracePhase.MEASURE) { measureAdmitted.countDown(); }
            if (!(holdMeasure && request.phase() == TracePhase.MEASURE)) {
                try {
                    executor.execute(() -> finish(p, request.phase() == TracePhase.MEASURE ? measureCategory : ClientOutcome.Category.SUCCESS,
                            request.phase() == TracePhase.MEASURE && measureCategory != ClientOutcome.Category.SUCCESS
                                    ? measureCategory == ClientOutcome.Category.NO_RESPONSE ? ClientOutcome.Reason.DEADLINE : ClientOutcome.Reason.CLIENT_ERROR
                                    : ClientOutcome.Reason.SUCCESS));
                } catch (RejectedExecutionException closing) { finish(p, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CLOSED); }
            }
            return p.future().minimalCompletionStage();
        }
        private void finish(Pending p, ClientOutcome.Category category, ClientOutcome.Reason reason) {
            if (!p.done().compareAndSet(false, true)) { return; }
            var request = p.request(); var op = ClientOutcome.Operation.of(request.command());
            ledger.terminal(op, category, 1);
            long end = System.nanoTime();
            Result value = category == ClientOutcome.Category.NO_RESPONSE ? null
                    : category == ClientOutcome.Category.ERROR_RESPONSE ? Result.of(ResultStatus.BUSY, RoutingVersion.ZERO, OwnershipEpoch.ZERO)
                    : Result.of(request.command() instanceof Command.Get ? ResultStatus.MISS : ResultStatus.OK, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
            var id = request.id(badMeasureId && request.phase() == TracePhase.MEASURE ? "bad-run" : "run", "load-1");
            var outcome = new ClientOutcome(id, op, category, reason, value, p.invoked(), end, end,
                    request.clientId(), request.sequence() + 1, 1,
                    List.of(new ClientOutcome.Attempt(1, p.invoked(), end, value == null ? null : value.status(), null)));
            pending.remove(request.id("run", "load-1"), p); p.future().complete(outcome);
        }
        long measurePending() { return pending.values().stream().filter(p -> p.request().phase() == TracePhase.MEASURE).count(); }
        void releaseMeasure() {
            for (Pending p : pending.values()) { if (p.request().phase() == TracePhase.MEASURE) { finish(p, ClientOutcome.Category.SUCCESS, ClientOutcome.Reason.SUCCESS); } }
        }
        @Override public boolean cancel(LogicalRequest.RequestId id) {
            Pending p = pending.get(id);
            if (p == null || ignoreMeasureClose && p.request().phase() == TracePhase.MEASURE) { return false; }
            finish(p, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CANCELLED); return true;
        }
        @Override public ClientAccounting.Snapshot snapshot() { return ledger.snapshot(); }
        @Override public void close() {
            List<Pending> copy;
            synchronized (this) { if (closed) { return; } closed = true; closes.incrementAndGet(); copy = List.copyOf(pending.values()); }
            for (Pending p : copy) {
                if (!(ignoreMeasureClose && p.request().phase() == TracePhase.MEASURE)) { finish(p, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CLOSED); }
            }
            executor.shutdownNow();
        }
    }
}

