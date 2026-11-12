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

@Timeout(10)
class ClosedLoopRunnerTest {
    private static final String RUN = "run";
    private static final String SLOT = "load-1";
    private static long ms(long value) { return TimeUnit.MILLISECONDS.toNanos(value); }
    private TraceGenerator generator(int streams, int get, int set, int delete) {
        return new TraceGenerator(new TraceConfig(101, 13, 24, 100, get, set, delete,
                streams, TraceConfig.Distribution.UNIFORM, 0));
    }
    private ClosedLoopPlan plan(int concurrency, long warmup, long measure) {
        return new ClosedLoopPlan(RUN, SLOT, concurrency, warmup, measure, 0, 0,
                new ClosedLoopPlan.Limits(ms(5000), ms(1000), ms(500), ms(300), ms(500), ms(1), 100));
    }
    private DrainProbe ready() {
        return expected -> new DrainProbe.Observation(DrainProbe.Status.READY, "fixture_ready", Map.of("target", expected));
    }

    @Test void fixedStreamsCoverEveryRequestOnceAndSeparatePhases() {
        Fake client = new Fake(); client.delayMillis = 2;
        var result = new ClosedLoopRunner(generator(5, 80, 15, 5), plan(2, 11, 31), client, ready()).run();
        assertTrue(result.passed(), result.toString());
        assertEquals(55, result.finalClient().admitted());
        assertEquals(1, client.closes.get());
        for (TracePhase phase : TracePhase.values()) {
            long wanted = phase == TracePhase.LOAD ? 13 : phase == TracePhase.WARMUP ? 11 : 31;
            var p = result.phases().get(phase);
            assertEquals(wanted, p.scheduled()); assertEquals(wanted, p.success());
            assertEquals(0, p.unresolved()); assertTrue(p.peakDriverSlots() <= 2);
            Set<Long> seen = new HashSet<>();
            client.accepted.values().stream().filter(r -> r.phase() == phase).forEach(r -> assertTrue(seen.add(r.globalIndex())));
            assertEquals(wanted, seen.size());
            for (long i = 0; i < wanted; i++) { assertTrue(seen.contains(i)); }
        }
        assertTrue(client.peak.get() <= 2);
        assertEquals(List.of(ClosedLoopResult.Stage.PREFLIGHT, ClosedLoopResult.Stage.LOAD,
                ClosedLoopResult.Stage.LOAD_DRAIN, ClosedLoopResult.Stage.WARMUP,
                ClosedLoopResult.Stage.WARMUP_DRAIN, ClosedLoopResult.Stage.MEASURE,
                ClosedLoopResult.Stage.FINAL_DRAIN, ClosedLoopResult.Stage.CLEANUP),
                result.events().stream().map(ClosedLoopResult.Event::stage).toList());
    }

    @Test void temporaryAdmissionRejectionRetriesTheSameRequestWithoutInflatingLogicalCount() {
        Fake client = new Fake(); client.rejectFirst = true;
        var result = new ClosedLoopRunner(generator(1, 100, 0, 0), plan(1, 2, 3), client, ready()).run();
        assertTrue(result.passed(), result.toString());
        assertEquals(18, result.finalClient().admitted());
        assertEquals(18, result.finalClient().rejected());
        assertEquals(18, result.finalClient().attempts());
        assertEquals(36, result.phases().values().stream().mapToLong(ClosedLoopResult.Phase::submissionCalls).sum());
        assertEquals(18, client.accepted.size());
    }

    @Test void exhaustedAdmissionIsNotFabricatedAsAnAdmittedTimeout() {
        Fake client = new Fake(); client.rejectAlways = true;
        var p = new ClosedLoopPlan(RUN, SLOT, 1, 1, 3, 0, 0,
                new ClosedLoopPlan.Limits(ms(2000), ms(200), ms(500), ms(100), ms(200), ms(1), 3));
        var result = new ClosedLoopRunner(generator(1, 100, 0, 0), p, client, ready()).run();
        assertFalse(result.passed()); assertEquals("ADMISSION_EXHAUSTED", result.failure());
        assertEquals(0, result.finalClient().admitted());
        assertEquals(3, result.finalClient().rejected());
        assertEquals(1, result.phases().get(TracePhase.LOAD).byOperation().get(ClientOutcome.Operation.SET).notAdmitted());
        assertFalse(result.phases().containsKey(TracePhase.WARMUP));
    }

    @Test void applyBarrierPreventsWarmupFromStartingEarly() {
        Fake client = new Fake();
        AtomicInteger loadPolls = new AtomicInteger();
        AtomicBoolean allowed = new AtomicBoolean();
        client.warmupAllowed = allowed;
        DrainProbe probe = expected -> {
            if (expected == 13 && !allowed.get()) {
                if (loadPolls.incrementAndGet() < 3) {
                    return new DrainProbe.Observation(DrainProbe.Status.WAIT, "follower_pending", Map.of());
                }
                allowed.set(true);
            }
            return new DrainProbe.Observation(DrainProbe.Status.READY, "applied", Map.of());
        };
        var result = new ClosedLoopRunner(generator(2, 100, 0, 0), plan(2, 7, 9), client, probe).run();
        assertTrue(result.passed(), result.toString()); assertEquals(3, loadPolls.get());
        assertFalse(client.warmupViolation.get());
    }

    @Test void barrierTimeoutStopsBeforeWarmup() {
        Fake client = new Fake();
        DrainProbe probe = expected -> new DrainProbe.Observation(expected == 0
                ? DrainProbe.Status.READY : DrainProbe.Status.WAIT, "pending_apply", Map.of());
        var result = new ClosedLoopRunner(generator(1, 100, 0, 0), plan(1, 2, 3), client, probe).run();
        assertFalse(result.passed()); assertTrue(result.failure().contains("LOAD_DRAIN"));
        assertFalse(result.phases().containsKey(TracePhase.WARMUP));
        assertEquals(13, result.finalClient().admitted());
        assertEquals(DrainProbe.Status.WAIT, result.cleanupObservation().status());
    }

    @Test void unsupportedOrDirtyPreflightSendsNothing() {
        for (DrainProbe.Status status : List.of(DrainProbe.Status.FAILED, DrainProbe.Status.UNSUPPORTED)) {
            Fake client = new Fake();
            var result = new ClosedLoopRunner(generator(1, 100, 0, 0), plan(1, 2, 3), client,
                    expected -> new DrainProbe.Observation(status, "not_qualified", Map.of())).run();
            assertFalse(result.passed()); assertTrue(result.phases().isEmpty());
            assertEquals(0, result.finalClient().admitted()); assertEquals(1, client.closes.get());
        }
    }

    @Test void tooShortFixedNDoesNotSendExtraRequestsToSatisfyMinimumDuration() {
        for (TracePhase shortPhase : List.of(TracePhase.WARMUP, TracePhase.MEASURE)) {
            Fake client = new Fake();
            var p = new ClosedLoopPlan(RUN, SLOT, 1, 2, 3,
                    shortPhase == TracePhase.WARMUP ? ms(20_000) : 0,
                    shortPhase == TracePhase.MEASURE ? ms(20_000) : 0,
                    new ClosedLoopPlan.Limits(ms(30_000), ms(1000), ms(500), ms(300), ms(500), ms(1), 100));
            var result = new ClosedLoopRunner(generator(1, 100, 0, 0), p, client, ready()).run();
            assertFalse(result.passed()); assertEquals("INSUFFICIENT_DURATION_" + shortPhase, result.failure());
            assertEquals(shortPhase == TracePhase.WARMUP ? 15 : 18, client.accepted.size());
            assertTrue(result.events().stream().anyMatch(e -> e.stage().name().equals(shortPhase.name()) && !e.passed()));
        }
    }

    @Test void badOutcomeIdAndDishonestLedgerCannotPass() {
        for (boolean badIdentity : new boolean[]{true, false}) {
            Fake client = new Fake(); client.badIdentity = badIdentity; client.hideLedger = !badIdentity;
            var result = new ClosedLoopRunner(generator(1, 100, 0, 0), plan(1, 2, 3), client, ready()).run();
            assertFalse(result.passed());
            assertFalse(result.phases().containsKey(TracePhase.WARMUP));
            assertEquals(1, client.closes.get());
        }
    }

    @Test void observedNoResponseIsNotRetriedByTheCoordinator() {
        Fake client = new Fake(); client.respondNoResponse = true;
        var result = new ClosedLoopRunner(generator(1, 100, 0, 0), plan(1, 2, 3), client, ready()).run();
        assertFalse(result.passed());
        assertEquals(1, result.finalClient().admitted()); assertEquals(1, result.finalClient().attempts());
        assertEquals(1, result.phases().get(TracePhase.LOAD).byOperation().get(ClientOutcome.Operation.SET).noResponse());
    }

    @Test void waitTimeoutCancelsAndPreservesTheCleanupOutcome() {
        Fake client = new Fake(); client.hold = true;
        var p = new ClosedLoopPlan(RUN, SLOT, 1, 2, 3, 0, 0,
                new ClosedLoopPlan.Limits(ms(1000), ms(30), ms(100), ms(100), ms(500), ms(1), 100));
        var result = new ClosedLoopRunner(generator(1, 100, 0, 0), p, client, ready()).run();
        assertFalse(result.passed()); assertEquals("REQUEST_WAIT_TIMEOUT", result.failure());
        assertTrue(result.workersTerminated()); assertEquals(0, result.finalClient().pending());
        var counts = result.phases().get(TracePhase.LOAD).byOperation().get(ClientOutcome.Operation.SET);
        assertEquals(1, counts.noResponse()); assertEquals(0, counts.notAdmitted()); assertEquals(0, counts.unresolved());
    }

    @Test void uncooperativeClientLeavesUnresolvedEvidenceInsteadOfFakeNoResponse() {
        Fake client = new Fake(); client.hold = true; client.ignoreCancelAndClose = true;
        var p = new ClosedLoopPlan(RUN, SLOT, 1, 2, 3, 0, 0,
                new ClosedLoopPlan.Limits(ms(500), ms(20), ms(50), ms(50), ms(30), ms(1), 10));
        var result = new ClosedLoopRunner(generator(1, 100, 0, 0), p, client, ready()).run();
        assertFalse(result.passed());
        assertEquals(1, result.phases().get(TracePhase.LOAD).unresolved());
        assertEquals(1, result.finalClient().pending()); assertEquals(0, result.finalClient().noResponse());
    }

    @Test void interruptionClosesTheClientAndRestoresInterruptStatus() throws Exception {
        Fake client = new Fake(); client.hold = true;
        AtomicReference<ClosedLoopResult> result = new AtomicReference<>();
        AtomicBoolean restored = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            result.set(new ClosedLoopRunner(generator(1, 100, 0, 0), plan(1, 2, 3), client, ready()).run());
            restored.set(Thread.currentThread().isInterrupted());
        });
        caller.start();
        assertTrue(client.submitted.await(5, TimeUnit.SECONDS));
        caller.interrupt(); caller.join(2000);
        assertFalse(caller.isAlive()); assertFalse(result.get().passed()); assertTrue(restored.get());
        assertEquals(1, client.closes.get()); assertEquals(0, result.get().finalClient().pending());
    }

    @Test void runIsOneShotAndConcurrencyCannotExceedTheLogicalStreams() {
        Fake client = new Fake();
        var runner = new ClosedLoopRunner(generator(1, 100, 0, 0), plan(1, 0, 1), client, ready());
        assertTrue(runner.run().passed());
        assertThrows(IllegalStateException.class, runner::run);
        assertThrows(IllegalArgumentException.class,
                () -> new ClosedLoopRunner(generator(1, 100, 0, 0), plan(2, 0, 1), new Fake(), ready()));
        assertThrows(IllegalArgumentException.class,
                () -> new ClosedLoopRunner(generator(1025, 100, 0, 0), plan(1, 0, 1), new Fake(), ready()));
        var huge = new TraceGenerator(new TraceConfig(101, Long.MAX_VALUE, 24, 100,
                100, 0, 0, 1, TraceConfig.Distribution.UNIFORM, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ClosedLoopRunner(huge, plan(1, 0, 1), new Fake(), ready()));
    }

    @Test void wholeWorkLimitStopsAdmissionAndStillCleansTheAdmittedRequest() {
        Fake client = new Fake(); client.hold = true;
        var p = new ClosedLoopPlan(RUN, SLOT, 1, 2, 3, 0, 0,
                new ClosedLoopPlan.Limits(ms(30), ms(1000), ms(100), ms(100), ms(500), ms(1), 100));
        var result = new ClosedLoopRunner(generator(1, 100, 0, 0), p, client, ready()).run();
        assertFalse(result.passed()); assertEquals("WORK_LIMIT", result.failure());
        assertFalse(result.phases().containsKey(TracePhase.WARMUP));
        assertEquals(0, result.finalClient().pending()); assertTrue(result.workersTerminated());
        assertEquals(1, client.closes.get());
    }

    private static final class Fake implements BenchmarkClient {
        final ClientAccounting ledger = new ClientAccounting();
        final ConcurrentMap<LogicalRequest.RequestId, LogicalRequest> accepted = new ConcurrentHashMap<>();
        final ConcurrentMap<LogicalRequest.RequestId, Pending> pending = new ConcurrentHashMap<>();
        final Set<LogicalRequest.RequestId> tried = ConcurrentHashMap.newKeySet();
        final AtomicInteger active = new AtomicInteger(), peak = new AtomicInteger(), closes = new AtomicInteger();
        final CountDownLatch submitted = new CountDownLatch(1);
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        volatile boolean rejectFirst, rejectAlways, hold, ignoreCancelAndClose, respondNoResponse, badIdentity, hideLedger;
        volatile int delayMillis;
        volatile AtomicBoolean warmupAllowed;
        final AtomicBoolean warmupViolation = new AtomicBoolean();

        private record Pending(LogicalRequest request, long invoked, CompletableFuture<ClientOutcome> future, AtomicBoolean done) {}

        @Override public CompletionStage<ClientOutcome> submit(LogicalRequest request) {
            var id = request.id(RUN, SLOT);
            if (rejectAlways || rejectFirst && tried.add(id)) {
                ledger.rejected(); return CompletableFuture.failedFuture(new RejectedExecutionException("fixture admission"));
            }
            if (accepted.putIfAbsent(id, request) != null) { return CompletableFuture.failedFuture(new IllegalArgumentException("duplicate fixture ID")); }
            if (request.phase() == TracePhase.WARMUP && warmupAllowed != null && !warmupAllowed.get()) { warmupViolation.set(true); }
            var op = ClientOutcome.Operation.of(request.command());
            ledger.admitted(op); ledger.attempt(op);
            peak.accumulateAndGet(active.incrementAndGet(), Math::max);
            var p = new Pending(request, System.nanoTime(), new CompletableFuture<>(), new AtomicBoolean());
            pending.put(id, p); submitted.countDown();
            if (!hold) {
                executor.schedule(() -> finish(p, respondNoResponse ? ClientOutcome.Category.NO_RESPONSE : ClientOutcome.Category.SUCCESS,
                        respondNoResponse ? ClientOutcome.Reason.DEADLINE : ClientOutcome.Reason.SUCCESS), delayMillis, TimeUnit.MILLISECONDS);
            }
            return p.future().minimalCompletionStage();
        }

        private void finish(Pending p, ClientOutcome.Category category, ClientOutcome.Reason reason) {
            if (!p.done().compareAndSet(false, true)) { return; }
            var r = p.request(); var op = ClientOutcome.Operation.of(r.command());
            ledger.terminal(op, category, 1); active.decrementAndGet();
            long terminal = System.nanoTime();
            Result value = category != ClientOutcome.Category.SUCCESS ? null : r.command() instanceof Command.Get
                    ? Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO)
                    : Result.of(ResultStatus.OK, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
            var id = r.id(badIdentity ? "wrong-run" : RUN, SLOT);
            var outcome = new ClientOutcome(id, op, category, reason, value, p.invoked(), terminal, terminal,
                    r.clientId(), r.sequence() + 1, 1,
                    List.of(new ClientOutcome.Attempt(1, p.invoked(), terminal, value == null ? null : value.status(), null)));
            pending.remove(r.id(RUN, SLOT), p);
            p.future().complete(outcome);
        }
        @Override public boolean cancel(LogicalRequest.RequestId id) {
            if (ignoreCancelAndClose) { return false; }
            Pending p = pending.get(id);
            if (p == null) { return false; }
            finish(p, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CANCELLED);
            return true;
        }
        @Override public ClientAccounting.Snapshot snapshot() {
            return hideLedger ? new ClientAccounting().snapshot() : ledger.snapshot();
        }
        @Override public void close() {
            closes.incrementAndGet();
            if (!ignoreCancelAndClose) {
                for (Pending p : pending.values()) { finish(p, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CLOSED); }
            }
            executor.shutdownNow();
        }
    }
}

