package io.openkache.runner.benchmark.execution;

import io.openkache.core.api.Command;
import io.openkache.core.api.ResultStatus;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static io.openkache.runner.benchmark.execution.ClosedLoopResult.*;

/**
 * 한 JVM의 streaming fixed-N closed-loop coordinator. run은 한 번만 호출하고 client 소유권을 넘긴다.
 * worker별 한 logical 요청만 기다리며 logical stream 분할은 concurrency와 독립적으로 유지한다.
 * client/probe/callback은 비차단이어야 한다. 모든 duration은 System.nanoTime과 같은 clock domain이다.
 */
public final class ClosedLoopRunner {
    /** 검증된 outcome의 비차단 관찰. callback은 동시에 호출될 수 있다. */
    public interface Observer {
        void outcome(LogicalRequest request, long dueNanos, ClientOutcome outcome);
    }
    private final Observer observer;
    private final RunLifecycle lifecycle;
    private enum Resolution { OUTCOME, REJECTED, FAILED }
    private final TraceGenerator generator;
    private final ClosedLoopPlan plan;
    private final BenchmarkClient client;
    private final DrainProbe probe;
    private final AtomicBoolean used = new AtomicBoolean();
    private final AtomicReference<String> failure = new AtomicReference<>();
    private final ConcurrentMap<LogicalRequest.RequestId, Work> outstanding = new ConcurrentHashMap<>();
    private final EnumMap<TracePhase, Stats> stats = new EnumMap<>(TracePhase.class);
    private final List<Event> events = new ArrayList<>();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private long workUntil;

    public ClosedLoopRunner(TraceGenerator generator, ClosedLoopPlan plan,
            BenchmarkClient client, DrainProbe probe) {
        this(generator, plan, client, probe, (request, due, outcome) -> {});
    }

    public ClosedLoopRunner(TraceGenerator generator, ClosedLoopPlan plan,
            BenchmarkClient client, DrainProbe probe, Observer observer) {
        this(generator, plan, client, probe, observer, RunLifecycle.NONE);
    }

    public ClosedLoopRunner(TraceGenerator generator, ClosedLoopPlan plan,
            BenchmarkClient client, DrainProbe probe, Observer observer, RunLifecycle lifecycle) {
        this.observer = Objects.requireNonNull(observer);
        this.lifecycle = Objects.requireNonNull(lifecycle);
        this.generator = Objects.requireNonNull(generator);
        this.plan = Objects.requireNonNull(plan);
        this.client = Objects.requireNonNull(client);
        this.probe = Objects.requireNonNull(probe);
        if (plan.concurrency() > generator.config().streamCount() || generator.config().streamCount() > 1024) {
            throw new IllegalArgumentException("requires concurrency<=streams<=1024");
        }
        try {
            Math.addExact(generator.config().keyCount(),
                    Math.addExact(plan.warmupRequests(), plan.measuredRequests()));
        } catch (ArithmeticException error) { throw new IllegalArgumentException("total request overflow", error); }
    }

    public ClosedLoopResult run() {
        if (!used.compareAndSet(false, true)) { throw new IllegalStateException("runner is one-shot"); }
        long started = now();
        workUntil = started + plan.limits().workNanos();
        boolean interrupted = false;
        boolean terminated = false;
        DrainProbe.Observation cleanup = null;
        try {
            if (client.snapshot().admitted() != 0 || client.snapshot().pending() != 0) { fail("CLIENT_ALREADY_USED"); }
            if (failure.get() == null) { barrier(Stage.PREFLIGHT, 0); }
            for (TracePhase phase : TracePhase.values()) {
                if (failure.get() != null) { break; }
                long count = switch (phase) {
                    case LOAD -> generator.config().keyCount();
                    case WARMUP -> plan.warmupRequests();
                    case MEASURE -> plan.measuredRequests();
                };
                executePhase(phase, count);
                if (failure.get() != null) { break; }
                Phase snapshot = stats.get(phase).snapshot();
                if (!reconciles(snapshot) || snapshot.scheduled() != count || snapshot.success() != count) {
                    invalidatePhase(phase, "PHASE_ACCOUNTING_MISMATCH_" + phase); break;
                }
                long minimum = switch (phase) {
                    case LOAD -> 0;
                    case WARMUP -> plan.minimumWarmupNanos();
                    case MEASURE -> plan.minimumMeasureNanos();
                };
                if (snapshot.elapsedNanos() < minimum) {
                    invalidatePhase(phase, "INSUFFICIENT_DURATION_" + phase); break;
                }
                Stage drain = switch (phase) {
                    case LOAD -> Stage.LOAD_DRAIN;
                    case WARMUP -> Stage.WARMUP_DRAIN;
                    case MEASURE -> Stage.FINAL_DRAIN;
                };
                barrier(drain, successfulWrites());
            }
        } catch (InterruptedException interruptedRun) {
            interrupted = true; Thread.interrupted(); fail("RUN_INTERRUPTED");
        } catch (RuntimeException unexpected) {
            fail("RUN_EXCEPTION_" + unexpected.getClass().getSimpleName());
        } finally {
            long cleanupStart = now();
            observeStage(Stage.CLEANUP, cleanupStart);
            long cleanupUntil = cleanupStart + plan.limits().cleanupNanos();
            for (Work work : outstanding.values()) {
                try { client.cancel(work.id); } catch (RuntimeException error) { fail("CANCEL_EXCEPTION"); }
            }
            try { client.close(); } catch (RuntimeException error) { fail("CLOSE_EXCEPTION"); }
            workers.shutdownNow();
            try {
                terminated = workers.awaitTermination(Math.max(1, remaining(cleanupUntil)), TimeUnit.NANOSECONDS);
                if (terminated) {
                    for (Work work : outstanding.values()) {
                        if (!work.awaiting) { work.notAdmitted(); }
                    }
                }
                do {
                    cleanup = safeProbe(successfulWrites(), true);
                    if (outstanding.isEmpty() && client.snapshot().pending() == 0
                            && cleanup.status() != DrainProbe.Status.WAIT) { break; }
                    if (remaining(cleanupUntil) <= 0) { break; }
                    pause(cleanupUntil);
                } while (true);
            } catch (InterruptedException cleanupInterrupted) {
                interrupted = true; Thread.interrupted(); fail("CLEANUP_INTERRUPTED");
            } catch (RuntimeException error) { fail("CLEANUP_EXCEPTION"); }
            if (!terminated) { fail("WORKERS_NOT_TERMINATED"); }
            if (!outstanding.isEmpty() || client.snapshot().pending() != 0) { fail("UNRESOLVED_REQUESTS"); }
            if (cleanup == null || cleanup.status() != DrainProbe.Status.READY) { fail("CLEANUP_NOT_DRAINED"); }
            boolean clean = terminated && outstanding.isEmpty() && client.snapshot().pending() == 0
                    && cleanup != null && cleanup.status() == DrainProbe.Status.READY;
            events.add(new Event(Stage.CLEANUP, cleanupStart, now(), clean,
                    clean ? "closed_and_drained" : "incomplete_cleanup", cleanup));
            try { lifecycle.finished(now()); }
            catch (RuntimeException error) { fail("LIFECYCLE_FINISH_FAILED"); }
        }
        EnumMap<TracePhase, Phase> phases = new EnumMap<>(TracePhase.class);
        if (failure.get() != null && !stats.isEmpty()) {
            stats.get(stats.keySet().stream().toList().getLast()).after = client.snapshot();
        }
        stats.forEach((phase, value) -> {
            value.after = value.after == null ? client.snapshot() : value.after;
            phases.put(phase, value.snapshot());
        });
        long observed = phases.values().stream().flatMap(p -> p.byOperation().values().stream())
                .mapToLong(Counts::observedOutcomes).sum();
        if (client.snapshot().admitted() != observed) { fail("FINAL_CLIENT_ACCOUNTING_MISMATCH"); }
        if (phases.values().stream().anyMatch(p -> p.unresolved() != 0)) { fail("UNRESOLVED_COHORT"); }
        ClosedLoopResult result = new ClosedLoopResult(failure.get() == null, failure.get(), started, now(),
                phases, events, client.snapshot(), cleanup, terminated, successfulWrites());
        if (interrupted) { Thread.currentThread().interrupt(); }
        return result;
    }

    private void executePhase(TracePhase phase, long total) throws InterruptedException {
        Stats counters = new Stats(phase, total, client.snapshot());
        stats.put(phase, counters);
        int concurrency = plan.concurrency();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch release = new CountDownLatch(1);
        CompletionService<Void> completion = new ExecutorCompletionService<>(workers);
        List<Future<Void>> jobs = new ArrayList<>();
        for (int worker = 0; worker < concurrency; worker++) {
            int owner = worker;
            jobs.add(completion.submit(() -> {
                ready.countDown();
                try {
                    release.await();
                    runWorker(counters, owner);
                } catch (InterruptedException interrupted) {
                    fail("WORKER_INTERRUPTED"); Thread.currentThread().interrupt();
                } catch (RuntimeException error) { fail("WORKER_EXCEPTION_" + error.getClass().getSimpleName()); }
                return null;
            }));
        }
        boolean allFinished = false;
        try {
            if (!ready.await(Math.max(1, remaining(workUntil)), TimeUnit.NANOSECONDS)) { fail("WORK_LIMIT"); }
            counters.start = now();
            observeStage(Stage.valueOf(phase.name()), counters.start);
            release.countDown();
            for (int i = 0; i < concurrency && failure.get() == null; i++) {
                long left = remaining(workUntil);
                if (left <= 0) { fail("WORK_LIMIT"); break; }
                Future<Void> job = completion.poll(left, TimeUnit.NANOSECONDS);
                if (job == null) { fail("WORK_LIMIT"); break; }
                try { job.get(); } catch (ExecutionException error) { fail("WORKER_FUTURE_FAILED"); }
            }
            allFinished = failure.get() == null;
        } finally {
            release.countDown();
            if (!allFinished) { for (Future<Void> job : jobs) { job.cancel(true); } }
            counters.end = now();
            counters.after = client.snapshot();
            events.add(new Event(Stage.valueOf(phase.name()), counters.start, counters.end,
                    allFinished, allFinished ? "workers_complete" : failure.get(), null));
        }
    }

    private void runWorker(Stats counters, int owner) throws InterruptedException {
        List<Iterator<LogicalRequest>> streams = new ArrayList<>();
        for (int stream = owner; stream < generator.config().streamCount(); stream += plan.concurrency()) {
            streams.add(generator.stream(counters.phase, counters.total, stream));
        }
        boolean any;
        do {
            any = false;
            for (Iterator<LogicalRequest> cursor : streams) {
                if (failure.get() != null) { return; }
                if (remaining(workUntil) <= 0) { fail("WORK_LIMIT"); return; }
                if (!cursor.hasNext()) { continue; }
                any = true;
                long scheduled = now();
                LogicalRequest request = cursor.next();
                Work work = new Work(counters, request, scheduled);
                if (outstanding.putIfAbsent(work.id, work) != null) { fail("DUPLICATE_GENERATED_ID"); return; }
                counters.generated(work.operation);
                long admissionUntil = scheduled + plan.limits().admissionWaitNanos();
                boolean completed = false;
                for (int attempt = 0; attempt < plan.limits().maxSubmissionAttempts()
                        && failure.get() == null; attempt++) {
                    if (remaining(admissionUntil) <= 0 || remaining(workUntil) <= 0) { break; }
                    CompletableFuture<Resolution> handled = submit(work);
                    long wait = Math.min(plan.limits().completionWaitNanos(), remaining(workUntil));
                    if (wait <= 0) { fail("WORK_LIMIT"); return; }
                    Resolution result;
                    try { result = handled.get(wait, TimeUnit.NANOSECONDS); }
                    catch (TimeoutException error) { fail(remaining(workUntil) <= 0 ? "WORK_LIMIT" : "REQUEST_WAIT_TIMEOUT"); return; }
                    catch (ExecutionException error) { fail("HANDLER_FAILED"); return; }
                    if (result != Resolution.REJECTED) { completed = true; break; }
                    long pause = Math.min(plan.limits().pollNanos(), Math.min(remaining(admissionUntil), remaining(workUntil)));
                    if (pause > 0) { TimeUnit.NANOSECONDS.sleep(pause); }
                }
                if (!completed) {
                    work.notAdmitted();
                    fail("ADMISSION_EXHAUSTED"); return;
                }
            }
        } while (any);
    }

    private CompletableFuture<Resolution> submit(Work work) {
        CompletableFuture<Resolution> handled = new CompletableFuture<>();
        work.awaiting = true;
        work.counters.call();
        CompletionStage<ClientOutcome> stage;
        try { stage = Objects.requireNonNull(client.submit(work.request), "client stage"); }
        catch (RuntimeException error) { stage = CompletableFuture.failedFuture(error); }
        stage.whenComplete((outcome, error) -> {
            try {
                Throwable cause = unwrap(error);
                if (cause instanceof RejectedExecutionException) {
                    work.awaiting = false;
                    work.counters.rejected(); handled.complete(Resolution.REJECTED);
                } else if (cause != null) {
                    work.notAdmitted(); fail("SUBMISSION_FAILED_" + cause.getClass().getSimpleName());
                    handled.complete(Resolution.FAILED);
                } else if (!valid(work, outcome)) {
                    work.invalid(); fail("INVALID_CLIENT_OUTCOME"); handled.complete(Resolution.FAILED);
                } else {
                    observer.outcome(work.request, work.scheduled, outcome);
                    work.outcome(outcome);
                    if (outcome.category() != ClientOutcome.Category.SUCCESS) { fail("CLIENT_OUTCOME_" + outcome.reason()); }
                    handled.complete(Resolution.OUTCOME);
                }
            } catch (RuntimeException badHandler) {
                work.invalid(); fail("OUTCOME_HANDLER_FAILED"); handled.complete(Resolution.FAILED);
            }
        });
        return handled;
    }

    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) { error = error.getCause(); }
        return error;
    }

    private boolean valid(Work work, ClientOutcome outcome) {
        if (outcome == null || !outcome.id().equals(work.id) || outcome.operation() != work.operation
                || outcome.invokedNanos() - work.scheduled < 0 || outcome.observedElapsedNanos() < 0
                || outcome.terminalNanos() - outcome.decidedNanos() < 0
                || outcome.rounds() < outcome.attempts().size() || outcome.attempts().size() > 100
                || outcome.wireClientId() < 0 || outcome.wireOperationId() < 0) { return false; }
        int expected = 1;
        for (ClientOutcome.Attempt attempt : outcome.attempts()) {
            if (attempt.number() != expected++ || attempt.startedNanos() - outcome.invokedNanos() < 0
                    || attempt.endedNanos() != null && (attempt.endedNanos() - attempt.startedNanos() < 0
                        || outcome.decidedNanos() - attempt.endedNanos() < 0)) { return false; }
        }
        boolean successShape = false;
        if (outcome.result() != null) {
            var r = outcome.result();
            if (work.request.command() instanceof Command.Get) {
                successShape = r.status() == ResultStatus.OK && r.value().isPresent()
                        || r.status() == ResultStatus.MISS && r.value().isEmpty();
            } else if (r.value().isEmpty()) {
                successShape = r.status() == ResultStatus.OK
                        || work.request.command() instanceof Command.Delete && r.status() == ResultStatus.MISS;
            }
        }
        return switch (outcome.category()) {
            case SUCCESS -> successShape && outcome.reason() == ClientOutcome.Reason.SUCCESS;
            case ERROR_RESPONSE -> outcome.result() != null && !successShape
                    && outcome.reason() != ClientOutcome.Reason.SUCCESS;
            case NO_RESPONSE -> outcome.reason() != ClientOutcome.Reason.SUCCESS
                    && (outcome.result() == null || outcome.result().status() == ResultStatus.TIMEOUT);
        };
    }

    private boolean reconciles(Phase phase) {
        if (phase.clientAfter().rejected() - phase.clientBefore().rejected() != phase.admissionRejections()) { return false; }
        for (var op : ClientOutcome.Operation.values()) {
            var own = phase.byOperation().get(op);
            var before = phase.clientBefore().byOperation().get(op);
            var after = phase.clientAfter().byOperation().get(op);
            if (own.unresolved() != 0 || own.notAdmitted() != 0
                    || after.admitted() - before.admitted() != own.observedOutcomes()
                    || after.success() - before.success() != own.success()
                    || after.errorResponse() - before.errorResponse() != own.errorResponse()
                    || after.noResponse() - before.noResponse() != own.noResponse()
                    || after.attempts() - before.attempts() != own.attempts()
                    || after.retriedLogical() - before.retriedLogical() != own.retriedLogical()
                    || after.pending() != 0) { return false; }
        }
        return true;
    }

    private void barrier(Stage stage, long expected) throws InterruptedException {
        long start = now();
        observeStage(stage, start);
        long until = start + Math.min(plan.limits().barrierWaitNanos(), Math.max(1, remaining(workUntil)));
        DrainProbe.Observation observation;
        do {
            observation = safeProbe(expected, false);
            if (observation.status() != DrainProbe.Status.WAIT || remaining(until) <= 0) { break; }
            pause(until);
        } while (true);
        boolean ready = observation.status() == DrainProbe.Status.READY && remaining(workUntil) > 0;
        events.add(new Event(stage, start, now(), ready, observation.reason(), observation));
        if (!ready) { fail("BARRIER_" + stage + "_" + observation.status()); }
    }

    private DrainProbe.Observation safeProbe(long expected, boolean afterClose) {
        try { return Objects.requireNonNull(afterClose ? probe.afterClose(expected) : probe.observe(expected)); }
        catch (RuntimeException error) {
            return new DrainProbe.Observation(DrainProbe.Status.FAILED,
                    "probe_exception_" + error.getClass().getSimpleName(), Map.of());
        }
    }

    private void pause(long until) throws InterruptedException {
        long nanos = Math.min(plan.limits().pollNanos(), remaining(until));
        if (nanos > 0) { TimeUnit.NANOSECONDS.sleep(nanos); }
    }
    private long successfulWrites() { return stats.values().stream().map(Stats::snapshot).mapToLong(Phase::successfulWrites).sum(); }
    private void fail(String reason) { failure.compareAndSet(null, reason); }
    private void invalidatePhase(TracePhase phase, String reason) {
        fail(reason);
        int index = events.size() - 1;
        Event previous = events.get(index);
        if (previous.stage() == Stage.valueOf(phase.name())) {
            events.set(index, new Event(previous.stage(), previous.startedNanos(), previous.endedNanos(), false, reason, null));
        }
    }
    private void observeStage(Stage stage, long at) {
        try { lifecycle.stage(stage, at); }
        catch (RuntimeException error) { fail("LIFECYCLE_STAGE_FAILED"); }
    }
    private static long now() { return System.nanoTime(); }
    private static long remaining(long until) { return until - now(); }

    private final class Work {
        final Stats counters;
        final LogicalRequest request;
        final LogicalRequest.RequestId id;
        final ClientOutcome.Operation operation;
        final long scheduled;
        final AtomicBoolean done = new AtomicBoolean();
        volatile boolean awaiting;
        Work(Stats counters, LogicalRequest request, long scheduled) {
            this.counters = counters; this.request = request; this.scheduled = scheduled;
            id = request.id(plan.runId(), plan.loadSlot()); operation = ClientOutcome.Operation.of(request.command());
        }
        void outcome(ClientOutcome outcome) {
            if (done.compareAndSet(false, true)) { counters.outcome(operation, outcome); outstanding.remove(id, this); }
        }
        void notAdmitted() {
            if (done.compareAndSet(false, true)) { counters.notAdmitted(operation); outstanding.remove(id, this); }
        }
        void invalid() {
            if (done.compareAndSet(false, true)) { counters.invalid(); outstanding.remove(id, this); }
        }
    }

    private static final class Stats {
        final TracePhase phase;
        final long total;
        final ClientAccounting.Snapshot before;
        final EnumMap<ClientOutcome.Operation, long[]> counts = new EnumMap<>(ClientOutcome.Operation.class);
        long calls, rejections, invalid;
        int slots, peak;
        volatile long start, end;
        volatile ClientAccounting.Snapshot after;
        Stats(TracePhase phase, long total, ClientAccounting.Snapshot before) {
            this.phase = phase; this.total = total; this.before = before;
            for (var op : ClientOutcome.Operation.values()) { counts.put(op, new long[7]); }
        }
        synchronized void generated(ClientOutcome.Operation op) { counts.get(op)[0]++; peak = Math.max(peak, ++slots); }
        synchronized void call() { calls++; }
        synchronized void rejected() { rejections++; }
        synchronized void invalid() { invalid++; slots--; }
        synchronized void notAdmitted(ClientOutcome.Operation op) { counts.get(op)[4]++; slots--; }
        synchronized void outcome(ClientOutcome.Operation op, ClientOutcome outcome) {
            long[] c = counts.get(op);
            switch (outcome.category()) {
                case SUCCESS -> c[1]++;
                case ERROR_RESPONSE -> c[2]++;
                case NO_RESPONSE -> c[3]++;
            }
            c[5] += outcome.attempts().size();
            if (outcome.attempts().size() > 1) { c[6]++; }
            slots--;
        }
        synchronized Phase snapshot() {
            EnumMap<ClientOutcome.Operation, Counts> copy = new EnumMap<>(ClientOutcome.Operation.class);
            counts.forEach((op, c) -> copy.put(op, new Counts(c[0], c[1], c[2], c[3], c[4],
                    c[0] - c[1] - c[2] - c[3] - c[4], c[5], c[6])));
            return new Phase(phase, total, start, end, copy, calls, rejections, invalid, peak, before, after);
        }
    }
}

