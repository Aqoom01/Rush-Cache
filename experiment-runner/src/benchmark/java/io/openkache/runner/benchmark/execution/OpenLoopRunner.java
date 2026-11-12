package io.openkache.runner.benchmark.execution;

import io.openkache.core.api.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static io.openkache.runner.benchmark.execution.OpenLoopResult.*;

/**
 * 한 JVM의 fixed-rate open-loop. 응답/상한/거절에 따라 예정 시각을 옮기거나 재제출하지 않는다.
 * run은 one-shot이며 client를 소유해 닫는다. client/probe/observer는 비차단이어야 한다.
 */
public final class OpenLoopRunner {
    /** callback은 동시에 올 수 있다. 요청 전체를 저장하지 않는 비차단 collector를 연결한다. */
    public interface Observer {
        default void decision(Decision decision) {}
        default void outcome(ScheduledOutcome outcome) {}
    }
    private static final Observer NONE = new Observer() {};
    private final TraceGenerator generator;
    private final OpenLoopPlan plan;
    private final BenchmarkClient client;
    private final DrainProbe probe;
    private final Observer observer;
    private final RunLifecycle lifecycle;
    private final AtomicBoolean used = new AtomicBoolean();
    private final AtomicReference<String> fatal = new AtomicReference<>();
    private final AtomicReference<String> issue = new AtomicReference<>();
    private final AtomicInteger occupied = new AtomicInteger();
    private final ConcurrentMap<LogicalRequest.RequestId, Work> pending = new ConcurrentHashMap<>();
    private final EnumMap<TracePhase, Stats> stats = new EnumMap<>(TracePhase.class);
    private final List<Event> events = new ArrayList<>();
    private long workUntil;

    public OpenLoopRunner(TraceGenerator generator, OpenLoopPlan plan, BenchmarkClient client, DrainProbe probe) {
        this(generator, plan, client, probe, NONE);
    }
    public OpenLoopRunner(TraceGenerator generator, OpenLoopPlan plan, BenchmarkClient client,
            DrainProbe probe, Observer observer) {
        this(generator, plan, client, probe, observer, RunLifecycle.NONE);
    }
    public OpenLoopRunner(TraceGenerator generator, OpenLoopPlan plan, BenchmarkClient client,
            DrainProbe probe, Observer observer, RunLifecycle lifecycle) {
        this.generator = Objects.requireNonNull(generator); this.plan = Objects.requireNonNull(plan);
        this.client = Objects.requireNonNull(client); this.probe = Objects.requireNonNull(probe);
        this.observer = Objects.requireNonNull(observer);
        this.lifecycle = Objects.requireNonNull(lifecycle);
        if (generator.config().streamCount() > 1024) { throw new IllegalArgumentException("streams exceed1024"); }
        try {
            Math.addExact(generator.config().keyCount(), Math.addExact(plan.warmupRequests(), plan.measuredRequests()));
            long span = 0;
            for (TracePhase phase : TracePhase.values()) { span = Math.addExact(span, schedule(phase).spanNanos()); }
            if (span > plan.limits().workNanos()) { throw new IllegalArgumentException("schedule spans exceed work budget"); }
        } catch (ArithmeticException error) { throw new IllegalArgumentException("schedule/request overflow", error); }
    }

    public OpenLoopResult run() {
        if (!used.compareAndSet(false, true)) { throw new IllegalStateException("runner is one-shot"); }
        long started = now(); workUntil = started + plan.limits().workNanos();
        boolean interrupted = false;
        DrainProbe.Observation cleanup = null;
        try {
            if (Thread.interrupted()) { throw new InterruptedException(); }
            if (client.snapshot().admitted() != 0 || client.snapshot().pending() != 0) { fail("CLIENT_ALREADY_USED"); }
            if (fatal.get() == null) { barrier(ClosedLoopResult.Stage.PREFLIGHT); }
            for (TracePhase phase : TracePhase.values()) {
                if (fatal.get() != null) { break; }
                emitPhase(phase);
                if (fatal.get() != null) { break; }
                Phase p = stats.get(phase).snapshot();
                if (!reconciles(p)) { fail("PHASE_ACCOUNTING_MISMATCH_" + phase); break; }
                if (phase != TracePhase.MEASURE && !p.healthy()) { fail("SETUP_NOT_HEALTHY_" + phase); break; }
                if (p.uncertainWrites() != 0) { issue.compareAndSet(null, "WRITE_EFFECTS_UNCERTAIN"); }
                ClosedLoopResult.Stage stage = switch (phase) {
                    case LOAD -> ClosedLoopResult.Stage.LOAD_DRAIN;
                    case WARMUP -> ClosedLoopResult.Stage.WARMUP_DRAIN;
                    case MEASURE -> ClosedLoopResult.Stage.FINAL_DRAIN;
                };
                barrier(stage);
            }
        } catch (InterruptedException error) {
            interrupted = true; Thread.interrupted(); fail("RUN_INTERRUPTED");
        } catch (RuntimeException error) { fail("RUN_EXCEPTION_" + error.getClass().getSimpleName()); }
        finally {
            long start = now(), until = start + plan.limits().cleanupNanos();
            observeStage(ClosedLoopResult.Stage.CLEANUP, start);
            for (Work work : pending.values()) {
                try { client.cancel(work.id); } catch (RuntimeException error) { fail("CANCEL_EXCEPTION"); }
            }
            try { client.close(); } catch (RuntimeException error) { fail("CLOSE_EXCEPTION"); }
            try {
                do {
                    cleanup = safeProbe(true);
                    if (pending.isEmpty() && occupied.get() == 0 && client.snapshot().pending() == 0
                            && cleanup.status() != DrainProbe.Status.WAIT) { break; }
                    if (remaining(until) <= 0) { break; }
                    sleep(until);
                } while (true);
            } catch (InterruptedException error) {
                interrupted = true; Thread.interrupted(); fail("CLEANUP_INTERRUPTED");
            }
            boolean ready = pending.isEmpty() && occupied.get() == 0 && client.snapshot().pending() == 0
                    && cleanup != null && cleanup.status() == DrainProbe.Status.READY;
            if (!ready) { fail("CLEANUP_INCOMPLETE"); }
            events.add(new Event(ClosedLoopResult.Stage.CLEANUP, start, now(), ready,
                    ready ? "closed_and_drained" : "unresolved_cleanup", cleanup));
            try { lifecycle.finished(now()); }
            catch (RuntimeException error) { fail("LIFECYCLE_FINISH_FAILED"); }
        }
        if (!stats.isEmpty()) { stats.get(stats.keySet().stream().toList().getLast()).after = client.snapshot(); }
        EnumMap<TracePhase, Phase> phases = new EnumMap<>(TracePhase.class);
        stats.forEach((phase, s) -> phases.put(phase, s.snapshot()));
        boolean all = phases.size() == TracePhase.values().length
                && phases.values().stream().allMatch(p -> p.considered() == p.schedule().requests());
        long outcomes = phases.values().stream().flatMap(p -> p.byOperation().values().stream()).mapToLong(Counts::outcomes).sum();
        if (outcomes != client.snapshot().admitted()) { fail("FINAL_CLIENT_ACCOUNTING_MISMATCH"); }
        if (phases.values().stream().anyMatch(p -> p.unresolved() != 0 || p.invalidOutcomes() != 0)) { fail("UNRESOLVED_COHORT"); }
        boolean clock = all && phases.values().stream().allMatch(p -> p.timingMisses() == 0
                && !p.decisionLag().overflow() && !p.submissionLag().overflow() && !p.admittedLag().overflow());
        if (phases.values().stream().anyMatch(p -> p.decisionLag().overflow()
                || p.submissionLag().overflow() || p.admittedLag().overflow())) { issue.compareAndSet(null, "LAG_SUM_OVERFLOW"); }
        String failure = fatal.get() != null ? fatal.get() : issue.get();
        OpenLoopResult result = new OpenLoopResult(all, all && failure == null, clock,
                all && phases.values().stream().allMatch(Phase::healthy), failure, started, now(), phases,
                events, client.snapshot(), cleanup, occupied.get(), successfulWrites());
        if (interrupted) { Thread.currentThread().interrupt(); }
        return result;
    }

    private void emitPhase(TracePhase phase) throws InterruptedException {
        Stats s = new Stats(phase, schedule(phase), client.snapshot());
        stats.put(phase, s); s.start = now();
        try {
        observeStage(ClosedLoopResult.Stage.valueOf(phase.name()), s.start);
        try {
            for (long index = 0; index < s.schedule.requests() && fatal.get() == null; index++) {
                if (Thread.interrupted()) { throw new InterruptedException(); }
                long due = s.start + s.schedule.offsetNanos(index);
                while (due - now() > 0 && fatal.get() == null) {
                    if (remaining(workUntil) <= 0) { fail("WORK_LIMIT"); break; }
                    long pause = Math.min(plan.limits().pollNanos(), Math.min(due - now(), remaining(workUntil)));
                    if (pause > 0) { TimeUnit.NANOSECONDS.sleep(pause); }
                }
                if (fatal.get() != null) { break; }
                if (remaining(workUntil) <= 0) { fail("WORK_LIMIT"); break; }
                LogicalRequest request = generator.at(phase, index);
                var op = ClientOutcome.Operation.of(request.command());
                long decision = now();
                s.considered(op, decision - due);
                DecisionKind kind;
                if (decision - due > plan.maxDispatchLagNanos()) {
                    s.drop(op, true); kind = DecisionKind.TIMING_MISS;
                } else if (!reserve(s)) {
                    s.drop(op, false); kind = DecisionKind.CAPACITY_DROP;
                } else {
                    kind = DecisionKind.SUBMIT_ATTEMPT;
                    Work work = new Work(s, request, due, now());
                    if (pending.putIfAbsent(work.id, work) != null) {
                        occupied.decrementAndGet(); s.invalid(); fail("DUPLICATE_GENERATED_ID"); break;
                    }
                    s.submission(work.submitted - due);
                    submit(work);
                }
                try { observer.decision(new Decision(phase, index, op, due, decision, kind)); }
                catch (RuntimeException error) { fail("DECISION_OBSERVER_FAILED"); }
            }
        } finally { s.emissionEnd = now(); }
        long until = now() + Math.min(plan.limits().outcomeDrainNanos(), Math.max(1, remaining(workUntil)));
        while ((!pending.isEmpty() || occupied.get() != 0) && fatal.get() == null) {
            if (remaining(until) <= 0) {
                fail(remaining(workUntil) <= 0 ? "WORK_LIMIT" : "OUTCOME_DRAIN_TIMEOUT"); break;
            }
            sleep(until);
        }
        } finally {
            s.drained = now(); s.after = client.snapshot();
            boolean complete = fatal.get() == null && s.snapshot().considered() == s.schedule.requests()
                    && pending.isEmpty() && occupied.get() == 0;
            events.add(new Event(ClosedLoopResult.Stage.valueOf(phase.name()), s.start, s.drained,
                    complete, complete ? "schedule_and_outcomes_complete"
                            : fatal.get() == null ? "interrupted_or_exception" : fatal.get(), null));
        }
    }

    private boolean reserve(Stats s) {
        while (true) {
            int before = occupied.get();
            if (before >= plan.maxInFlight()) { return false; }
            if (occupied.compareAndSet(before, before + 1)) { s.peak(before + 1); return true; }
        }
    }

    private void submit(Work work) {
        CompletionStage<ClientOutcome> stage;
        try { stage = Objects.requireNonNull(client.submit(work.request), "client stage"); }
        catch (RuntimeException error) { resolve(work, null, error); return; }
        try { stage.whenComplete((outcome, error) -> resolve(work, outcome, error)); }
        catch (RuntimeException error) { fail("CALLBACK_REGISTRATION_FAILED"); }
    }

    private void resolve(Work work, ClientOutcome outcome, Throwable error) {
        if (!work.done.compareAndSet(false, true)) { return; }
        try {
            while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) {
                error = error.getCause();
            }
            if (error instanceof RejectedExecutionException) {
                work.stats.rejected(work.op);
            } else if (error != null) {
                work.stats.submitFailed(work.op); fail("SUBMIT_FAILED_" + error.getClass().getSimpleName());
            } else if (!valid(work, outcome)) {
                work.stats.invalid(); fail("INVALID_CLIENT_OUTCOME");
            } else {
                work.stats.outcome(work.op, outcome, outcome.invokedNanos() - work.due);
                try { observer.outcome(new ScheduledOutcome(work.request, work.due, work.submitted, outcome)); }
                catch (RuntimeException badObserver) { fail("OUTCOME_OBSERVER_FAILED"); }
            }
        } catch (RuntimeException handler) { fail("OUTCOME_HANDLER_FAILED"); }
        finally { pending.remove(work.id, work); occupied.decrementAndGet(); }
    }

    private boolean valid(Work w, ClientOutcome o) {
        if (o == null || !o.id().equals(w.id) || o.operation() != w.op
                || o.invokedNanos() - w.submitted < 0 || o.observedElapsedNanos() < 0
                || o.terminalNanos() - o.decidedNanos() < 0 || o.rounds() < o.attempts().size()
                || o.attempts().size() > 100 || o.wireClientId() < 0 || o.wireOperationId() < 0) { return false; }
        int number = 1;
        for (var attempt : o.attempts()) {
            if (attempt.number() != number++ || attempt.startedNanos() - o.invokedNanos() < 0
                    || attempt.endedNanos() != null && (attempt.endedNanos() - attempt.startedNanos() < 0
                        || o.decidedNanos() - attempt.endedNanos() < 0)) { return false; }
        }
        boolean shape = false;
        if (o.result() != null) {
            var r = o.result();
            if (w.request.command() instanceof Command.Get) {
                shape = r.status() == ResultStatus.OK && r.value().isPresent()
                        || r.status() == ResultStatus.MISS && r.value().isEmpty();
            } else if (r.value().isEmpty()) {
                shape = r.status() == ResultStatus.OK
                        || w.request.command() instanceof Command.Delete && r.status() == ResultStatus.MISS;
            }
        }
        return switch (o.category()) {
            case SUCCESS -> shape && o.reason() == ClientOutcome.Reason.SUCCESS;
            case ERROR_RESPONSE -> o.result() != null && !shape && o.reason() != ClientOutcome.Reason.SUCCESS;
            case NO_RESPONSE -> o.reason() != ClientOutcome.Reason.SUCCESS
                    && (o.result() == null || o.result().status() == ResultStatus.TIMEOUT);
        };
    }

    private boolean reconciles(Phase p) {
        long rejected = 0;
        for (var op : ClientOutcome.Operation.values()) {
            var own = p.byOperation().get(op);
            var before = p.clientBefore().byOperation().get(op);
            var after = p.clientAfter().byOperation().get(op);
            rejected += own.clientRejected();
            if (own.unresolved() != 0 || after.admitted() - before.admitted() != own.outcomes()
                    || after.success() - before.success() != own.success()
                    || after.errorResponse() - before.errorResponse() != own.errorResponse()
                    || after.noResponse() - before.noResponse() != own.noResponse()
                    || after.attempts() - before.attempts() != own.attempts()
                    || after.retriedLogical() - before.retriedLogical() != own.retriedLogical()
                    || after.pending() != 0) { return false; }
        }
        return p.clientAfter().rejected() - p.clientBefore().rejected() == rejected;
    }

    private void barrier(ClosedLoopResult.Stage stage) throws InterruptedException {
        long start = now(), until = start + Math.min(plan.limits().barrierNanos(), Math.max(1, remaining(workUntil)));
        observeStage(stage, start);
        DrainProbe.Observation observation;
        do {
            observation = safeProbe(false);
            if (observation.status() != DrainProbe.Status.WAIT || remaining(until) <= 0) { break; }
            sleep(until);
        } while (true);
        boolean ready = observation.status() == DrainProbe.Status.READY && remaining(workUntil) > 0;
        events.add(new Event(stage, start, now(), ready, observation.reason(), observation));
        if (!ready) { fail(remaining(workUntil) <= 0 ? "WORK_LIMIT" : "BARRIER_" + stage + "_" + observation.status()); }
    }
    private DrainProbe.Observation safeProbe(boolean closing) {
        try { return Objects.requireNonNull(closing ? probe.afterClose(successfulWrites()) : probe.observe(successfulWrites())); }
        catch (RuntimeException error) {
            return new DrainProbe.Observation(DrainProbe.Status.FAILED, "probe_exception_" + error.getClass().getSimpleName(), Map.of());
        }
    }
    private FixedRateSchedule schedule(TracePhase phase) {
        return switch (phase) {
            case LOAD -> new FixedRateSchedule(generator.config().keyCount(), plan.loadOpsPerSecond());
            case WARMUP -> new FixedRateSchedule(plan.warmupRequests(), plan.warmupOpsPerSecond());
            case MEASURE -> new FixedRateSchedule(plan.measuredRequests(), plan.measureOpsPerSecond());
        };
    }
    private void sleep(long until) throws InterruptedException {
        long pause = Math.min(plan.limits().pollNanos(), remaining(until));
        if (pause > 0) { TimeUnit.NANOSECONDS.sleep(pause); }
    }
    private long successfulWrites() { return stats.values().stream().map(Stats::snapshot).mapToLong(Phase::successfulWrites).sum(); }
    private void fail(String reason) { fatal.compareAndSet(null, reason); }
    private void observeStage(ClosedLoopResult.Stage stage, long at) {
        try { lifecycle.stage(stage, at); }
        catch (RuntimeException error) { fail("LIFECYCLE_STAGE_FAILED"); }
    }
    private static long now() { return System.nanoTime(); }
    private static long remaining(long until) { return until - now(); }

    private final class Work {
        final Stats stats;
        final LogicalRequest request;
        final LogicalRequest.RequestId id;
        final ClientOutcome.Operation op;
        final long due, submitted;
        final AtomicBoolean done = new AtomicBoolean();
        Work(Stats stats, LogicalRequest request, long due, long submitted) {
            this.stats = stats; this.request = request; this.due = due; this.submitted = submitted;
            id = request.id(plan.runId(), plan.loadSlot()); op = ClientOutcome.Operation.of(request.command());
        }
    }

    private static final class LagState {
        long count, sum, min = Long.MAX_VALUE, max;
        boolean overflow;
        void add(long value) {
            if (value < 0) { throw new IllegalArgumentException("negative lag"); }
            count++; min = Math.min(min, value); max = Math.max(max, value);
            if (!overflow) {
                try { sum = Math.addExact(sum, value); }
                catch (ArithmeticException error) { overflow = true; }
            }
        }
        Lag snapshot() { return new Lag(count, count == 0 || overflow ? null : sum,
                count == 0 ? null : min, count == 0 ? null : max, overflow); }
    }

    private static final class Stats {
        final TracePhase phase;
        final FixedRateSchedule schedule;
        final ClientAccounting.Snapshot before;
        final EnumMap<ClientOutcome.Operation, long[]> counts = new EnumMap<>(ClientOutcome.Operation.class);
        final LagState decisionLag = new LagState(), submitLag = new LagState(), admittedLag = new LagState();
        volatile long start, emissionEnd, drained;
        volatile ClientAccounting.Snapshot after;
        long submissions, invalid;
        int peak;
        Stats(TracePhase phase, FixedRateSchedule schedule, ClientAccounting.Snapshot before) {
            this.phase = phase; this.schedule = schedule; this.before = before;
            for (var op : ClientOutcome.Operation.values()) { counts.put(op, new long[10]); }
        }
        synchronized void considered(ClientOutcome.Operation op, long lag) { counts.get(op)[0]++; decisionLag.add(lag); }
        synchronized void drop(ClientOutcome.Operation op, boolean timing) { counts.get(op)[timing ? 5 : 4]++; }
        synchronized void submission(long lag) { submissions++; submitLag.add(lag); }
        synchronized void rejected(ClientOutcome.Operation op) { counts.get(op)[6]++; }
        synchronized void submitFailed(ClientOutcome.Operation op) { counts.get(op)[7]++; }
        synchronized void invalid() { invalid++; }
        synchronized void peak(int slots) { peak = Math.max(peak, slots); }
        synchronized void outcome(ClientOutcome.Operation op, ClientOutcome o, long lag) {
            long[] c = counts.get(op);
            switch (o.category()) { case SUCCESS -> c[1]++; case ERROR_RESPONSE -> c[2]++; case NO_RESPONSE -> c[3]++; }
            c[8] += o.attempts().size();
            if (o.attempts().size() > 1) { c[9]++; }
            admittedLag.add(lag);
        }
        synchronized Phase snapshot() {
            Map<ClientOutcome.Operation, Counts> copy = new EnumMap<>(ClientOutcome.Operation.class);
            long considered = 0;
            for (var entry : counts.entrySet()) {
                long[] c = entry.getValue(); considered += c[0];
                copy.put(entry.getKey(), new Counts(c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7],
                        c[0] - c[1] - c[2] - c[3] - c[4] - c[5] - c[6] - c[7], c[8], c[9]));
            }
            long end = emissionEnd == 0 ? now() : emissionEnd;
            long unissued = schedule.requests() - considered;
            long dueNotConsidered = Math.max(0, schedule.dueCountAt(end - start) - considered);
            return new Phase(phase, schedule, start, end, drained, unissued,
                    dueNotConsidered, unissued - dueNotConsidered, copy, submissions, invalid, peak,
                    decisionLag.snapshot(), submitLag.snapshot(), admittedLag.snapshot(), before, after);
        }
    }
}

