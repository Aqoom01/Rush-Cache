package io.openkache.runner.benchmark.measurement;

import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.*;

/**
 * 1초 목표 주기의 실제 process 관측 구간 + terminal timestamp 기준 sparse 요청 창.
 * 요청 callback은 management API/파일 I/O를 호출하지 않는다. 원본 payload/요청 ID 목록을 저장하지 않는다.
 * source/client snapshot/clock은 비차단 계약이다. 비협력 native 호출의 강제 종료기는 아니다.
 */
public final class JvmWindowCollector implements RunLifecycle, ClosedLoopRunner.Observer, OpenLoopRunner.Observer, AutoCloseable {
    public static final long PERIOD_NS = 1_000_000_000L;
    public enum Role { LOAD_PROCESS, SERVER_PROCESS, SHARED_FIXTURE }
    public record Limits(int maxSamples, int maxOutcomeBuckets, long shutdownWaitNanos) {
        public Limits {
            if (maxSamples<1 || maxSamples>100_000 || maxOutcomeBuckets<1 || maxOutcomeBuckets>100_000
                    || shutdownWaitNanos<1 || shutdownWaitNanos>10_000_000_000L) { throw new IllegalArgumentException("collector limits"); }
        }
    }
    public record Settings(String runId, String loadSlot, String clockId, Role role, Limits limits, Set<String> requiredSources) {
        public Settings {
            if (Objects.requireNonNull(runId).isBlank() || Objects.requireNonNull(loadSlot).isBlank()
                    || Objects.requireNonNull(clockId).isBlank()) { throw new IllegalArgumentException("identity required"); }
            Objects.requireNonNull(role); Objects.requireNonNull(limits); requiredSources=Set.copyOf(requiredSources);
            if (!SOURCE_KEYS.containsAll(requiredSources)) { throw new IllegalArgumentException("unknown required source"); }
        }
        public static Settings local(String run, String slot, String clock, Role role) {
            return new Settings(run,slot,clock,role,new Limits(10_000,30_000,1_000_000_000L),
                    Set.of("process_cpu_ns","heap_used_bytes","non_heap_used_bytes","gc_count","gc_time_ns"));
        }
    }
    public static final Set<String> SOURCE_KEYS = Set.of("process_cpu_ns","heap_used_bytes","heap_committed_bytes","heap_max_bytes",
            "non_heap_used_bytes","non_heap_committed_bytes","non_heap_max_bytes","gc_count","gc_time_ns","rss_bytes","allocation_bytes","gc_pause_ns");
    private static final List<String> CUMULATIVE = List.of("process_cpu_ns","gc_count","gc_time_ns");
    public record CounterDelta(long admitted, long success, long errorResponse, long noResponse, long attempts, long pendingAtEnd) {}
    public record Sample(long sequence, ClosedLoopResult.Stage stage, long intervalStartNanos, long intervalEndNanos,
            long collectionStartedNanos, long collectionEndedNanos, boolean baseline,
            JvmSnapshotSource.Snapshot jvm, ClientAccounting.Snapshot client,
            Map<String,JvmSnapshotSource.Value> resourceDeltas, Map<ClientOutcome.Operation,CounterDelta> clientDeltas) {
        public Sample { resourceDeltas=Map.copyOf(resourceDeltas); clientDeltas=Map.copyOf(clientDeltas); }
    }
    public record Transition(ClosedLoopResult.Stage stage, long requestedNanos, long observedNanos, long sampleSequence) {}
    public record RequestWindow(long index, long startNanos, long endNanos, TracePhase cohortPhase,
            ClientOutcome.Operation operation, long success, long errorResponse, long noResponse) {
        public long outcomes() { return success+errorResponse+noResponse; }
    }
    public record Snapshot(String token, Settings settings, long anchorNanos, long finishedNanos,
            List<Sample> samples, List<Transition> transitions, List<RequestWindow> requestWindows,
            List<String> issues, boolean samplerTerminated, long lateCallbacks, long sampledPeakPending) {
        public Snapshot {
            samples=List.copyOf(samples); transitions=List.copyOf(transitions); requestWindows=List.copyOf(requestWindows); issues=List.copyOf(issues);
        }
        public boolean valid() { return issues.isEmpty() && samplerTerminated; }
    }
    private record Key(long index, TracePhase phase, ClientOutcome.Operation operation) implements Comparable<Key> {
        @Override public int compareTo(Key other) { int n=Long.compare(index,other.index); if(n==0){n=phase.compareTo(other.phase);} return n==0?operation.compareTo(other.operation):n; }
    }
    private final Settings settings;
    private final JvmSnapshotSource source;
    private final Supplier<ClientAccounting.Snapshot> client;
    private final LongSupplier clock;
    private final boolean automatic;
    private final String token=UUID.randomUUID().toString();
    private final Object sampleLock=new Object();
    private final ConcurrentLinkedQueue<Sample> samples=new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Transition> transitions=new ConcurrentLinkedQueue<>();
    // CHM computeIfAbsent performs one creation per key; skip-list mapping may run concurrently.
    private final ConcurrentHashMap<Key,AtomicLongArray> buckets=new ConcurrentHashMap<>();
    private final Set<String> issues=new ConcurrentSkipListSet<>();
    private final AtomicInteger bucketCount=new AtomicInteger();
    private final AtomicLong late=new AtomicLong(), peakPending=new AtomicLong();
    private final ReentrantReadWriteLock callbackGate=new ReentrantReadWriteLock();
    private final AtomicBoolean closed=new AtomicBoolean();
    private volatile boolean started, accepting=true, terminated=true, finishedReady;
    private volatile long anchor, ended;
    private ClosedLoopResult.Stage stage;
    private Sample previous;
    private long sequence, lastStageTime;
    private ScheduledExecutorService sampler;
    private volatile ScheduledFuture<?> ticking;

    public JvmWindowCollector(Settings settings, JvmSnapshotSource source, Supplier<ClientAccounting.Snapshot> client) {
        this(settings,source,client,System::nanoTime,true);
    }
    JvmWindowCollector(Settings settings, JvmSnapshotSource source, Supplier<ClientAccounting.Snapshot> client, LongSupplier clock, boolean automatic) {
        this.settings=Objects.requireNonNull(settings); this.source=Objects.requireNonNull(source);
        this.client=Objects.requireNonNull(client); this.clock=Objects.requireNonNull(clock); this.automatic=automatic;
    }
    @Override public void stage(ClosedLoopResult.Stage next, long at) {
        synchronized(sampleLock) {
            if(closed.get()) { throw new IllegalStateException("collector finished"); }
            Objects.requireNonNull(next);
            if(!started) { anchor=at; stage=next; started=true; }
            if(at-anchor<0 || !transitions.isEmpty() && at-lastStageTime<0) { issue("STAGE_CLOCK_ORDER"); }
            Sample sample=capture();
            transitions.add(new Transition(next,at,sample.collectionEndedNanos(),sample.sequence())); stage=next;
            lastStageTime=at;
            if(automatic && sampler==null) {
                sampler=Executors.newSingleThreadScheduledExecutor(r -> Thread.ofPlatform().daemon(true).name("benchmark-jvm-"+token).unstarted(r));
                terminated=false;
                ticking=sampler.scheduleAtFixedRate(() -> {
                    try { synchronized(sampleLock) { if(closed.get()){return;} capture(); } }
                    catch(RuntimeException error) { issues.add("PERIODIC_SAMPLE_FAILED:"+error.getClass().getSimpleName()); if(ticking!=null){ticking.cancel(false);} }
                },PERIOD_NS,PERIOD_NS,TimeUnit.NANOSECONDS);
            }
        }
    }
    /** 동일 source/clock을 쓰는 수동 검증·명시 snapshot용. */
    public void sampleNow() {
        synchronized(sampleLock) { if(!started || closed.get()){throw new IllegalStateException("collector not active");} capture(); }
    }
    private Sample capture() {
        if(sequence>=settings.limits().maxSamples()) { issue("SAMPLE_CAPACITY"); }
        long begin=clock.getAsLong(); JvmSnapshotSource.Snapshot observed;
        ClientAccounting.Snapshot accounting;
        try { observed=Objects.requireNonNull(source.sample()); accounting=Objects.requireNonNull(client.get()); }
        catch(RuntimeException error) { issues.add("SOURCE_EXCEPTION:"+error.getClass().getSimpleName()); throw error; }
        long end=clock.getAsLong();
        if(observed.startedNanos()-begin<0 || end-observed.endedNanos()<0 || end-begin<0
                || previous!=null && observed.startedNanos()-previous.jvm().endedNanos()<0) { issue("SOURCE_CLOCK_ORDER"); }
        if(previous!=null && (!previous.jvm().processInstanceId().equals(observed.processInstanceId()) || previous.jvm().pid()!=observed.pid())) { issue("PROCESS_INSTANCE_CHANGED"); }
        for(String required:settings.requiredSources()) {
            var value=observed.values().get(required);
            if(value==null || !value.status().equals("measured")) { issues.add("REQUIRED_SOURCE_UNAVAILABLE:"+required); }
        }
        if(!observed.values().keySet().equals(SOURCE_KEYS)) { issues.add("SOURCE_KEY_COVERAGE"); }
        Map<String,JvmSnapshotSource.Value> deltas=new LinkedHashMap<>();
        for(String key:CUMULATIVE) {
            var after=observed.values().get(key); var before=previous==null?null:previous.jvm().values().get(key);
            String unit=key.equals("gc_count")?"count":"ns";
            if(after==null || before==null || after.value()==null || before.value()==null) {
                deltas.put(key,JvmSnapshotSource.Value.unsupported(unit,previous==null?"baseline":"counter_source_unavailable"));
            } else if(after.value()<before.value()) {
                issues.add("COUNTER_RESET:"+key); deltas.put(key,JvmSnapshotSource.Value.unsupported(unit,"counter_reset"));
            } else { deltas.put(key,JvmSnapshotSource.Value.measured(after.value()-before.value(),unit)); }
        }
        Map<ClientOutcome.Operation,CounterDelta> clientDeltas=new EnumMap<>(ClientOutcome.Operation.class);
        if(!accounting.byOperation().keySet().equals(Set.of(ClientOutcome.Operation.values()))) { issue("CLIENT_OPERATION_COVERAGE"); }
        for(var op:ClientOutcome.Operation.values()) {
            var value=accounting.byOperation().get(op);
            if(value.admitted()<0 || value.success()<0 || value.errorResponse()<0 || value.noResponse()<0 || value.attempts()<0 || value.pending()<0
                    || value.admitted()-value.success()-value.errorResponse()-value.noResponse()!=value.pending()) { issue("CLIENT_CONSERVATION"); }
            if(previous!=null) {
                var before=previous.client().byOperation().get(op);
                var d=new CounterDelta(value.admitted()-before.admitted(),value.success()-before.success(),value.errorResponse()-before.errorResponse(),
                        value.noResponse()-before.noResponse(),value.attempts()-before.attempts(),value.pending());
                if(d.admitted()<0 || d.success()<0 || d.errorResponse()<0 || d.noResponse()<0 || d.attempts()<0) { issue("CLIENT_COUNTER_RESET"); }
                clientDeltas.put(op,d);
            }
        }
        peakPending.accumulateAndGet(accounting.pending(),Math::max);
        var sample=new Sample(++sequence,stage,previous==null?observed.endedNanos():previous.jvm().endedNanos(),observed.endedNanos(),begin,end,
                previous==null,observed,accounting,deltas,clientDeltas);
        samples.add(sample); previous=sample; return sample;
    }
    @Override public void outcome(OpenLoopResult.ScheduledOutcome event) { outcome(event.request(),event.dueNanos(),event.outcome()); }
    @Override public void outcome(LogicalRequest request, long due, ClientOutcome outcome) {
        var gate=callbackGate.readLock(); gate.lock();
        try {
            if(!accepting) { late.incrementAndGet(); return; }
            if(!started || !request.id(settings.runId(),settings.loadSlot()).equals(outcome.id())
                    || ClientOutcome.Operation.of(request.command())!=outcome.operation() || outcome.invokedNanos()-due<0
                    || outcome.terminalNanos()-outcome.invokedNanos()<0 || outcome.terminalNanos()-anchor<0
                    || clock.getAsLong()-outcome.terminalNanos()<0) { issue("OUTCOME_ID_OR_CLOCK"); }
            long index=(outcome.terminalNanos()-anchor)/PERIOD_NS;
            if(index>86_400) { issue("OUTCOME_TIME_RANGE"); }
            var key=new Key(index,request.phase(),outcome.operation());
            var counts=buckets.computeIfAbsent(key, ignored -> {
                if(bucketCount.incrementAndGet()>settings.limits().maxOutcomeBuckets()) { bucketCount.decrementAndGet(); issue("OUTCOME_BUCKET_CAPACITY"); }
                return new AtomicLongArray(3);
            });
            counts.incrementAndGet(outcome.category().ordinal());
        } finally { gate.unlock(); }
    }
    @Override public void finished(long at) {
        if(!closed.compareAndSet(false,true)) { return; }
        var gate=callbackGate.writeLock(); gate.lock(); try { accepting=false; } finally { gate.unlock(); }
        boolean interrupted=false;
        if(ticking!=null) { ticking.cancel(true); }
        if(sampler!=null) {
            sampler.shutdownNow();
            try { terminated=sampler.awaitTermination(settings.limits().shutdownWaitNanos(),TimeUnit.NANOSECONDS); }
            catch(InterruptedException error) { interrupted=true; terminated=sampler.isTerminated(); issues.add("SAMPLER_CLOSE_INTERRUPTED"); }
        }
        if(!terminated) { issues.add("SAMPLER_NOT_TERMINATED"); }
        if(terminated) {
            synchronized(sampleLock) {
                if(!started) { issues.add("NO_STAGES_OBSERVED"); }
                else { try { capture(); } catch(RuntimeException error) { issues.add("FINAL_SAMPLE_FAILED:"+error.getClass().getSimpleName()); } }
            }
        }
        ended=clock.getAsLong();
        finishedReady=true;
        if(started && (at-anchor<0 || ended-at<0)) { issues.add("FINISH_CLOCK_ORDER"); }
        if(interrupted) { Thread.currentThread().interrupt(); }
        if(!issues.isEmpty()) { throw new IllegalStateException("JVM collection incomplete: "+issues); }
    }
    @Override public void close() { finished(clock.getAsLong()); }
    public Snapshot snapshot() {
        if(!finishedReady) { throw new IllegalStateException("collector must finish first"); }
        var failures=new TreeSet<>(issues); if(late.get()>0) { failures.add("LATE_CALLBACK_AFTER_FINISH"); }
        List<RequestWindow> windows=new ArrayList<>();
        for(var e:new TreeMap<>(buckets).entrySet()) {
            var k=e.getKey(); long start=anchor+k.index()*PERIOD_NS;
            long end=anchor+Math.min((k.index()+1)*PERIOD_NS,ended-anchor);
            windows.add(new RequestWindow(k.index(),start,end,k.phase(),k.operation(),e.getValue().get(0),e.getValue().get(1),e.getValue().get(2)));
        }
        return new Snapshot(token,settings,anchor,ended,List.copyOf(samples),List.copyOf(transitions),windows,List.copyOf(failures),terminated,late.get(),peakPending.get());
    }
    private void issue(String reason) { issues.add(reason); throw new IllegalStateException(reason); }
}
