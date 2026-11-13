package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.balance.*;
import io.openkache.core.balance.placement.PlacementInput;
import io.openkache.core.balance.split.SplitDecision;
import io.openkache.core.migration.*;
import io.openkache.core.migration.experiment.SchedulerAlgorithms;
import io.openkache.core.migration.postcopy.*;
import io.openkache.core.migration.precopy.*;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.migration.strategy.*;
import io.openkache.core.model.*;
import io.openkache.core.tablet.*;
import io.openkache.core.tablet.resize.ResizableSlotScheme;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.Consumer;

/**
 * A의 native 복제 그룹과 B의 분배/균형/증감/스케줄러/복사를 연결한다.
 * 정책·복사 상태는 하나의 controller에서만 변경한다. 요청은 실제 replica RPC를 사용한다.
 * Joint 구성의 routing snapshot은 A1 commit 후 공개한다. Gateway/ledger 자동 복구와 물리 EC2 증감은 별도다.
 */
final class IntegratedCache implements AutoCloseable {
    private static final class Guard { final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true); volatile long frozenBy; }
    private static final class Group {
        final Set<Bytes> keys = ConcurrentHashMap.newKeySet(), uncertain = ConcurrentHashMap.newKeySet();
        final Map<Bytes, Integer> sizes = new ConcurrentHashMap<>();
        final List<TabletRange> quarantined = new CopyOnWriteArrayList<>();
    }
    private static final class Heat { final LongAdder reads = new LongAdder(), writes = new LongAdder(); }
    private record Dirty(Move move, Bytes key) { }
    private record Observation(Map<Bytes, Heat> heat, double seconds, double p99, long completed) { }
    private final IntegrationConfig config;
    private final DistributionScheme scheme;
    private final List<ReplicatedBackend> backends;
    private final List<Group> groups;
    private final AtomicReference<RoutingTable> table;
    private final AtomicReference<List<Integer>> active;
    private final ConcurrentMap<TabletId, Guard> guards = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock metadata = new ReentrantReadWriteLock(true);
    private final ReentrantLock[] keyLocks = new ReentrantLock[1024];
    private final WriteLedger ledger;
    private final AtomicReference<ConcurrentMap<Bytes, Heat>> heat = new AtomicReference<>(new ConcurrentHashMap<>());
    private final ConcurrentLinkedQueue<Long> latency = new ConcurrentLinkedQueue<>();
    private final AtomicInteger latencySize = new AtomicInteger();
    private final ConcurrentLinkedQueue<Dirty> dirty = new ConcurrentLinkedQueue<>();
    private final AtomicInteger dirtySize = new AtomicInteger();
    private final ConcurrentMap<TabletId, Move> inFlight = new ConcurrentHashMap<>(), post = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, String> jobs = new ConcurrentHashMap<>(), repairs = new ConcurrentHashMap<>();
    private final AtomicLong jobId = new AtomicLong();
    private final Set<Long> unresolvedJobs = ConcurrentHashMap.newKeySet();
    private final LongAdder scalingDecisions = new LongAdder(), completedJobs = new LongAdder(), recoveredJobs = new LongAdder();
    private final AtomicInteger pendingJobs = new AtomicInteger(), pendingRepairs = new AtomicInteger();
    private final LongAdder requests = new LongAdder(), failures = new LongAdder(), backendNanos = new LongAdder(), completed = new LongAdder();
    private final ScheduledExecutorService controller = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService repairWorkers = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean routingInitialized = new AtomicBoolean(), routingUncertain = new AtomicBoolean();
    private final JsonLine journal;
    private final DrainStrategy drain;
    private final Map<TabletId, Long> lastMoved = new HashMap<>();
    private final Map<Bytes, Integer> keyIndices = new ConcurrentHashMap<>();
    private final List<Double> history = new ArrayList<>();
    private PerformanceModel model;
    private NodeCountPolicy scaler;
    private Batch batch;
    private volatile String controlState = "IDLE", controlReason = "initial", modelOrigin = "unused";
    private volatile int window, plans, requestedCount = -1;
    private volatile boolean autoEnabled;
    private long observationAt = System.nanoTime(), completedAt;
    private int lastCompletedWindow = -100;
    private int lastStableCount;
    private String lastAction = "";
    private Observation lastObservation;

    IntegratedCache(IntegrationConfig config, List<ReplicatedBackend> backends, JsonLine journal) {
        if (backends.size() != config.groups()) throw new IllegalArgumentException("backend inventory size differs from groups.count");
        this.config = config; this.scheme = config.scheme(); this.backends = List.copyOf(backends); this.journal = journal; lastStableCount = config.initialGroups();
        routingInitialized.set(!config.composed());
        groups = java.util.stream.IntStream.range(0, backends.size()).mapToObj(i -> new Group()).toList();
        active = new AtomicReference<>(java.util.stream.IntStream.range(0, config.initialGroups()).boxed().toList());
        table = new AtomicReference<>(scheme.initial(active.get().stream().map(i -> new NodeId(i)).toList()));
        table.get().tablets().forEach(t -> guards.put(t.id(), new Guard()));
        for (int i = 0; i < keyLocks.length; i++) keyLocks[i] = new ReentrantLock();
        ledger = new WriteLedger(config.number("ledger.capacity", 65536)); drain = DrainStrategies.create(config.text("drain", "preflight-all"));
        if (config.values().containsKey("scaling.history.file")) {
            try { for (String line : Files.readAllLines(Path.of(config.text("scaling.history.file", "")))) if (!line.isBlank() && !line.startsWith("#")) { double v = Double.parseDouble(line.trim()); if (!Double.isFinite(v) || v < 0) throw new IllegalArgumentException("invalid historical rate"); history.add(v); } }
            catch (IOException e) { throw new IllegalArgumentException("cannot load observed history", e); }
        }
        controller.scheduleWithFixedDelay(this::tickSafely, config.number("migration.tick.ms", 10), config.number("migration.tick.ms", 10), TimeUnit.MILLISECONDS);
        if (config.number("control.interval.ms", 0) > 0) controller.scheduleWithFixedDelay(() -> { if (autoEnabled) {
            try { decide(null); } catch (Exception e) { hold("control-error:" + e); }
        } }, config.number("control.interval.ms", 0), config.number("control.interval.ms", 0), TimeUnit.MILLISECONDS);
        journal.event("integrated-started", "\"protocol\":" + JsonLine.quote(config.protocol()) + ",\"distribution\":" + JsonLine.quote(scheme.name())
                + ",\"scheduler\":" + JsonLine.quote(config.text("scheduler", "fifo")) + ",\"scaler\":" + JsonLine.quote(config.text("scaler", "fixed"))
                + ",\"copy\":" + JsonLine.quote(config.copyName()) + ",\"drain\":" + JsonLine.quote(drain.name()) + ",\"preparedGroups\":" + config.groups());
    }
    RoutingTable table() { return table.get(); }
    private static Bytes key(Command c) { return c instanceof Command.Get g ? g.key() : c instanceof Command.Set s ? s.key() : ((Command.Delete) c).key(); }
    private Result local(ResultStatus s, Tablet t) { return Result.of(s, table.get().version(), t == null ? OwnershipEpoch.ZERO : t.epoch()); }
    private <T> CompletableFuture<T> control(Callable<T> action) {
        CompletableFuture<T> future = new CompletableFuture<>();
        try { controller.execute(() -> { try { future.complete(action.call()); } catch (Exception e) { future.completeExceptionally(e); } }); }
        catch (RejectedExecutionException e) { future.completeExceptionally(e); } return future;
    }
    Result execute(IntegrationWire.Request r, long version) throws InterruptedException {
        long began = System.nanoTime(), deadline = began + TimeUnit.MILLISECONDS.toNanos(r.timeoutMs() == 0 ? config.number("request.timeout.ms", 3000) : r.timeoutMs());
        requests.increment(); Command command = r.command(); Bytes key = key(command); boolean write = !(command instanceof Command.Get);
        if (closed.get() || r.client() > 1_000_000) return local(ResultStatus.BAD_REQUEST, null);
        if (!routingInitialized.get() || routingUncertain.get()) return local(ResultStatus.UNAVAILABLE, null);
        Heat h = heat.get().computeIfAbsent(key, ignored -> new Heat()); if (write) h.writes.increment(); else h.reads.increment();
        WriteLedger.Admission admission = write ? ledger.admit(r.client(), r.operation(), command) : null;
        if (admission != null && admission.rejection() != null) return admission.rejection();
        if (admission != null && !admission.fresh()) {
            try { return admission.entry().result().get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
            catch (ExecutionException | TimeoutException e) { return local(ResultStatus.TIMEOUT, null); }
        }
        Result result = local(ResultStatus.TIMEOUT, null); AtomicBoolean attempted = new AtomicBoolean();
        ReentrantLock keyLock = keyLocks[(key.hashCode() & Integer.MAX_VALUE) % keyLocks.length];
        try {
            if (!keyLock.tryLock(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) return result;
            try {
                while (System.nanoTime() < deadline) {
                    Move pulling = null;
                    metadata.readLock().lockInterruptibly();
                    try {
                        Tablet t = table.get().lookup(scheme.keyHash(key)); Guard g = guards.get(t.id());
                        if (version > table.get().version().value() || r.epoch() != 0 && r.epoch() != t.epoch().value()) { result = local(ResultStatus.STALE_EPOCH, t); break; }
                        Move candidate = post.get(t.id());
                        if (candidate != null && candidate.published && t.owner().value() == candidate.targetIndex()) pulling = candidate;
                        else {
                            if (!g.lock.readLock().tryLock(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) break;
                            try {
                                if (g.frozenBy != 0) { result = local(ResultStatus.BUSY, t); break; }
                                result = nativeRequest(t, r, deadline, attempted); break;
                            } finally { g.lock.readLock().unlock(); }
                        }
                    } finally { metadata.readLock().unlock(); }
                    if (pulling != null) {
                        Move selected = pulling;
                        try { result = control(() -> selected.foreground(r, deadline, attempted)).get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                        catch (ExecutionException | TimeoutException e) { if (write && attempted.get()) groups.get(selected.targetIndex()).uncertain.add(key); result = local(ResultStatus.TIMEOUT, null); }
                        break;
                    }
                }
            } finally { keyLock.unlock(); }
            return result;
        } finally {
            if (write) { admission.entry().result().complete(result); if (!attempted.get() || !config.ionia() && result.status() == ResultStatus.BUSY) ledger.releaseRejected(r.client(), r.operation(), admission.entry()); }
            long elapsed = System.nanoTime() - began; if (latencySize.incrementAndGet() <= 65536) latency.add(elapsed); else latencySize.decrementAndGet();
            if (result.status() == ResultStatus.OK || result.status() == ResultStatus.MISS) completed.increment(); else failures.increment();
        }
    }
    private Result nativeRequest(Tablet t, IntegrationWire.Request r, long deadline, AtomicBoolean attempted) throws InterruptedException {
        // A request may have waited for the metadata lock since its initial admission check.
        if (routingUncertain.get()) return local(ResultStatus.UNAVAILABLE, t);
        Command command = r.command(); Bytes key = key(command); int owner = (int) t.owner().value(); Group group = groups.get(owner); boolean write = !(command instanceof Command.Get);
        if (command instanceof Command.Delete && !backends.get(owner).supportsDelete()) return local(ResultStatus.NOT_IMPLEMENTED, t);
        if (write) { group.keys.add(key); notifyDirty(owner, key); }
        attempted.set(true); long began = System.nanoTime(); Result result;
        try {
            Result value = backends.get(owner).execute(command, r.client(), r.operation(), deadline);
            result = new Result(value.status(), value.value(), value.valueVersion(), table.get().version(), t.epoch(), Optional.empty());
        } catch (IOException e) { result = local(ResultStatus.UNAVAILABLE, t); }
        catch (InterruptedException e) { if (write) group.uncertain.add(key); throw e; }
        finally { backendNanos.add(System.nanoTime() - began); if (write) notifyDirty(owner, key); }
        if (write) {
            boolean success = result.status() == ResultStatus.OK || command instanceof Command.Delete && result.status() == ResultStatus.MISS;
            if (!success && ambiguous(result.status())) group.uncertain.add(key);
            if (success && command instanceof Command.Set s) group.sizes.put(key, s.value().size());
            if (success && command instanceof Command.Delete) group.sizes.remove(key);
        }
        return result;
    }
    private boolean ambiguous(ResultStatus s) { return s != ResultStatus.BAD_REQUEST && s != ResultStatus.NOT_IMPLEMENTED && (config.ionia() || s != ResultStatus.BUSY); }
    private void notifyDirty(int group, Bytes key) {
        for (Move m : inFlight.values()) if (m.sourceIndex() == group && m.tablet.range().contains(scheme.keyHash(key))) {
            m.dirtyObserved.add(key.size() + groups.get(group).sizes.getOrDefault(key, 0) + 16L);
            if (dirtySize.incrementAndGet() <= 65536) dirty.add(new Dirty(m, key)); else { dirtySize.decrementAndGet(); m.dirtyOverflow = true; }
        }
    }
    private void drainDirty() { Dirty d; while ((d = dirty.poll()) != null) { dirtySize.decrementAndGet(); if (d.move.hook != null) d.move.hook.accept(d.key); } }
    boolean ready() throws IOException, InterruptedException {
        if (routingUncertain.get()) return false;
        for (ReplicatedBackend b : backends) { Result r = b.administrative(new Command.Get(Bytes.utf8("__integration_preflight__"))); if (r.status() != ResultStatus.OK && r.status() != ResultStatus.MISS) return false; }
        if (!routingInitialized.get()) {
            metadata.writeLock().lockInterruptibly();
            try { if (!routingInitialized.get()) { backends.getFirst().publishRouting(null, table.get()); routingInitialized.set(true); } }
            finally { metadata.writeLock().unlock(); }
        }
        return true;
    }
    void resetMetrics() {
        control(() -> { requests.reset(); failures.reset(); backendNanos.reset(); completed.reset(); completedAt = 0; heat.set(new ConcurrentHashMap<>()); latency.clear(); latencySize.set(0); observationAt = System.nanoTime(); autoEnabled = true;
            journal.event("measurement-reset", ""); return null; }).join();
    }
    void stopControl() { control(() -> { autoEnabled = false; journal.event("automatic-control-stopped", ""); return null; }).join(); }
    CompletableFuture<Integer> balance() { return control(() -> decide(null)); }
    CompletableFuture<Integer> scaleTo(int count) { return control(() -> decide(count)); }
    long move(long tabletId, int destination) {
        if (!scheme.supportsMove() || destination < 0 || destination >= config.groups()) throw new IllegalArgumentException("unsupported move destination/distribution");
        return control(() -> {
            if (batch != null || requestedCount >= 0) throw new IllegalArgumentException("another plan is running");
            Tablet t = table.get().get(new TabletId(tabletId)); if (t.owner().value() == destination || !active.get().contains(destination)) throw new IllegalArgumentException("move target must be another active group");
            return admit(List.of(new Change(t.id(), t.owner(), new NodeId(destination))), active.get());
        }).join();
    }
    CompletableFuture<Boolean> resume(long id) { return control(() -> {
        if (batch != null || pendingRepairs.get() != 0) throw new IllegalArgumentException("another plan/repair is running");
        Move m = post.values().stream().filter(p -> p.id == id).findFirst().orElseThrow(() -> new IllegalArgumentException("retained post-copy job required"));
        if (!m.broken || !m.published || uncertain(groups.get(m.sourceIndex()), m.tablet.range()) || uncertain(groups.get(m.targetIndex()), m.tablet.range())) throw new IllegalArgumentException("uncertain data cannot be resumed without audit");
        Result ready = backends.get(m.sourceIndex()).administrative(new Command.Get(Bytes.utf8("__integration_resume__")));
        if (ready.status() != ResultStatus.OK && ready.status() != ResultStatus.MISS) return false;
        m.sourceLost = false; m.resumedAt = System.nanoTime(); m.session.postProtocol().resume(); m.broken = false; m.recovering = true; m.reason = "";
        batch = new Batch(List.of(m), List.of(), active.get()); pendingJobs.set(1); controlState = "TRANSFER"; controlReason = "post-copy-resume";
        journal.event("post-copy-resumed", "\"id\":" + id); return true;
    }); }
    long confirmedCrash(int group, int replica) {
        if (group < 0 || group >= config.groups() || replica < 0 || replica > 2) throw new IllegalArgumentException("invalid crash confirmation");
        long id = jobId.incrementAndGet(); pendingRepairs.incrementAndGet(); repairs.put(id, "{\"id\":" + id + ",\"state\":\"RUNNING\"}");
        repairWorkers.execute(() -> { String state = "DONE", reason = "";
            try { backends.get(group).confirmedCrash(replica); } catch (Exception e) { state = "FAILED"; reason = e.toString(); }
            repairs.put(id, "{\"id\":" + id + ",\"state\":" + JsonLine.quote(state) + ",\"reason\":" + JsonLine.quote(reason) + "}");
            journal.event("confirmed-crash-repair", "\"group\":" + group + ",\"replica\":" + replica + ",\"state\":" + JsonLine.quote(state)); pendingRepairs.decrementAndGet();
        }); return id;
    }
    String status() {
        String migration = jobs.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(java.util.stream.Collectors.joining(",", "[", "]"));
        String repair = repairs.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(java.util.stream.Collectors.joining(",", "[", "]"));
        String owners = table.get().tablets().stream().map(t -> "{\"id\":" + t.id().value() + ",\"owner\":" + t.owner().value() + ",\"epoch\":" + t.epoch().value() + "}").collect(java.util.stream.Collectors.joining(",", "[", "]"));
        return "{\"runId\":" + JsonLine.quote(config.runId()) + ",\"protocol\":" + JsonLine.quote(config.protocol()) + ",\"routingVersion\":" + table.get().version().value() + ",\"tablets\":" + table.get().size()
                + ",\"jointCache\":" + config.composed() + ",\"routingInitialized\":" + routingInitialized.get() + ",\"routingUncertain\":" + routingUncertain.get()
                + ",\"components\":" + backends.stream().map(ReplicatedBackend::compositionStatus).collect(java.util.stream.Collectors.joining(",", "[", "]"))
                + ",\"owners\":" + owners + ",\"activeGroups\":" + active.get() + ",\"preparedGroups\":" + config.groups() + ",\"controlState\":" + JsonLine.quote(controlState) + ",\"controlReason\":" + JsonLine.quote(controlReason)
                + ",\"scalingDecisions\":" + scalingDecisions.sum() + ",\"completedMigrations\":" + completedJobs.sum() + ",\"recoveredMigrations\":" + recoveredJobs.sum() + ",\"unresolvedMigrations\":" + unresolvedJobs.size() + ",\"window\":" + window + ",\"plans\":" + plans + ",\"requestedCount\":" + requestedCount + ",\"automaticControl\":" + autoEnabled + ",\"scaler\":" + JsonLine.quote(config.text("scaler", "fixed"))
                + ",\"scheduler\":" + JsonLine.quote(config.text("scheduler", "fifo")) + ",\"copy\":" + JsonLine.quote(config.copyName()) + ",\"drain\":" + JsonLine.quote(drain.name()) + ",\"modelOrigin\":" + JsonLine.quote(modelOrigin)
                + ",\"requests\":" + requests.sum() + ",\"failures\":" + failures.sum() + ",\"backendNanos\":" + backendNanos.sum() + ",\"uncertainKeys\":" + groups.stream().mapToLong(g -> g.uncertain.size()).sum()
                + ",\"pendingJobs\":" + pendingJobs.get() + ",\"pendingRepairs\":" + pendingRepairs.get() + ",\"repairs\":" + repair + ",\"migrations\":" + migration + "}";
    }
    private Observation observe() {
        long now = System.nanoTime(); double seconds = Math.max(.000001, (now - observationAt) / 1e9); observationAt = now;
        List<Long> samples = new ArrayList<>(); Long l; while ((l = latency.poll()) != null) { samples.add(l); latencySize.decrementAndGet(); } samples.sort(Long::compare);
        double p99 = samples.isEmpty() ? 0 : samples.get((int) Math.ceil(samples.size() * .99) - 1) / 1e9;
        long done = completed.sum(), delta = done - completedAt; completedAt = done;
        return new Observation(heat.getAndSet(new ConcurrentHashMap<>()), seconds, p99, delta);
    }
    private int decide(Integer explicit) throws IOException, InterruptedException {
        Observation observed = observe(); window++;
        double total = observed.heat.values().stream().mapToLong(h -> h.reads.sum() + h.writes.sum()).sum() / observed.seconds;
        history.add(total); if (scaler != null) scaler.observe(total);
        journal.event("observation-window", "\"window\":" + window + ",\"seconds\":" + observed.seconds + ",\"attemptRate\":" + total + ",\"p99Seconds\":" + observed.p99 + ",\"completed\":" + observed.completed);
        if (batch != null || requestedCount >= 0 || pendingRepairs.get() != 0) { if (explicit != null) throw new IllegalArgumentException("control plan/repair in flight"); return 0; }
        if (controlState.equals("SAFE_HOLD")) return 0;
        if (plans >= config.number("control.max.plans", 1000)) { hold("control plan budget exhausted"); return 0; }
        int target = explicit == null ? active.get().size() : explicit;
        if (target < config.minGroups() || target > config.groups()) throw new IllegalArgumentException("scale target outside prepared inventory/minimum");
        if (explicit == null && !config.text("scaler", "fixed").equals("fixed")) {
            if (window - lastCompletedWindow <= config.number("control.cooldown.windows", 2)) return 0;
            ScalingSnapshot snapshot = scalingSnapshot(observed);
            if (scaler == null) {
                if (config.text("scaler", "fixed").equals("pstore") && history.size() <= 2 * snapshot.config().period() + 5 + snapshot.config().forecastHorizon()) { controlState = "TRAINING"; controlReason = "P-Store requires observed historical periods"; return 0; }
                if (model == null) model = performanceModel(snapshot.config());
                scaler = ScalingPolicies.create(config.text("scaler", "fixed"), model, history);
            }
            if ((scaler.name().equals("estore") || scaler.name().equals("aws-warmup")) && snapshot.cpu().size() != active.get().size()) { controlState = "OBSERVING"; controlReason = "waiting for actual replica CPU deltas"; return 0; }
            var decision = scaler.decideCount(snapshot); scalingDecisions.increment(); target = decision.nodes();
            journal.event("scaling-decision", "\"window\":" + window + ",\"method\":" + JsonLine.quote(scaler.name()) + ",\"target\":" + target + ",\"reason\":" + JsonLine.quote(decision.reason()) + ",\"detail\":" + JsonLine.quote(decision.detail().toString()));
        }
        lastObservation = observed;
        if (target != active.get().size()) { requestedCount = target; return membershipStep(); }
        split(observed);
        List<Change> changes = placement(observed, active.get()); plans++;
        if (!changes.isEmpty()) { admit(changes, active.get()); return changes.size(); }
        if (scheme instanceof ResizableSlotScheme resize && resize.decide(table.get()) == ResizableSlotScheme.Resize.HALVE) {
            var plan = resize.halving(table.get()); adoptPrepared(plan.prepared()); if (!plan.moves().isEmpty()) { admit(plan.moves(), active.get()); return plan.moves().size(); }
            adoptPrepared(resize.merged(table.get()));
        }
        controlState = "STABLE"; controlReason = "keep"; return 0;
    }
    private PerformanceModel performanceModel(ScalingConfig c) throws IOException {
        if (!config.values().containsKey("scaling.calibration.file")) { modelOrigin = "reference-queue-calibration"; return PerformanceModel.calibrate(c); }
        List<double[]> observations = new ArrayList<>(), rewards = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(config.text("scaling.calibration.file", "")))) {
            if (line.isBlank() || line.startsWith("#") || line.startsWith("nodes,")) continue;
            String[] f = line.split(",", -1); if (f.length != 5) throw new IllegalArgumentException("calibration columns: nodes,reads,writes,p99_seconds,completed_per_second");
            int n = Integer.parseInt(f[0]); double reads = Double.parseDouble(f[1]), writes = Double.parseDouble(f[2]), p99 = Double.parseDouble(f[3]), done = Double.parseDouble(f[4]);
            if (n < c.minNodes() || n > c.maxNodes() || !Double.isFinite(reads + writes + p99 + done) || Math.min(Math.min(reads, writes), Math.min(p99, done)) < 0) throw new IllegalArgumentException("invalid calibration row");
            observations.add(new double[]{reads / n, writes / n, p99, done / n}); rewards.add(new double[]{n, reads + writes, p99, done});
        }
        modelOrigin = "provided-measured-calibration"; return new PerformanceModel(c, observations, rewards);
    }
    private ScalingSnapshot scalingSnapshot(Observation o) {
        groups.forEach(g -> g.keys.forEach(k -> keyIndices.computeIfAbsent(k, ignored -> keyIndices.size())));
        o.heat.keySet().forEach(k -> keyIndices.computeIfAbsent(k, ignored -> keyIndices.size()));
        ScalingConfig c = config.scalingSettings(keyIndices.size()); double[] reads = new double[c.keys()], writes = new double[c.keys()];
        o.heat.forEach((k, h) -> { Integer i = keyIndices.get(k); if (i != null) { reads[i] = h.reads.sum() / o.seconds; writes[i] = h.writes.sum() / o.seconds; } });
        List<PartitionUnit> units = new ArrayList<>();
        for (Tablet t : table.get().tablets()) { List<Integer> keys = keyIndices.entrySet().stream().filter(e -> t.range().contains(scheme.keyHash(e.getKey()))).map(Map.Entry::getValue).sorted().toList(); if (!keys.isEmpty()) units.add(new PartitionUnit(keys, List.of((int) t.owner().value()))); }
        Map<Integer, Double> cpu = new TreeMap<>(); for (int n : active.get()) backends.get(n).cpu().ifPresent(value -> cpu.put(n, value));
        double total = Arrays.stream(reads).sum() + Arrays.stream(writes).sum();
        return new ScalingSnapshot(c, window, active.get(), units, reads, writes, cpu, o.p99, total, o.completed / o.seconds, new long[c.maxNodes()], lastCompletedWindow, lastAction);
    }
    private int membershipStep() throws IOException, InterruptedException {
        int before = active.get().size(), target = before + Integer.signum(requestedCount - before);
        if (target == before) { requestedCount = -1; pendingJobs.set(0); return 0; }
        List<Integer> desired = java.util.stream.IntStream.range(0, target).boxed().toList();
        for (int n : desired) if (!active.get().contains(n)) { Result r = backends.get(n).administrative(new Command.Get(Bytes.utf8("__integration_ready__"))); if (r.status() != ResultStatus.OK && r.status() != ResultStatus.MISS) { hold("new group quorum not ready"); return 0; } }
        List<Change> changes;
        if (config.text("placement", "NONE").equals("NONE") || !scheme.supportsMove()) {
            Plan plan = target > before ? scheme.onNodeAdded(table.get(), new NodeId(target - 1)) : table.get().nodes().contains(new NodeId(before - 1)) ? scheme.onNodeRemoved(table.get(), new NodeId(before - 1)) : Plan.movesOnly(table.get(), List.of());
            adoptPrepared(plan.prepared()); changes = plan.moves();
        } else { split(lastObservation); changes = placement(lastObservation, desired); }
        plans++; journal.event("membership-plan", "\"before\":" + before + ",\"target\":" + target + ",\"requested\":" + requestedCount + ",\"moves\":" + changes.size());
        admit(changes, desired); return changes.size();
    }
    private void split(Observation observed) {
        if (!scheme.supportsSplit() || config.text("split", "NONE").equals("NONE")) return;
        Tablet hot = table.get().tablets().stream().max(Comparator.comparingLong(t -> load(t, observed).totalRequests())).orElseThrow();
        if (config.splitter().decide(load(hot, observed)) instanceof SplitDecision.Split split) { adoptPrepared(table.get().withSplit(hot.id(), split.at())); journal.event("split", "\"tablet\":" + hot.id().value() + ",\"routingVersion\":" + table.get().version().value()); }
    }
    private TabletLoad load(Tablet t, Observation o) {
        TreeMap<Long, long[]> hashes = new TreeMap<>(Long::compareUnsigned);
        o.heat.forEach((k, h) -> { long hash = scheme.keyHash(k); if (t.range().contains(hash)) { long[] c = hashes.computeIfAbsent(hash, ignored -> new long[2]); c[0] += h.reads.sum(); c[1] += h.writes.sum(); } });
        return new TabletLoad(t.range(), window, hashes.entrySet().stream().map(e -> new KeyLoad(e.getKey(), e.getValue()[0], e.getValue()[1])).toList());
    }
    private long bytes(Tablet t) { Group g = groups.get((int) t.owner().value()); return g.sizes.entrySet().stream().filter(e -> t.range().contains(scheme.keyHash(e.getKey()))).mapToLong(e -> 8L + e.getKey().size() + e.getValue()).sum(); }
    private List<Change> placement(Observation o, List<Integer> desired) {
        if (config.text("placement", "NONE").equals("NONE") || !scheme.supportsMove()) return List.of();
        if (config.text("placement", "NONE").equals("FIXED_LPT")) return fixedTabletPlacement(o, desired);
        Map<TabletId, Long> loads = new LinkedHashMap<>(), sizes = new LinkedHashMap<>(); Map<NodeId, Double> capacity = new TreeMap<>(Comparator.comparingLong(NodeId::value));
        desired.forEach(n -> capacity.put(new NodeId(n), 1.0)); for (Tablet t : table.get().tablets()) { loads.put(t.id(), load(t, o).totalRequests()); sizes.put(t.id(), bytes(t)); }
        var decision = config.placer().decide(new PlacementInput(table.get(), loads, sizes, capacity, window, Map.copyOf(lastMoved)));
        journal.event("balance-plan", "\"window\":" + window + ",\"method\":" + JsonLine.quote(config.text("placement", "NONE")) + ",\"moves\":" + decision.moves().size() + ",\"tabletLoads\":" + JsonLine.quote(loads.toString())); return decision.moves();
    }
    /** Reuse the original B3 fixed-unit LPT decision, then translate its existing key sets to B1 tablets. */
    private List<Change> fixedTabletPlacement(Observation observation, List<Integer> desired) {
        ScalingSnapshot snapshot = scalingSnapshot(observation);
        var owners = new HashMap<List<Integer>, Integer>();
        for (PartitionUnit unit : FixedTabletPlacement.units(snapshot, desired.size())) owners.put(unit.keys(), unit.holders().getFirst());
        var changes = new ArrayList<Change>();
        for (Tablet tablet : table.get().tablets()) {
            List<Integer> keys = keyIndices.entrySet().stream().filter(e -> tablet.range().contains(scheme.keyHash(e.getKey()))).map(Map.Entry::getValue).sorted().toList();
            // Empty metadata units carry no forecast load or memory. Do not invent synthetic keys for the model.
            int target = keys.isEmpty() ? desired.contains((int) tablet.owner().value()) ? (int) tablet.owner().value() : desired.get(Math.floorMod(tablet.id().value(), desired.size()))
                    : Objects.requireNonNull(owners.get(keys), "fixed planner omitted an observed tablet");
            if (target != tablet.owner().value()) changes.add(new Change(tablet.id(), tablet.owner(), new NodeId(target)));
        }
        journal.event("balance-plan", "\"window\":" + window + ",\"method\":\"FIXED_LPT\",\"sourcePolicy\":" + JsonLine.quote(FixedTabletPlacement.VERSION) + ",\"moves\":" + changes.size());
        return changes;
    }
    private void adoptPrepared(RoutingTable after) {
        metadata.writeLock().lock(); try { publishAuthority(table.get(), after); table.set(after); after.tablets().forEach(t -> guards.computeIfAbsent(t.id(), ignored -> new Guard())); guards.keySet().removeIf(id -> !after.contains(id)); } finally { metadata.writeLock().unlock(); }
    }
    private void publishAuthority(RoutingTable before, RoutingTable after) {
        if (routingUncertain.get() || !routingInitialized.get()) throw new IllegalStateException("routing authority is not ready");
        try { backends.getFirst().publishRouting(before, after); }
        catch (IOException | InterruptedException failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            routingUncertain.set(true); hold("A1-routing-unconfirmed:" + failure);
            throw new IllegalStateException("A1 publication failed; all cache requests held", failure);
        }
    }
    private void hold(String reason) { controlState = "SAFE_HOLD"; controlReason = reason; requestedCount = -1; autoEnabled = false; if (batch == null) pendingJobs.set(0); journal.event("control-held", "\"reason\":" + JsonLine.quote(reason)); }
    private boolean uncertain(Group g, TabletRange range) { return g.uncertain.stream().anyMatch(k -> range.contains(scheme.keyHash(k))); }
    private boolean blocked(Change c) {
        Tablet t = table.get().get(c.unit());
        return guards.get(c.unit()).frozenBy != 0 || uncertain(groups.get((int) c.from().value()), t.range()) || uncertain(groups.get((int) c.to().value()), t.range())
                || groups.get((int) c.to().value()).quarantined.stream().anyMatch(r -> r.contains(t.range().start()) || t.range().contains(r.start()));
    }
    private long admit(List<Change> changes, List<Integer> desired) {
        if (changes.size() > 60) throw new IllegalArgumentException("native scheduler batch supports at most 60 tablets");
        if (changes.isEmpty()) { finishMembership(desired); return 0; }
        Map<TabletId, Long> sizes = new HashMap<>(); Map<NodeId, Long> free = new HashMap<>(); Set<TabletId> blocked = new HashSet<>();
        long capacity = config.scalingSettings(4).nodeBytes(); for (int n = 0; n < config.groups(); n++) { int group = n; long used = table.get().ownedBy(new NodeId(group)).stream().mapToLong(this::bytes).sum(); free.put(new NodeId(group), Math.max(0, capacity - used)); }
        for (Change c : changes) { sizes.put(c.unit(), bytes(table.get().get(c.unit()))); if (blocked(c) || controlState.equals("SAFE_HOLD")) blocked.add(c.unit()); }
        var decision = drain.admit(new DrainStrategy.Snapshot(changes, sizes, free, Set.of(), blocked, true, desired.size(), config.minGroups(), false));
        if (decision.admitted().isEmpty()) {
            long first = 0; for (Change c : changes) { long id = jobId.incrementAndGet(); if (first == 0) first = id; unresolvedJobs.add(id); jobs.put(id, "{\"id\":" + id + ",\"tablet\":" + c.unit().value() + ",\"state\":\"HELD\",\"published\":false,\"reason\":" + JsonLine.quote(decision.reason()) + "}"); }
            hold(decision.reason()); return first;
        }
        var union = new TreeSet<>(active.get()); union.addAll(desired); active.set(List.copyOf(union));
        List<Move> moving = decision.admitted().stream().map(c -> new Move(jobId.incrementAndGet(), c)).toList();
        batch = new Batch(moving, changes.stream().filter(c -> !decision.admitted().contains(c)).toList(), desired);
        pendingJobs.set(moving.size() + (requestedCount >= 0 ? 1 : 0)); controlState = "TRANSFER"; controlReason = decision.reason();
        moving.forEach(m -> { inFlight.put(m.tablet.id(), m); m.observe("QUEUED"); }); return moving.getFirst().id;
    }
    private void finishMembership(List<Integer> desired) {
        for (int n : active.get()) if (!desired.contains(n)) {
            int owned = table.get().ownedBy(new NodeId(n)).size(); int unfinished = (int) inFlight.values().stream().filter(m -> m.sourceIndex() == n || m.targetIndex() == n).count();
            boolean clear = post.values().stream().noneMatch(m -> m.sourceIndex() == n || m.targetIndex() == n);
            if (!drain.canRemove(owned, unfinished, clear)) { hold("drain guard: remaining data/jobs/metadata"); return; }
            journal.event("group-drained-removed", "\"group\":" + n + ",\"remainingTablets\":0,\"unfinishedJobs\":0,\"metadataCleared\":true");
        }
        active.set(List.copyOf(desired)); lastCompletedWindow = window; lastAction = desired.size() < lastStableCount ? "in" : desired.size() > lastStableCount ? "out" : "rebalance"; lastStableCount = desired.size();
        if (requestedCount >= 0 && desired.size() != requestedCount) { try { membershipStep(); } catch (IOException | InterruptedException e) { if (e instanceof InterruptedException) Thread.currentThread().interrupt(); hold(e.toString()); } return; }
        requestedCount = -1;
        if (!unresolvedJobs.isEmpty()) { hold("retained failed/held migrations require explicit recovery"); return; }
        controlState = "STABLE"; controlReason = "plan-complete";
        if (jobs.size() > 512) jobs.entrySet().stream().filter(e -> e.getValue().contains("\"state\":\"DONE\"")).sorted(Map.Entry.comparingByKey()).limit(jobs.size() - 512L).map(Map.Entry::getKey).toList().forEach(jobs::remove);
        if (scheme instanceof ResizableSlotScheme resize && resize.decide(table.get()) == ResizableSlotScheme.Resize.HALVE) {
            Plan half = resize.halving(table.get()); if (half.moves().isEmpty()) adoptPrepared(resize.merged(table.get()));
        }
        pendingJobs.set(0);
    }
    private final class Batch {
        final List<Move> moving; final List<Change> remaining; final List<Integer> desired; final MigrationScheduler scheduler;
        Batch(List<Move> moving, List<Change> remaining, List<Integer> desired) {
            this.moving = moving; this.remaining = remaining; this.desired = List.copyOf(desired);
            var tasks = java.util.stream.IntStream.range(0, moving.size()).mapToObj(i -> new SchedulerAlgorithms.Task(i, moving.get(i).change, bytes(moving.get(i).tablet), moving.get(i).dirtyBytes(), config.schedulerSettings().maxJobTicks())).toList();
            var policy = new SchedulerAlgorithms(config.text("scheduler", "fifo"), tasks, config.schedulerSettings(), i -> Math.max(0, tasks.get(i).bytes() - moving.get(i).sentBytes()), i -> moving.get(i).dirtyBytes());
            Map<Change, Move> byChange = new HashMap<>(); moving.forEach(m -> byChange.put(m.change, m));
            scheduler = new MigrationScheduler(moving.stream().map(m -> m.change).toList(), config.schedulerSettings(), byChange::get, policy);
        }
    }
    private void tickSafely() {
        if (closed.get()) return;
        try {
            drainDirty(); if (batch == null) return; Batch current = batch;
            var tick = current.scheduler.step();
            journal.event("scheduler-tick", "\"tick\":" + tick.tick() + ",\"allocated\":" + JsonLine.quote(tick.allocated().toString()) + ",\"protocolConsumed\":" + JsonLine.quote(tick.consumed().toString()));
            pendingJobs.set(Math.max(1, (int) tick.jobs().stream().filter(j -> j.status() == MigrationScheduler.Status.RUNNING || j.status() == MigrationScheduler.Status.QUEUED).count() + (requestedCount >= 0 ? 1 : 0)));
            if (!current.scheduler.finished()) return;
            batch = null;
            if (current.scheduler.jobs().stream().anyMatch(j -> j.status() != MigrationScheduler.Status.DONE)) { hold("migration failed; keep source and membership"); return; }
            if (!current.remaining.isEmpty()) { admit(current.remaining, current.desired); return; }
            finishMembership(current.desired);
        } catch (Exception failure) {
            if (batch != null) { batch.moving.forEach(m -> { if (!m.done()) m.fail(failure); }); batch = null; }
            hold("scheduler-error:" + failure);
        }
    }
    private final class Move implements MigrationScheduler.Driver, CopyDrivers.Binding, PreCopyProtocol.Source, PreCopyProtocol.Destination, PostCopyProtocol.Source {
        final long id, began = System.nanoTime(); final Change change; final Tablet tablet; final Guard guard;
        final PreCopyReceiver receiver = new PreCopyReceiver(scheme::keyHash); final MigrationJob receiverSession;
        final Map<Bytes, Bytes> postInstalled = new HashMap<>(); final Set<Bytes> postDeleted = new HashSet<>();
        final LongAdder dirtyObserved = new LongAdder();
        CopyDrivers.Session session; Consumer<Bytes> hook; volatile boolean dirtyOverflow; boolean published, touched, broken, sourceLost, recovering; long resumedAt = began;
        String reason = ""; long freezeAt = -1, frozenNanos, validationNanos, networkInstalled;
        Move(long id, Change change) { this.id = id; this.change = change; tablet = table.get().get(change.unit()); guard = guards.get(tablet.id()); receiverSession = MigrationJob.pending(change, "pre-copy"); }
        int sourceIndex() { return (int) change.from().value(); } int targetIndex() { return (int) change.to().value(); }
        List<Bytes> keys(Group g) { return g.keys.stream().filter(k -> tablet.range().contains(scheme.keyHash(k))).sorted((a, b) -> Arrays.compareUnsigned(a.toByteArray(), b.toByteArray())).toList(); }
        double dirtyBytes() { return dirtyObserved.sum() / 2.0 * TimeUnit.MILLISECONDS.toNanos(config.number("migration.tick.ms", 10)) / Math.max(1, System.nanoTime() - began); }
        private void check() { if (closed.get() || dirtyOverflow || System.nanoTime() - resumedAt > TimeUnit.MILLISECONDS.toNanos(config.number("migration.timeout.ms", 300000))) throw new IllegalStateException("migration deadline/dirty capacity/lifecycle"); }
        @Override public void begin() {
            if (recovering) { recovering = false; observe(session.state()); return; }
            try {
                check(); if (blocked(change)) throw new IllegalStateException("HELD: uncertain/frozen/quarantined range");
                List<Bytes> old = keys(groups.get(targetIndex()));
                if (!backends.get(targetIndex()).supportsDelete()) {
                    if (old.stream().anyMatch(k -> !groups.get(sourceIndex()).sizes.containsKey(k))) throw new IllegalStateException("HELD: PB cannot clear keys absent from incoming manifest");
                } else for (Bytes key : old) { touched = true; Result r = backends.get(targetIndex()).administrative(new Command.Delete(key)); if (r.status() != ResultStatus.OK && r.status() != ResultStatus.MISS) throw new IOException("destination clear failed: " + r.status()); groups.get(targetIndex()).sizes.remove(key); }
                session = CopyDrivers.create(config.copyName(), change, this, config.copySettings()); session.begin(); observe(session.state());
                journal.event("migration-begin", "\"id\":" + id + ",\"tablet\":" + tablet.id().value() + ",\"copy\":" + JsonLine.quote(config.copyName()));
            } catch (Exception e) { fail(e); }
        }
        @Override public void step(long budget) { if (broken) return; try { check(); drainDirty(); session.step(budget); if (config.copyName().equals("post-copy") && published) syncPost(); if (session.failed()) throw new IllegalStateException("protocol failed");
            if (session.done()) { completedJobs.increment(); if (unresolvedJobs.remove(id)) recoveredJobs.increment(); inFlight.remove(tablet.id(), this); post.remove(tablet.id(), this); lastMoved.put(tablet.id(), (long) window); hook = null; journal.event("migration-done", "\"id\":" + id + ",\"published\":true"); }
            observe(session.state());
        } catch (Exception e) { fail(e); } }
        void fail(Exception failure) {
            if (broken) return;
            unresolvedJobs.add(id); broken = true; reason = failure.getMessage() == null ? failure.toString() : failure.getMessage();
            if (session != null) session.stop();
            if (touched && !published) groups.get(targetIndex()).quarantined.add(tablet.range());
            if (!published) { unfreeze(tablet); inFlight.remove(tablet.id(), this); post.remove(tablet.id(), this); }
            observe(published ? "FAILED_AFTER_PUBLICATION" : reason.startsWith("HELD") ? "HELD" : "ABORTED");
            journal.event("migration-failed", "\"id\":" + id + ",\"published\":" + published + ",\"reason\":" + JsonLine.quote(reason));
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
        }
        @Override public long sentBytes() { return session == null ? 0 : session.sentBytes(); }
        @Override public boolean done() { return !broken && session != null && session.done(); }
        @Override public boolean failed() { return broken || session != null && session.failed(); }
        @Override public String state() { return broken ? published ? "FAILED_AFTER_PUBLICATION" : "ABORTED" : session == null ? "PENDING" : session.state(); }
        @Override public void stop() { fail(new IllegalStateException("scheduler timeout/cancel")); }
        void observe(String state) {
            long freeze = frozenNanos + (freezeAt < 0 ? 0 : System.nanoTime() - freezeAt);
            jobs.put(id, "{\"id\":" + id + ",\"tablet\":" + tablet.id().value() + ",\"from\":" + sourceIndex() + ",\"to\":" + targetIndex() + ",\"state\":" + JsonLine.quote(state) + ",\"published\":" + published
                    + ",\"copy\":" + JsonLine.quote(config.copyName()) + ",\"elapsedNanos\":" + (System.nanoTime() - began) + ",\"freezeNanos\":" + freeze + ",\"validationNanos\":" + validationNanos + ",\"protocolModelBytes\":" + sentBytes() + ",\"installedRecords\":" + networkInstalled + ",\"reason\":" + JsonLine.quote(reason) + "}");
        }
        @Override public long keyHash(Bytes key) { return scheme.keyHash(key); }
        @Override public RoutingTable current() { return table.get(); }
        @Override public PreCopyProtocol.Source source() { return this; }
        @Override public StopAndCopyProtocol.Destination stopDestination() { return this; }
        @Override public PreCopyProtocol.Destination preDestination() { return this; }
        @Override public PostCopyProtocol.Source postSource() { return this; }
        @Override public void beforePublish(Change c, String copy, RoutingTable before, RoutingTable after) {
            if (!frozen(tablet) || uncertain(groups.get(sourceIndex()), tablet.range())) throw new IllegalStateException("source freeze/uncertainty barrier failed");
            if (!copy.equals("post-copy")) { for (var record : receiver.data().entrySet()) install(record.getKey(), record.getValue()); validateDestination(receiver.data()); }
        }
        @Override public boolean compareAndSet(RoutingTable before, RoutingTable after) {
            metadata.writeLock().lock(); try {
                if (table.get() != before) return false;
                publishAuthority(before, after);
                if (config.copyName().equals("post-copy")) post.put(tablet.id(), this);
                if (!table.compareAndSet(before, after)) { post.remove(tablet.id(), this); return false; }
                published = true; journal.event("routing-published", "\"id\":" + id + ",\"routingVersion\":" + after.version().value()); return true;
            } finally { metadata.writeLock().unlock(); }
        }
        @Override public void installed(Change c, String copy, RoutingTable before, RoutingTable after) { /* Pre/Stop은 beforePublish에서 적용, Post는 각 pull/foreground에 실제 적용한다. */ }
        private void install(Bytes key, Bytes value) {
            long beganValidation = System.nanoTime(); touched = true;
            try {
                Result r = backends.get(targetIndex()).administrative(new Command.Set(key, value)); if (r.status() != ResultStatus.OK) throw new IOException("bootstrap write failed: " + r.status());
                groups.get(targetIndex()).keys.add(key); groups.get(targetIndex()).sizes.put(key, value.size());
                Result read = backends.get(targetIndex()).administrative(new Command.Get(key)); if (read.status() != ResultStatus.OK || !read.value().equals(Optional.of(value))) throw new IOException("destination apply/read barrier failed: " + read.status()); networkInstalled++;
            } catch (IOException e) { throw new IllegalStateException(e); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            finally { validationNanos += System.nanoTime() - beganValidation; }
        }
        private void syncPost() { for (var e : session.postData().entrySet()) if (!e.getValue().equals(postInstalled.get(e.getKey()))) { install(e.getKey(), e.getValue()); postInstalled.put(e.getKey(), e.getValue()); } }
        private void validateDestination(Map<Bytes, Bytes> data) {
            try {
                for (var e : data.entrySet()) { Result r = backends.get(targetIndex()).administrative(new Command.Get(e.getKey())); if (r.status() != ResultStatus.OK || !r.value().equals(Optional.of(e.getValue()))) throw new IOException("final apply barrier failed"); }
                for (Bytes key : postDeleted) if (backends.get(targetIndex()).administrative(new Command.Get(key)).status() != ResultStatus.MISS) throw new IOException("final tombstone barrier failed");
            } catch (IOException e) { throw new IllegalStateException(e); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        }
        Result foreground(IntegrationWire.Request r, long deadline, AtomicBoolean attempted) throws InterruptedException {
            if (post.get(tablet.id()) != this || table.get().get(tablet.id()).owner().value() != targetIndex()) return local(ResultStatus.BUSY, null);
            if (System.nanoTime() >= deadline) return local(ResultStatus.TIMEOUT, null);
            Bytes key = key(r.command()); if (groups.get(targetIndex()).uncertain.contains(key)) return local(ResultStatus.UNAVAILABLE, table.get().get(tablet.id()));
            if (r.command() instanceof Command.Get) { Result logical = session.postRequest(r.command()); if (logical.status() == ResultStatus.BUSY || logical.status() == ResultStatus.UNAVAILABLE || logical.status() == ResultStatus.MISS) return logical;
                try { syncPost(); } catch (RuntimeException e) { fail(e); return local(ResultStatus.UNAVAILABLE, null); }
                return nativeRequest(table.get().get(tablet.id()), r, deadline, attempted);
            }
            Result nativeResult = nativeRequest(table.get().get(tablet.id()), r, deadline, attempted);
            if (nativeResult.status() == ResultStatus.OK || r.command() instanceof Command.Delete && nativeResult.status() == ResultStatus.MISS) {
                Result logical = session.postRequest(r.command()); if (r.command() instanceof Command.Set set) { postInstalled.put(key, set.value()); postDeleted.remove(key); } else { postDeleted.add(key); postInstalled.remove(key); }
                nativeResult = new Result(logical.status(), nativeResult.value(), nativeResult.valueVersion(), nativeResult.routingVersion(), nativeResult.ownershipEpoch(), Optional.empty());
            } else if (attempted.get() && ambiguous(nativeResult.status())) { session.postRequest(r.command()); fail(new IOException("ambiguous post-copy destination write")); }
            return nativeResult;
        }
        @Override public boolean freeze(Tablet t) { guard.lock.writeLock().lock(); try { drainDirty(); if (guard.frozenBy != 0 || uncertain(groups.get(sourceIndex()), tablet.range())) return false; guard.frozenBy = id; freezeAt = System.nanoTime(); journal.event("freeze", "\"id\":" + id); return true; } finally { guard.lock.writeLock().unlock(); } }
        @Override public boolean frozen(Tablet t) { return guard.frozenBy == id; }
        @Override public void unfreeze(Tablet t) { guard.lock.writeLock().lock(); try { if (guard.frozenBy == id) { guard.frozenBy = 0; if (freezeAt >= 0) frozenNanos += System.nanoTime() - freezeAt; freezeAt = -1; } } finally { guard.lock.writeLock().unlock(); } }
        @Override public boolean cleanup(Tablet t) {
            if (config.copyName().equals("post-copy")) { syncPost(); validateDestination(session.postData()); }
            if (backends.get(sourceIndex()).supportsDelete()) for (Bytes key : keys(groups.get(sourceIndex()))) {
                try { Result r = backends.get(sourceIndex()).administrative(new Command.Delete(key)); if (r.status() != ResultStatus.OK && r.status() != ResultStatus.MISS) return false; groups.get(sourceIndex()).sizes.remove(key); }
                catch (IOException e) { return false; } catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
            }
            return true;
        }
        @Override public boolean startTracking(Tablet t, Consumer<Bytes> callback) { guard.lock.writeLock().lock(); try { drainDirty(); if (guard.frozenBy != 0 || uncertain(groups.get(sourceIndex()), tablet.range())) return false; hook = callback; return true; } finally { guard.lock.writeLock().unlock(); } }
        @Override public void stopTracking(Tablet t) { hook = null; }
        @Override public List<TransferBatch.Record> snapshot(Tablet t) {
            List<Bytes> keys = keys(groups.get(sourceIndex())); if (keys.size() > config.number("migration.max.records", 200000)) throw new IllegalStateException("snapshot record capacity");
            List<TransferBatch.Record> records = new ArrayList<>(); for (Bytes key : keys) read(t, key).ifPresent(v -> records.add(new TransferBatch.Record(key, v))); return records;
        }
        @Override public Optional<Bytes> read(Tablet t, Bytes key) { check(); try { Result r = backends.get(sourceIndex()).administrative(new Command.Get(key)); if (r.status() != ResultStatus.OK && r.status() != ResultStatus.MISS) throw new IOException("source read failed: " + r.status()); return r.value(); }
            catch (IOException e) { sourceLost = true; throw new IllegalStateException(e); } catch (InterruptedException e) { Thread.currentThread().interrupt(); sourceLost = true; throw new IllegalStateException(e); } }
        @Override public List<Bytes> keys(Tablet t) { List<Bytes> keys = keys(groups.get(sourceIndex())).stream().filter(groups.get(sourceIndex()).sizes::containsKey).toList(); if (keys.size() > config.number("migration.max.records", 200000)) throw new IllegalStateException("manifest capacity"); return keys; }
        @Override public boolean available(Tablet t) { return !closed.get() && !sourceLost && !uncertain(groups.get(sourceIndex()), tablet.range()); }
        @Override public Optional<PullReply> pull(MigrationJob j, OwnershipEpoch epoch, long request, Bytes key) { return Optional.of(PullReply.create(j, epoch, request, key, read(tablet, key))); }
        @Override public boolean prepare(MigrationJob j, Tablet t) { return receiver.prepare(receiverSession, t); }
        @Override public void receive(MigrationJob j, OwnershipEpoch e, TransferBatch b) { receiver.receive(receiverSession, e, b); }
        @Override public Optional<StopAndCopyProtocol.Acknowledgement> acknowledgement(MigrationJob j, long id) { return receiver.acknowledgement(receiverSession, id); }
        @Override public boolean complete(MigrationJob j) { return receiver.complete(receiverSession); }
        @Override public void discard(MigrationJob j) { receiver.discard(receiverSession); }
        @Override public void receiveDelta(MigrationJob j, OwnershipEpoch e, DeltaBatch b) { receiver.receiveDelta(receiverSession, e, b); }
        @Override public Optional<DeltaBatch.Acknowledgement> deltaAcknowledgement(MigrationJob j, long id) { return receiver.deltaAcknowledgement(receiverSession, id); }
        @Override public boolean caughtUp(MigrationJob j, long seq) { return receiver.caughtUp(receiverSession, seq); }
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return; autoEnabled = false;
        controller.shutdown(); try { controller.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        controller.shutdownNow(); repairWorkers.shutdownNow(); backends.forEach(ReplicatedBackend::close);
    }
}
