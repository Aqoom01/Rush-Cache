package io.openkache.core.integration;

import io.openkache.core.api.*;
import io.openkache.core.balance.*;
import io.openkache.core.balance.placement.PlacementInput;
import io.openkache.core.balance.split.SplitDecision;
import io.openkache.core.failure.*;
import io.openkache.core.migration.*;
import io.openkache.core.migration.precopy.*;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.model.*;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.replication.raft.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import io.openkache.core.tablet.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * 단일 thread의 결정론적 통합 실험. B1 owner는 고정 3 replica 그룹의 논리 ID다.
 * 실제 PB/Raft, SWIM/Lifeguard, B1/B2, B3 코드를 실행한다. 지연과 byte는 모형 값이다.
 * TCP/SSD/CPU 성능 또는 제품 서버의 완성도를 측정하는 fixture가 아니다.
 */
public final class IntegratedCluster implements AutoCloseable {
    static final long MS = 1_000_000;
    static final int KEY_COUNT = 96;
    static final int DURATION_MS = 6000;
    static final int REQUEST_INTERVAL_MS = 3;
    static final int BALANCE_MS = 1000;
    static final int CRASH_MS = 1300;
    static final int EGRESS_BYTES_PER_MS = 1024;
    private record Event(long at, long order, Runnable task) {}
    private record Pending(Bytes key, Command command, CompletableFuture<Result> future,
                           long start, boolean measured) {}
    /** 지연은 성공 요청만의 가상 시간이다. failure/pending은 별도 집계한다. */
    public record Measurement(String variant, String scenario, long seed, long offered, long ok, long rejected,
                             long pending, double p99Ms, long lost, long staleReads, long falseDead,
                             long replicationBytes, long detectorBytes, long migrationBytes,
                             long freezeMs, int splits, int plannedMoves, int completedMoves,
                             long recoveryMs, long tapeHash, List<String> events) {
        public double goodput() { return ok * 1000.0 / DURATION_MS; }
        public boolean safe() { return lost == 0 && staleReads == 0 && falseDead == 0 && completedMoves == plannedMoves; }
    }

    final SystemVariant variant;
    final String scenario;
    final long seed;
    final long serviceNanos;
    long now, sequence, operation;
    final TimeSource time = new TimeSource() {
        @Override public long monotonicNanos() { return now; }
        @Override public long epochMillis() { return now / MS; }
    };
    final PriorityQueue<Event> events = new PriorityQueue<>(Comparator.comparingLong(Event::at).thenComparingLong(Event::order));
    final Map<NodeId, CacheEngine> engines = new LinkedHashMap<>();
    final Map<NodeId, Store> stores = new LinkedHashMap<>();
    final Map<NodeId, SwimFailureDetector> detectors = new LinkedHashMap<>();
    final Set<NodeId> dead = new HashSet<>();
    final Map<NodeId, Long> egress = new HashMap<>();
    final List<Pending> pending = new ArrayList<>();
    final Set<Bytes> busyKeys = new HashSet<>();
    final Map<Bytes, Bytes> oracle = new LinkedHashMap<>();
    final Map<Bytes, Map<Bytes, Long>> issued = new HashMap<>();
    final Map<Bytes, Long> acknowledged = new HashMap<>();
    long writeSequence;
    final Map<Bytes, long[]> heat = new LinkedHashMap<>();
    final List<Double> latencies = new ArrayList<>();
    final List<Change> moves = new ArrayList<>();
    final List<Bytes> keys = new ArrayList<>();
    final List<String> journal = new ArrayList<>();
    RoutingTable table;
    TabletRange beginBarrier;
    Move active;
    long start, offered, ok, rejected, staleReads, falseDead, replicationBytes, detectorBytes, migrationBytes, freezeTicks;
    long crashedAt = -1, recoveredAt = -1, tapeHash = 1;
    int splits, plannedMoves, completedMoves;
    boolean measured;

    public IntegratedCluster(SystemVariant variant, String scenario, long seed) {
        this(variant, scenario, seed, 4000);
    }
    public IntegratedCluster(SystemVariant variant, String scenario, long seed, int serviceMicros) {
        if (serviceMicros < 0 || serviceMicros > 10000) throw new IllegalArgumentException("serviceMicros must be 0..10000");
        serviceNanos = serviceMicros * 1000L;
        if (!List.of("uniform", "hotspot", "crash-overlap", "jitter").contains(scenario))
            throw new IllegalArgumentException("unknown scenario: " + scenario);
        this.variant = variant; this.scenario = scenario; this.seed = seed;
        table = variant.scheme().initial(List.of(new NodeId(1), new NodeId(2)));
        for (int k = 0; k < KEY_COUNT; k++) keys.add(Bytes.utf8("key-" + k));
        for (int g = 1; g <= 2; g++) {
            var members = members(g);
            for (NodeId id : members) {
                Store store = new Store(id); stores.put(id, store);
                Transport network = this::send;
                CacheEngine engine;
                if (variant.replication().equals("PB")) {
                    engine = new CrashOnlyPrimaryBackupEngine(new PrimaryBackupConfig(id, members.getFirst(),
                            members.subList(1, 3), new OwnershipEpoch(1)), store, new DirectMailbox(), network,
                            time, Duration.ofMillis(20), Duration.ofMillis(120), false);
                } else {
                    engine = new RaftEngine(id, members, store, new DirectMailbox(), network, time,
                            Duration.ofMillis(20), Duration.ofMillis(120), seed * 1009 + id.value(), true);
                }
                engines.put(id, engine);
                boolean lha = variant.detector().equals("LIFEGUARD");
                var cfg = new SwimConfig(Duration.ofMillis(50), Duration.ofMillis(12), Duration.ofMillis(12),
                        Duration.ofMillis(90), 1, 3, 6, lha, lha ? 4 : 0, lha ? 3 : 1, lha ? 1 : 0);
                detectors.put(id, new SwimFailureDetector(id, members, cfg, network, time,
                        event -> membership(id, event), seed * 7919 + id.value()));
            }
        }
        detectors.values().forEach(SwimFailureDetector::start);
    }

    static List<NodeId> members(long group) {
        long base = (group - 1) * 3;
        return List.of(new NodeId(base), new NodeId(base + 1), new NodeId(base + 2));
    }
    NodeId leader(long group) {
        for (NodeId id : members(group)) {
            if (dead.contains(id)) continue;
            CacheEngine e = engines.get(id);
            if (e instanceof RaftEngine raft && raft.observation().role() == RaftEngine.Role.LEADER) return id;
            if (e instanceof CrashOnlyPrimaryBackupEngine pb) {
                if (id.equals(members(group).getFirst()) && pb.snapshot().ownershipEpoch().value() == 1) return id;
                if (id.equals(members(group).get(1)) && pb.readyAtNanos() >= 0) return id;
            }
        }
        return null;
    }
    void membership(NodeId observer, MembershipEvent event) {
        if (measured) trace("membership observer=" + observer.value() + " member=" + event.member().value() + " state=" + event.current());
        if (measured && event.current() == MemberStatus.DEAD && !dead.contains(event.member())) falseDead++;
        CacheEngine e = engines.get(observer);
        if (e instanceof RaftEngine raft) raft.onMembershipChanged(event);
        else ((CrashOnlyPrimaryBackupEngine) e).onMembershipChanged(event);
    }
    void trace(String detail) { journal.add((now - start) / MS + ":" + detail); }
    void schedule(long at, Runnable task) { events.add(new Event(at, sequence++, task)); }
    CompletionStage<Void> send(PeerEnvelope envelope) {
        long bytes = size(envelope.message());
        if (envelope.message() instanceof SwimMessage) detectorBytes += measured ? bytes : 0;
        else replicationBytes += measured ? bytes : 0;
        long finish = Math.max(now, egress.getOrDefault(envelope.from(), now))
                + (bytes * MS + EGRESS_BYTES_PER_MS - 1) / EGRESS_BYTES_PER_MS;
        egress.put(envelope.from(), finish);
        long delay = MS;
        // 경로·시각의 함수다. 전략별 난수 소비량이 장애 조건을 바꾸지 않는다.
        if (measured && scenario.equals("jitter") && (now - start) / MS >= 600 && (now - start) / MS < 1100)
            delay += 25 * MS;
        schedule(finish + delay, () -> {
            if (dead.contains(envelope.from()) || dead.contains(envelope.to())) return;
            if (envelope.message() instanceof SwimMessage m) detectors.get(envelope.to()).onMessage(envelope.from(), m);
            else engines.get(envelope.to()).onPeerMessage(envelope.from(), envelope.message());
        });
        return CompletableFuture.completedFuture(null);
    }
    static long size(PeerMessage message) {
        if (message instanceof SwimMessage m) return 56 + 32L * m.updates().size();
        if (message instanceof PrimaryBackupMessage.Replicate m) return 40 + commandSize(m.command());
        if (message instanceof PrimaryBackupMessage.ReplicateAck) return 40;
        if (message instanceof CrashOnlyPrimaryBackupEngine.Control) return 36;
        if (message instanceof RaftMessage.Vote) return 44;
        if (message instanceof RaftMessage.VoteReply) return 33;
        if (message instanceof RaftMessage.AppendReply) return 45;
        if (message instanceof RaftMessage.Append m) {
            long bytes = 60;
            for (var e : m.entries()) bytes += 9 + e.request().map(r -> 16 + commandSize(r.command())).orElse(0L);
            return bytes;
        }
        throw new IllegalArgumentException("unmodeled message " + message.getClass());
    }
    static long commandSize(Command c) {
        return switch (c) {
            case Command.Set s -> 9L + s.key().size() + s.value().size();
            case Command.Get g -> 5L + g.key().size();
            case Command.Delete d -> 5L + d.key().size();
            case Command.Exists e -> 5L + e.key().size();
            case Command.Count ignored -> 1;
        };
    }
    void tick() {
        long target = now + MS;
        int guard = 0;
        while (!events.isEmpty() && events.peek().at() <= target) {
            if (++guard > 100_000) throw new IllegalStateException("event storm");
            Event event = events.remove(); now = event.at(); event.task().run();
        }
        now = target;
        for (var entry : engines.entrySet()) {
            if (dead.contains(entry.getKey())) continue;
            CacheEngine e = entry.getValue();
            e.onTimer(new TimerEvent(e instanceof RaftEngine ? RaftEngine.TICK : CrashOnlyPrimaryBackupEngine.TICK, now));
            detectors.get(entry.getKey()).tick();
        }
        collect();
    }
    void advance(int millis) { for (int i = 0; i < millis; i++) tick(); }
    CompletableFuture<Result> execute(long group, Command c) {
        NodeId node = leader(group);
        if (node == null) return CompletableFuture.completedFuture(Result.of(ResultStatus.UNAVAILABLE, table.version(), OwnershipEpoch.ZERO));
        CacheEngine engine = engines.get(node);
        return engine.execute(c, new RequestContext(new ClientId(7), new OperationId(++operation), table.version(),
                engine.snapshot().ownershipEpoch(), new Deadline(now + 250 * MS))).toCompletableFuture();
    }
    void collect() {
        Iterator<Pending> iterator = pending.iterator();
        while (iterator.hasNext()) {
            Pending p = iterator.next();
            if (!p.future().isDone()) continue;
            Result r = p.future().join();
            busyKeys.remove(p.key()); iterator.remove();
            if (r.status() == ResultStatus.OK) {
                if (p.command() instanceof Command.Set s) {
                    oracle.put(s.key(), s.value());
                    acknowledged.put(s.key(), issued.get(s.key()).get(s.value()));
                }
                if (p.command() instanceof Command.Get && !validValue(p.key(), r.value().orElse(null))) staleReads++;
                if (p.measured()) {
                    ok++; latencies.add((now - p.start()) / (double) MS);
                    if (crashedAt >= 0 && recoveredAt < 0
                            && table.lookup(variant.scheme().keyHash(p.key())).owner().value() == 1
                            && p.command() instanceof Command.Set) recoveredAt = now;
                }
            } else {
                if (p.command() instanceof Command.Get && r.status() == ResultStatus.MISS && oracle.containsKey(p.key())) staleReads++;
                if (p.measured()) rejected++;
            }
        }
    }
    void offer(Bytes key, Command command) {
        offered++;
        long hash = variant.scheme().keyHash(key);
        long[] counts = heat.computeIfAbsent(key, ignored -> new long[2]);
        counts[command instanceof Command.Get ? 0 : 1]++;
        Tablet tablet = table.lookup(hash);
        if (busyKeys.contains(key) || beginBarrier != null && beginBarrier.contains(hash)
                || active != null && (active.frozen || active.draining) && active.tablet.range().contains(hash)) {
            rejected++; return;
        }
        busyKeys.add(key);
        if (command instanceof Command.Set s) issued.computeIfAbsent(key, ignored -> new LinkedHashMap<>()).put(s.value(), ++writeSequence);
        pending.add(new Pending(key, command, execute(tablet.owner().value(), command), now, true));
        collect();
    }

    /** 같은 seed는 모든 전략에서 같은 요청열이다. 요청열 난수와 detector 난수는 분리한다. */
    public Measurement run() {
        advance(1000);
        for (Bytes key : keys) {
            var value = Bytes.utf8("prefill-" + key + "-" + "x".repeat(240));
            CompletableFuture<Result> f = execute(table.lookup(variant.scheme().keyHash(key)).owner().value(), new Command.Set(key, value));
            int waited = 0;
            while (!f.isDone() && waited++ < 1000) tick();
            if (!f.isDone() || f.join().status() != ResultStatus.OK) throw new IllegalStateException("prefill failed " + variant.id());
            oracle.put(key, value);
            issued.put(key, new LinkedHashMap<>(Map.of(value, 0L)));
            acknowledged.put(key, 0L);
        }
        advance(400);
        start = now; measured = true;
        Random tape = new Random(seed);
        List<Bytes> hot = keys.stream().filter(k -> Long.compareUnsigned(variant.scheme().keyHash(k), 1L << 61) < 0).toList();
        for (int elapsed = 0; elapsed < DURATION_MS; elapsed++) {
            if (elapsed == BALANCE_MS) plan();
            if (elapsed == CRASH_MS && scenario.equals("crash-overlap")) {
                NodeId target = leader(1);
                if (target == null) throw new IllegalStateException("no leader to crash");
                dead.add(target); engines.get(target).close(); crashedAt = now; trace("crash node=" + target.value());
            }
            if (elapsed % REQUEST_INTERVAL_MS == 0) {
                // RANGE와 SLOT은 같은 KeyHash를 사용해 hotspot key 집합도 같다.
                boolean useHot = !scenario.equals("uniform") && tape.nextDouble() < 0.85;
                Bytes key = useHot && !hot.isEmpty() ? hot.get(tape.nextInt(hot.size())) : keys.get(tape.nextInt(keys.size()));
                boolean write = tape.nextDouble() < 0.35;
                tapeHash = 31 * tapeHash + key.hashCode(); tapeHash = 31 * tapeHash + (write ? 1 : 0);
                Command command = write ? new Command.Set(key, Bytes.utf8(seed + ":" + elapsed + ":" + "v".repeat(240))) : new Command.Get(key);
                offer(key, command);
            }
            progressMove();
            if (beginBarrier != null || active != null && (active.frozen || active.draining)) freezeTicks++;
            tick();
        }
        // 측정 종료 후 완료는 goodput에 넣지 않는다. pending은 별도로 남긴다.
        measured = false;
        long outstanding = pending.stream().filter(Pending::measured).count();
        long finalOk = ok, finalRejected = rejected;
        List<Double> measuredLatencies = new ArrayList<>(latencies);
        // 수락한 요청을 배출한 뒤 oracle을 검사한다. 미완료 이동은 별도 열에 남긴다.
        advance(600);
        long lost = 0;
        for (var entry : oracle.entrySet()) {
            long owner = table.lookup(variant.scheme().keyHash(entry.getKey())).owner().value();
            NodeId l = leader(owner);
            if (l == null || !validValue(entry.getKey(), stores.get(l).data.get(entry.getKey()))) lost++;
        }
        Collections.sort(measuredLatencies);
        double p99 = measuredLatencies.isEmpty() ? Double.NaN : measuredLatencies.get((int) Math.ceil(measuredLatencies.size() * .99) - 1);
        return new Measurement(variant.id(), scenario, seed, offered, finalOk, finalRejected, outstanding, p99, lost,
                staleReads, falseDead, replicationBytes, detectorBytes, migrationBytes, freezeTicks, splits, plannedMoves,
                completedMoves, crashedAt < 0 || recoveredAt < 0 ? -1 : (recoveredAt - crashedAt) / MS, tapeHash, List.copyOf(journal));
    }

    boolean validValue(Bytes key, Bytes value) {
        // TIMEOUT도 나중에 적용될 수 있다. 마지막 OK보다 새로 발행된 값은 유실로 세지 않는다.
        if (value == null) return false;
        Long version = issued.getOrDefault(key, Map.of()).get(value);
        return version != null && version >= acknowledged.getOrDefault(key, 0L);
    }

    void plan() {
        if (!variant.split().equals("NONE")) {
            Tablet hottest = table.tablets().stream().max(Comparator.comparingLong(t -> load(t).totalRequests())).orElseThrow();
            SplitDecision decision = variant.splitter().decide(load(hottest));
            if (decision instanceof SplitDecision.Split s) { table = table.withSplit(hottest.id(), s.at()); splits++; }
        }
        Map<TabletId, Long> loads = new LinkedHashMap<>(), bytes = new LinkedHashMap<>();
        for (Tablet t : table.tablets()) {
            loads.put(t.id(), load(t).totalRequests());
            NodeId l = leader(t.owner().value());
            long count = l == null ? 0 : stores.get(l).data.entrySet().stream()
                    .filter(e -> t.range().contains(variant.scheme().keyHash(e.getKey())))
                    .mapToLong(e -> 8L + e.getKey().size() + e.getValue().size()).sum();
            bytes.put(t.id(), count);
        }
        moves.addAll(variant.placer().decide(new PlacementInput(table, loads, bytes,
                Map.of(new NodeId(1), 100.0, new NodeId(2), 100.0), 1, Map.of())).moves());
        plannedMoves = moves.size();
        trace("plan splits=" + splits + " moves=" + plannedMoves);
    }
    TabletLoad load(Tablet t) {
        TreeMap<Long, long[]> byHash = new TreeMap<>(Long::compareUnsigned);
        heat.forEach((key, counts) -> {
            long hash = variant.scheme().keyHash(key);
            if (t.range().contains(hash)) {
                long[] existing = byHash.computeIfAbsent(hash, ignored -> new long[2]);
                existing[0] += counts[0]; existing[1] += counts[1];
            }
        });
        return new TabletLoad(t.range(), 1, byHash.entrySet().stream()
                .map(e -> new KeyLoad(e.getKey(), e.getValue()[0], e.getValue()[1])).toList());
    }
    void progressMove() {
        if (active == null && !moves.isEmpty()) {
            Change next = moves.getFirst();
            Tablet t = table.get(next.unit());
            // Begin 전에 source의 client 요청을 배출해 snapshot 경계를 보장한다.
            if (leader(next.from().value()) == null) return;
            beginBarrier = t.range();
            if (pending.stream().anyMatch(p -> t.range().contains(variant.scheme().keyHash(p.key())))) return;
            beginBarrier = null;
            moves.removeFirst(); active = new Move(next);
            trace("move-begin tablet=" + next.unit().value() + " from=" + next.from().value() + " to=" + next.to().value());
            active.protocol.begin(active.job);
        }
        if (active != null) active.step();
    }

    final class Move implements PreCopyProtocol.Source, PreCopyProtocol.Destination {
        final Change change;
        final Tablet tablet;
        final MigrationJob job;
        final MigrationJob receiverSession;
        final MigrationProtocol protocol;
        final PreCopyReceiver receiver = new PreCopyReceiver();
        final List<Map.Entry<Bytes, Bytes>> installing = new ArrayList<>();
        CompletableFuture<Result> install;
        Consumer<Bytes> hook;
        boolean frozen, importStarted, draining;
        DeltaBatch lastDelta;
        int offset;
        Move(Change change) {
            this.change = change; tablet = table.get(change.unit());
            protocol = variant.copy().equals("PRE") ? new PreCopyProtocol(this, this, table,
                    new PreCopyProtocol.Settings(2048, 512, 3)) : new StopAndCopyProtocol(this, this, table, 2048);
            job = MigrationJob.pending(change, protocol.name());
            // Receiver의 snapshot 검증을 공유하되 protocol session은 adapter 안에서만 변환한다.
            receiverSession = MigrationJob.pending(change, "pre-copy");
        }
        Store source() {
            NodeId l = leader(change.from().value());
            return l == null ? null : stores.get(l);
        }
        long sent() { return protocol instanceof PreCopyProtocol pre ? pre.job().progress().sentBytes()
                : ((StopAndCopyProtocol) protocol).job().progress().sentBytes(); }
        boolean ready() { return protocol instanceof PreCopyProtocol pre ? pre.readyToCommit()
                : ((StopAndCopyProtocol) protocol).readyToCommit(); }
        void step() {
            NodeId l = leader(change.from().value());
            if (l == null) return;
            boolean boundary = protocol instanceof PreCopyProtocol pre && (pre.state() == JobState.COPYING && receiver.complete(receiverSession)
                    || pre.state() == JobState.CATCHING_UP && lastDelta != null && lastDelta.last()
                    && receiver.deltaAcknowledgement(receiverSession, lastDelta.batchId()).isPresent());
            if (boundary && pending.stream().anyMatch(p -> tablet.range().contains(variant.scheme().keyHash(p.key())))) {
                draining = true; return;
            }
            draining = false;
            long before = sent();
            long budget = egress.getOrDefault(l, now) <= now ? EGRESS_BYTES_PER_MS : 0;
            JobState state = protocol.step(budget);
            long used = sent() - before;
            migrationBytes += used;
            if (used > 0) egress.put(l, now + (used * MS + EGRESS_BYTES_PER_MS - 1) / EGRESS_BYTES_PER_MS);
            if (state == JobState.DONE) { trace("move-done tablet=" + tablet.id().value()); completedMoves++; active = null; return; }
            if (!ready()) return;
            // Staging 데이터도 목적지의 실제 복제 엔진으로 확정한 뒤 표를 게시한다.
            if (!importStarted) { installing.addAll(receiver.data().entrySet()); importStarted = true; }
            if (install != null) {
                if (!install.isDone()) return;
                if (install.join().status() == ResultStatus.OK) offset++;
                install = null;
            }
            if (offset < installing.size()) {
                var record = installing.get(offset);
                install = execute(change.to().value(), new Command.Set(record.getKey(), record.getValue()));
                return;
            }
            RoutingTable beforeTable = table;
            table = table.apply(change);
            trace("publish tablet=" + tablet.id().value() + " owner=" + change.to().value());
            if (protocol instanceof PreCopyProtocol pre) pre.committed(beforeTable, table);
            else ((StopAndCopyProtocol) protocol).committed(beforeTable, table);
        }
        @Override public boolean freeze(Tablet t) { if (frozen) return false; frozen = true; trace("freeze tablet=" + t.id().value()); return true; }
        @Override public boolean frozen(Tablet t) { return frozen; }
        @Override public void unfreeze(Tablet t) { frozen = false; }
        // 복제 로그에 남은 source 값은 실행 끝까지 보관한다. 라우팅이 접근을 차단한다.
        @Override public boolean cleanup(Tablet t) { return true; }
        @Override public boolean startTracking(Tablet t, Consumer<Bytes> onWrite) { hook = onWrite; return true; }
        @Override public void stopTracking(Tablet t) { hook = null; }
        @Override public List<TransferBatch.Record> snapshot(Tablet t) {
            Store store = Objects.requireNonNull(source(), "source unavailable");
            return store.data.entrySet().stream().filter(e -> t.range().contains(variant.scheme().keyHash(e.getKey())))
                    .map(e -> new TransferBatch.Record(e.getKey(), e.getValue())).toList();
        }
        @Override public Optional<Bytes> read(Tablet t, Bytes key) {
            return Optional.ofNullable(Objects.requireNonNull(source()).data.get(key));
        }
        @Override public boolean prepare(MigrationJob j, Tablet t) { return j == job && receiver.prepare(receiverSession, t); }
        @Override public void receive(MigrationJob j, OwnershipEpoch epoch, TransferBatch b) {
            schedule(now + MS, () -> receiver.receive(receiverSession, epoch, b));
        }
        @Override public Optional<StopAndCopyProtocol.Acknowledgement> acknowledgement(MigrationJob j, long id) {
            return receiver.acknowledgement(receiverSession, id);
        }
        @Override public boolean complete(MigrationJob j) { return receiver.complete(receiverSession); }
        @Override public void discard(MigrationJob j) { receiver.discard(receiverSession); }
        @Override public void receiveDelta(MigrationJob j, OwnershipEpoch epoch, DeltaBatch b) {
            lastDelta = b;
            schedule(now + MS, () -> receiver.receiveDelta(receiverSession, epoch, b));
        }
        @Override public Optional<DeltaBatch.Acknowledgement> deltaAcknowledgement(MigrationJob j, long id) {
            return receiver.deltaAcknowledgement(receiverSession, id);
        }
        @Override public boolean caughtUp(MigrationJob j, long seq) { return receiver.caughtUp(receiverSession, seq); }
    }

    final class Store implements CacheEngine {
        final NodeId node;
        final Map<Bytes, Bytes> data = new LinkedHashMap<>();
        long version, availableAt;
        Store(NodeId node) { this.node = node; }
        @Override public CompletionStage<Result> execute(Command c, RequestContext context) {
            if (serviceNanos == 0) return CompletableFuture.completedFuture(apply(c, context));
            CompletableFuture<Result> result = new CompletableFuture<>();
            availableAt = Math.max(availableAt, now) + serviceNanos;
            schedule(availableAt, () -> result.complete(dead.contains(node)
                    ? Result.of(ResultStatus.UNAVAILABLE, table.version(), context.ownershipEpoch()) : apply(c, context)));
            return result;
        }
        Result apply(Command c, RequestContext context) {
            if (context.deadline().isExpired(time)) return Result.of(ResultStatus.TIMEOUT, table.version(), context.ownershipEpoch());
            Result result;
            switch (c) {
                case Command.Set s -> {
                    data.put(s.key(), s.value()); version++;
                    if (active != null && active.hook != null && members(active.change.from().value()).contains(node)
                            && active.tablet.range().contains(variant.scheme().keyHash(s.key()))) active.hook.accept(s.key());
                    result = Result.of(ResultStatus.OK, table.version(), context.ownershipEpoch());
                }
                case Command.Get g -> result = data.containsKey(g.key())
                        ? Result.ok(data.get(g.key()), version, table.version(), context.ownershipEpoch())
                        : Result.of(ResultStatus.MISS, table.version(), context.ownershipEpoch());
                case Command.Count ignored -> result = Result.ok(Bytes.copyOf(ByteBuffer.allocate(8).putLong(data.size()).array()),
                        version, table.version(), context.ownershipEpoch());
                default -> throw new IllegalArgumentException("fixture only supports GET/SET/COUNT");
            }
            return result;
        }
        @Override public void onPeerMessage(NodeId from, PeerMessage message) { throw new UnsupportedOperationException(); }
        @Override public void onTimer(TimerEvent event) { }
        @Override public EngineSnapshot snapshot() { return new EngineSnapshot(node, now, table.version(), OwnershipEpoch.ZERO); }
        @Override public void close() { }
    }
    @Override public void close() { engines.values().forEach(CacheEngine::close); events.clear(); }
}
