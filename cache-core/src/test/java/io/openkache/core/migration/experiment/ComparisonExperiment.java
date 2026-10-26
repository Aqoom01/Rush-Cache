package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.Redirect;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.postcopy.PostCopyProtocol;
import io.openkache.core.migration.postcopy.PostCopyState;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.RoutingTable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/** 같은 요청 시도 열·시간 예산 아래 실제 세 protocol을 호출하는 단일 Tablet 비교 모델. */
final class ComparisonExperiment {
    static final List<String> VARIANTS = List.of("stop-and-copy", "pre-copy", "post-copy");
    static final String GENERATOR = "b3-comparison-tick-trace-2";
    static final Bytes PROBE = Bytes.utf8("b3-comparison-control-missing-key");
    static final int READS_PER_TICK = 16;

    record Case(String id, String scenario, int keys, long budget, double rho, int deletePercent,
            double theta, long threshold, int maxRounds) {
        Case {
            if (keys < 1 || budget < 1 || rho < 0 || deletePercent < 0 || deletePercent > 100
                    || theta < 0 || threshold < 0 || maxRounds < 0) throw new IllegalArgumentException("invalid case");
        }
        Map<String, Object> config() {
            return Map.of("keys", keys, "budgetBytesPerTick", budget, "rho", rho, "deletePercent", deletePercent,
                    "zipfTheta", theta, "stopThresholdBytes", threshold, "maxRounds", maxRounds,
                    "readsPerTick", READS_PER_TICK, "maxTicks", 100_000);
        }
    }

    record Run(Case condition, long seed, String variant, String inputHash, Map<String, Object> metrics,
            String stopReason, List<Map<String, Object>> phases) {}

    static List<Case> cases(boolean quick) {
        int[] sizes = quick ? new int[] {100, 1_000, 10_000} : new int[] {10_000, 100_000, 1_000_000};
        int normal = sizes[1];
        long budget = quick ? 4_096 : 65_536;
        long threshold = quick ? 1_024 : 262_144;
        List<Case> cases = new ArrayList<>();
        for (int n : sizes) cases.add(new Case("S1-n" + n, "S1", n, budget, 0, 30, 0, threshold, 10));
        for (int n : sizes) cases.add(new Case("S2-n" + n, "S2", n, budget, .1, 30, 0, threshold, 10));
        for (double rho : new double[] {.01, .1, .5, .9, 1.2}) {
            cases.add(new Case("S3-rho" + rho, "S3", normal, budget, rho, 30, 0, threshold, 10));
        }
        for (double theta : new double[] {0, .99, 1.2}) {
            cases.add(new Case("S4-theta" + theta, "S4", normal, budget, .5, 30, theta, threshold, 10));
        }
        for (int deletes : new int[] {0, 30}) {
            cases.add(new Case("S5-delete" + deletes, "S5", normal, budget, .5, deletes, 0, threshold, 10));
        }
        return List.copyOf(cases);
    }

    static boolean sensitivity(String profile) {
        return profile.equals("sensitivity-full") || profile.equals("sensitivity-quick");
    }

    static List<Case> cases(String profile) {
        if (profile.equals("full") || profile.equals("quick")) return cases(profile.equals("quick"));
        if (!sensitivity(profile)) throw new IllegalArgumentException("invalid profile: " + profile);
        boolean quick = profile.equals("sensitivity-quick");
        List<Case> cases = new ArrayList<>();
        // quick도 threshold/budget 비율은 보존한다. 데이터 크기를 줄인 재현 검증용이다.
        for (double rho : new double[] {.01, .1, .5, .9, 1.2}) {
            for (long threshold : quick ? new long[] {4_096, 16_384, 65_536}
                    : new long[] {65_536, 262_144, 1_048_576}) {
                for (int rounds : new int[] {3, 10, 30}) {
                    cases.add(new Case("S3-sensitivity-rho" + rho + "-threshold" + threshold + "-rounds" + rounds,
                            "S3-sensitivity", quick ? 1_000 : 100_000, quick ? 4_096 : 65_536,
                            rho, 30, 0, threshold, rounds));
                }
            }
        }
        return List.copyOf(cases);
    }

    /** 알고리즘 설정을 제외한 요청 생성 규격. 설정 사이에도 같은 workload를 사용했는지 검증한다. */
    static String workloadHash(Case c, long seed) {
        var config = new java.util.TreeMap<>(c.config());
        config.remove("stopThresholdBytes");
        config.remove("maxRounds");
        return sha256(GENERATOR + ":seed=" + seed + ":" + BaselineJson.encode(config));
    }

    /** 각 tick에 같은 연산 수/내용을 생성하며 결과·variant에 따라 난수 소비를 바꾸지 않는다. */
    static final class Trace {
        private final Case condition;
        private final long seed;
        private final double[] cdf;
        private final Bytes[] keys;

        Trace(Case condition, long seed) {
            this.condition = condition;
            this.seed = seed;
            keys = new Bytes[condition.keys()];
            for (int i = 0; i < keys.length; i++) keys[i] = MigrationFixture.key(seed, i);
            if (condition.theta() == 0) cdf = null;
            else {
                cdf = new double[keys.length];
                double sum = 0;
                for (int i = 0; i < cdf.length; i++) { sum += 1 / Math.pow(i + 1.0, condition.theta()); cdf[i] = sum; }
                for (int i = 0; i < cdf.length; i++) cdf[i] /= sum;
                cdf[cdf.length - 1] = 1;
            }
        }

        private Bytes choose(SplittableRandom random) {
            if (cdf == null) return keys[random.nextInt(keys.length)];
            double sample = random.nextDouble();
            int low = 0, high = cdf.length - 1;
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (cdf[mid] <= sample) low = mid + 1;
                else high = mid;
            }
            // 고정 홀수 affine permutation으로 hot rank와 초기 key index의 순서를 분리한다.
            return keys[(int) ((low * 104_729L + seed * 97) % keys.length)];
        }

        List<Command> at(int tick) {
            // 단순 golden-ratio 증가 seed는 SplittableRandom 내부 증가량과 겹쳐 tick 사이 열을 재사용한다.
            long mixed = seed ^ (0x9E3779B97F4A7C15L * (tick + 1));
            mixed = (mixed ^ (mixed >>> 30)) * 0xBF58476D1CE4E5B9L;
            mixed = (mixed ^ (mixed >>> 27)) * 0x94D049BB133111EBL;
            SplittableRandom random = new SplittableRandom(mixed ^ (mixed >>> 31));
            double rate = condition.rho() * condition.budget() / 152.0;
            int writes = (int) (Math.floor((tick + 1) * rate) - Math.floor(tick * rate));
            List<Command> commands = new ArrayList<>(writes + READS_PER_TICK);
            for (int i = 0; i < writes; i++) {
                Bytes key = choose(random);
                if (random.nextInt(100) < condition.deletePercent()) commands.add(new Command.Delete(key));
                else commands.add(new Command.Set(key, MigrationFixture.value(((long) tick << 32) | i)));
            }
            for (int i = 0; i < READS_PER_TICK; i++) commands.add(new Command.Get(choose(random)));
            return commands;
        }

        Map<Bytes, Bytes> initial() {
            Map<Bytes, Bytes> initial = new LinkedHashMap<>();
            Bytes value = MigrationFixture.value(-1);
            for (Bytes key : keys) initial.put(key, value);
            return initial;
        }
    }

    private abstract static class Driver {
        boolean committed;
        long commitTick = -1;
        abstract void begin();
        abstract String state();
        abstract boolean done();
        abstract boolean ready();
        abstract void commit();
        abstract void step(long budget);
        abstract long bytes();
        abstract RoutingTable table();
        abstract Map<Bytes, Bytes> source();
        abstract Map<Bytes, Bytes> destination();
        abstract Result requestOwner(Command command);
        int rounds() { return 0; }
        String stopReason() { return null; }

        Result request(NodeId target, Command command) {
            var tablet = table().get(LiveMigrationFixture.MOVING);
            if (!target.equals(tablet.owner())) {
                return Result.redirect(ResultStatus.MOVED, new Redirect(tablet.owner()), table().version(), tablet.epoch());
            }
            return requestOwner(command);
        }
        Result request(Command command) { return request(table().get(LiveMigrationFixture.MOVING).owner(), command); }
        Result status(ResultStatus status) {
            return Result.of(status, table().version(), table().get(LiveMigrationFixture.MOVING).epoch());
        }
        Result local(Map<Bytes, Bytes> data, Command command, boolean writable, LiveMigrationFixture.Store hooks) {
            if (!(command instanceof Command.Get) && !writable) return status(ResultStatus.BUSY);
            return switch (command) {
                case Command.Get get -> data.containsKey(get.key())
                        ? Result.ok(data.get(get.key()), 0, table().version(), table().get(LiveMigrationFixture.MOVING).epoch())
                        : status(ResultStatus.MISS);
                case Command.Set set -> {
                    if (hooks == null) data.put(set.key(), set.value()); else hooks.put(set.key(), set.value());
                    yield status(ResultStatus.OK);
                }
                case Command.Delete delete -> {
                    boolean exists = data.containsKey(delete.key());
                    if (exists) {
                        if (hooks == null) data.remove(delete.key()); else hooks.delete(delete.key());
                    }
                    yield status(exists ? ResultStatus.OK : ResultStatus.MISS);
                }
                default -> throw new IllegalArgumentException("CRUD only");
            };
        }
    }

    private static Driver driver(String variant, Case c, Map<Bytes, Bytes> initial) {
        if (variant.equals("stop-and-copy")) {
            var f = new MigrationFixture(false, MigrationFixture.Fault.NONE);
            f.source.data.putAll(initial);
            return new Driver() {
                @Override void begin() { f.protocol.begin(f.initialJob); }
                @Override String state() { return f.protocol.state().name(); }
                @Override boolean done() { return f.protocol.state() == JobState.DONE; }
                @Override boolean ready() { return f.protocol.readyToCommit(); }
                @Override void commit() {
                    if (!f.coordinator.commit(f.protocol.job(), f.protocol.expectedTable())) throw new IllegalStateException("CAS failed");
                    f.protocol.committed(f.coordinator.current()); committed = true;
                }
                @Override void step(long budget) { f.protocol.step(budget); }
                @Override long bytes() { return f.protocol.job().progress().sentBytes(); }
                @Override RoutingTable table() { return f.coordinator.current(); }
                @Override Map<Bytes, Bytes> source() { return f.source.data; }
                @Override Map<Bytes, Bytes> destination() { return f.destination; }
                @Override Result requestOwner(Command command) {
                    return f.request(table().get(LiveMigrationFixture.MOVING).owner(), command);
                }
            };
        }
        var f = new LiveMigrationFixture(false);
        f.source.data.putAll(initial);
        if (variant.equals("pre-copy")) {
            var p = new PreCopyProtocol(f.source, f.link, f.current, new PreCopyProtocol.Settings(65_536, c.threshold(), c.maxRounds()));
            return new Driver() {
                private Map<Bytes, Bytes> destination = Map.of();
                @Override void begin() { p.begin(f.job(p.name())); }
                @Override String state() { return p.state().name(); }
                @Override boolean done() { return p.state() == JobState.DONE; }
                @Override boolean ready() { return p.readyToCommit(); }
                @Override void commit() {
                    if (!f.commit(p)) throw new IllegalStateException("CAS failed");
                    destination = new LinkedHashMap<>(f.link.receiver.data()); committed = true;
                }
                @Override void step(long budget) { p.step(budget); }
                @Override long bytes() { return p.job().progress().sentBytes(); }
                @Override RoutingTable table() { return f.current; }
                @Override Map<Bytes, Bytes> source() { return f.source.data; }
                @Override Map<Bytes, Bytes> destination() { return destination; }
                @Override Result requestOwner(Command command) {
                    return committed ? local(destination, command, true, null)
                            : local(f.source.data, command, p.acceptsWrite(), f.source);
                }
                @Override int rounds() { return p.rounds(); }
                @Override String stopReason() { return p.stopReason().map(Enum::name).orElse(null); }
            };
        }
        if (!variant.equals("post-copy")) throw new IllegalArgumentException(variant);
        var p = new PostCopyProtocol(f.source, f.current);
        MigrationJob job = f.job(p.name());
        return new Driver() {
            @Override void begin() { p.begin(job); }
            @Override String state() { return p.state().name(); }
            @Override boolean done() { return p.state() == PostCopyState.DONE; }
            @Override boolean ready() { return p.readyToCommit(); }
            @Override void commit() {
                if (!f.commit(p, job)) throw new IllegalStateException("CAS failed"); committed = true;
            }
            @Override void step(long budget) { p.step(budget); }
            @Override long bytes() { return p.sentBytes(); }
            @Override RoutingTable table() { return f.current; }
            @Override Map<Bytes, Bytes> source() { return f.source.data; }
            @Override Map<Bytes, Bytes> destination() { return p.data(); }
            @Override Result requestOwner(Command command) {
                return committed ? p.execute(command) : local(f.source.data, command, p.acceptsWrite(), f.source);
            }
        };
    }

    static Run run(Case c, long seed, String variant) {
        Trace trace = new Trace(c, seed);
        Map<Bytes, Bytes> oracle = trace.initial();
        Driver d = driver(variant, c, oracle);
        long writes = 0, accepted = 0, writeBusy = 0, reads = 0, readBusy = 0;
        long blockedTicks = 0;
        List<Map<String, Object>> phases = new ArrayList<>();
        d.begin();
        phases.add(Map.of("tick", 0, "state", d.state(), "sentBytes", d.bytes(), "round", d.rounds()));
        int tick = 0;
        for (; tick < 100_000 && !d.done(); tick++) {
            Result probe = d.request(new Command.Delete(PROBE));
            if (probe.status() == ResultStatus.BUSY) blockedTicks++;
            else require(probe.status() == ResultStatus.MISS, "control request changed data");
            for (Command command : trace.at(tick)) {
                Result result = d.request(command);
                switch (command) {
                    case Command.Get get -> {
                        reads++;
                        if (result.status() == ResultStatus.BUSY) readBusy++;
                        else if (oracle.containsKey(get.key())) require(result.status() == ResultStatus.OK
                                && result.value().orElseThrow().equals(oracle.get(get.key())), "stale/false-miss read");
                        else require(result.status() == ResultStatus.MISS, "resurrected key");
                    }
                    case Command.Set set -> {
                        writes++;
                        if (result.status() == ResultStatus.BUSY) writeBusy++;
                        else { require(result.status() == ResultStatus.OK, "SET outcome"); oracle.put(set.key(), set.value()); accepted++; }
                    }
                    case Command.Delete delete -> {
                        writes++;
                        if (result.status() == ResultStatus.BUSY) writeBusy++;
                        else {
                            boolean existed = oracle.remove(delete.key()) != null;
                            require(result.status() == (existed ? ResultStatus.OK : ResultStatus.MISS), "DELETE outcome"); accepted++;
                        }
                    }
                    default -> throw new IllegalArgumentException("CRUD only");
                }
            }
            String before = d.state();
            int previousRound = d.rounds();
            long bytes = d.bytes();
            if (d.ready()) { d.commit(); d.commitTick = tick + 1L; }
            else d.step(c.budget());
            require(d.bytes() - bytes <= c.budget(), "byte budget exceeded");
            if (!before.equals(d.state()) || previousRound != d.rounds()) {
                phases.add(Map.of("tick", tick + 1, "state", d.state(), "sentBytes", d.bytes(), "round", d.rounds()));
            }
        }
        require(d.done(), "model timeout " + c.id() + "/" + variant);
        require(d.committed && d.table().version().value() == 1
                && d.table().get(LiveMigrationFixture.MOVING).epoch().value() == 1, "ownership commit");
        require(d.source().isEmpty(), "source cleanup");
        Map<Bytes, Bytes> actual = d.destination();
        require(oracle.equals(actual), "destination exact comparison");
        Result moved = d.request(LiveMigrationFixture.SOURCE, new Command.Get(trace.keys[0]));
        require(moved.status() == ResultStatus.MOVED && moved.redirect().orElseThrow().target().equals(LiveMigrationFixture.DESTINATION), "source fencing");
        require(d.request(new Command.Delete(PROBE)).status() == ResultStatus.MISS, "write resume");
        Map<String, Object> metrics = new LinkedHashMap<>();
        metric(metrics, "total_ticks", tick, "tick"); metric(metrics, "commit_tick", d.commitTick, "tick");
        metric(metrics, "writes_blocked_ticks", blockedTicks, "tick"); metric(metrics, "sent_bytes", d.bytes(), "bytes");
        metric(metrics, "initial_data_bytes", c.keys() * 152L, "bytes"); metric(metrics, "final_data_bytes", oracle.size() * 152L, "bytes");
        metric(metrics, "transfer_ratio", d.bytes() / (c.keys() * 152.0), "ratio");
        metric(metrics, "writes_attempted", writes, "count"); metric(metrics, "writes_accepted", accepted, "count");
        metric(metrics, "writes_busy", writeBusy, "count"); metric(metrics, "reads_attempted", reads, "count");
        metric(metrics, "reads_busy", readBusy, "count"); metric(metrics, "read_busy_ratio", readBusy / (double) reads, "ratio");
        metric(metrics, "invariant_violations", 0, "count"); metric(metrics, "exact_comparisons", 1, "count");
        if (variant.equals("pre-copy")) metric(metrics, "live_delta_rounds", d.rounds(), "count");
        String input = GENERATOR + ":seed=" + seed + ":" + BaselineJson.encode(new java.util.TreeMap<>(c.config()));
        return new Run(c, seed, variant, sha256(input), Map.copyOf(metrics), d.stopReason(), List.copyOf(phases));
    }

    private static void metric(Map<String, Object> metrics, String name, Number value, String unit) {
        metrics.put(name, Map.of("value", value, "unit", unit));
    }
    static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
    static String sha256(String input) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException failure) { throw new AssertionError(failure); }
    }
    private ComparisonExperiment() {}
}
