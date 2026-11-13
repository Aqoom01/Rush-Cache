package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.migration.experiment.SchedulerAlgorithms;
import io.openkache.core.migration.strategy.ScalingPolicies;
import io.openkache.core.tablet.Tablet;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

/** 연결 계약 검사. Fake는 정책/상태 기계 검사에만 사용하고 native 검사는 실제 TCP/파일 KV를 쓴다. */
class FullIntegratedCacheTest {
    @TempDir Path directory;
    static Stream<String> protocols() { return IntegrationConfig.PROTOCOLS.stream().sorted(); }
    static Stream<String> distributions() { return IntegrationConfig.DISTRIBUTIONS.stream().sorted(); }
    static Stream<String> schedulers() { return SchedulerAlgorithms.METHODS.stream(); }
    static Stream<String> scalers() { return ScalingPolicies.METHODS.stream(); }
    static Properties properties(String protocol, boolean reserve) {
        Properties p = IntegratedCacheTest.properties(protocol);
        p.setProperty("replica.port", "27501"); p.setProperty("gateway.port", "27510");
        p.setProperty("tablets.per.group", "2"); p.setProperty("migration.tick.ms", "1");
        if (reserve) { p.setProperty("groups.count", "3"); p.setProperty("group.2.hosts", "127.0.0.17,127.0.0.18,127.0.0.19"); }
        return p;
    }
    private static Result send(IntegratedCache c, Command command, long id) throws InterruptedException {
        // Tail completion can precede the head's ACK. Follow the real client's bounded
        // same-ID BUSY retry contract instead of assuming head readiness at tail reply.
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5); Result result;
        do {
            result = c.execute(new IntegrationWire.Request(command, 1, id, 0, 5000), 0);
            if (result.status() == ResultStatus.BUSY) Thread.sleep(1);
        } while (result.status() == ResultStatus.BUSY && System.nanoTime() < until);
        return result;
    }
    private static void drained(IntegratedCache c) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (!c.status().contains("\"pendingJobs\":0") && System.nanoTime() < until) Thread.sleep(5);
        assertTrue(c.status().contains("\"pendingJobs\":0"), c.status());
    }
    private static Bytes key(IntegrationConfig c, Tablet t, int start) {
        for (int i = start; i < start + 100000; i++) { Bytes key = Bytes.utf8("{tag" + i + "}:k" + i); if (t.range().contains(c.scheme().keyHash(key))) return key; }
        throw new AssertionError("no key");
    }
    @ParameterizedTest @MethodSource("protocols")
    void allNativeFamiliesUsePostCopyAndRealDestinationApply(String protocol) throws Exception {
        var p = properties(protocol, false); p.setProperty("copy", "POST"); IntegrationConfig cfg = new IntegrationConfig(p);
        var nodes = new ArrayList<AutoCloseable>();
        try (var journal = new JsonLine(directory.resolve("native.jsonl"))) {
            for (int g = 0; g < 2; g++) for (int n = 0; n < 3; n++) nodes.add(cfg.ionia() ? IoniaProfile.replica(cfg, g, n, directory.resolve("g" + g + "n" + n)) : new A1Replica(cfg, g, n, directory.resolve("g" + g + "n" + n)));
            try (var cache = new IntegratedCache(cfg, List.of(new ReplicatedBackend.Native(cfg, 0), new ReplicatedBackend.Native(cfg, 1)), journal)) {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10); while (!cache.ready() && System.nanoTime() < until) Thread.sleep(20); assertTrue(cache.ready());
                Tablet moving = cache.table().ownedBy(new NodeId(0)).getFirst(); Bytes k = key(cfg, moving, 0);
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(k, Bytes.utf8("before")), 1).status());
                cache.move(moving.id().value(), 1); drained(cache); assertTrue(cache.status().contains("\"state\":\"DONE\""), cache.status());
                assertEquals(Optional.of(Bytes.utf8("before")), send(cache, new Command.Get(k), 2).value());
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(k, Bytes.utf8("after")), 3).status());
                cache.move(moving.id().value(), 0); drained(cache);
                assertEquals(Optional.of(Bytes.utf8("after")), send(cache, new Command.Get(k), 4).value(), cache.status());
            }
        } finally { Collections.reverse(nodes); for (var node : nodes) node.close(); }
    }
    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"PB", "PB_FENCED", "RAFT", "VR", "CHAIN", "CRAQ", "ROSE", "O00", "V07"})
    void nativeThreeGroupScaleOutAndDrainPreserveValues(String protocol) throws Exception {
        var p = properties(protocol, true); p.setProperty("copy", "POST"); var cfg = new IntegrationConfig(p); var nodes = new ArrayList<AutoCloseable>();
        try (var journal = new JsonLine(directory.resolve("native-scale.jsonl"))) {
            for (int g = 0; g < 3; g++) for (int n = 0; n < 3; n++) nodes.add(cfg.ionia() ? IoniaProfile.replica(cfg, g, n, directory.resolve("g" + g + "n" + n)) : new A1Replica(cfg, g, n, directory.resolve("g" + g + "n" + n)));
            try (var c = new IntegratedCache(cfg, List.of(new ReplicatedBackend.Native(cfg, 0), new ReplicatedBackend.Native(cfg, 1), new ReplicatedBackend.Native(cfg, 2)), journal)) {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10); while (!c.ready() && System.nanoTime() < until) Thread.sleep(20); assertTrue(c.ready());
                for (int i = 0; i < 12; i++) assertEquals(ResultStatus.OK, send(c, new Command.Set(Bytes.utf8("k" + i), Bytes.utf8("v")), i + 1).status());
                c.scaleTo(3).get(); drained(c); c.scaleTo(1).get(); drained(c); assertTrue(c.status().contains("\"activeGroups\":[0]"), c.status());
                for (int i = 0; i < 12; i++) assertEquals(Optional.of(Bytes.utf8("v")), send(c, new Command.Get(Bytes.utf8("k" + i)), 100 + i).value(), c.status());
                assertEquals(0, ReplicaDiagnostics.number(c.status(), "unresolvedMigrations"));
            }
        } finally { Collections.reverse(nodes); for (var node : nodes) node.close(); }
    }
    @ParameterizedTest @MethodSource("distributions")
    void everyB1SchemeScalesOutAndBackWithAllThreeCopyProtocols(String distribution) throws Exception {
        for (String copy : List.of("STOP", "PRE", "POST")) {
            var p = properties("O00", true); p.setProperty("distribution", distribution); p.setProperty("copy", copy);
            var cfg = new IntegrationConfig(p); var stores = List.of(new Store(), new Store(), new Store());
            try (var journal = new JsonLine(directory.resolve(distribution + copy + ".jsonl")); var cache = new IntegratedCache(cfg, new ArrayList<ReplicatedBackend>(stores), journal)) {
                List<Bytes> keys = new ArrayList<>(); for (int i = 0; i < 40; i++) { Bytes k = Bytes.utf8("{tag" + (i % 7) + "}:key" + i); keys.add(k); assertEquals(ResultStatus.OK, send(cache, new Command.Set(k, Bytes.utf8("v" + i)), i + 1).status()); }
                cache.scaleTo(3).get(); drained(cache); assertTrue(cache.status().contains("\"activeGroups\":[0, 1, 2]"), cache.status());
                cache.scaleTo(1).get(); drained(cache); assertTrue(cache.status().contains("\"activeGroups\":[0]"), cache.status());
                for (int i = 0; i < keys.size(); i++) assertEquals(Optional.of(Bytes.utf8("v" + i)), send(cache, new Command.Get(keys.get(i)), 100 + i).value(), cache.status());
                assertFalse(cache.status().contains("SAFE_HOLD"), cache.status());
            }
        }
    }
    @ParameterizedTest @MethodSource("schedulers")
    void everySchedulerRunsRealB3ProtocolsWithB2PlacementAndDrain(String scheduler) throws Exception {
        for (String copy : List.of("STOP", "PRE", "POST")) {
            var p = properties("O00", true); p.setProperty("scheduler", scheduler); p.setProperty("copy", copy); p.setProperty("placement", "GREEDY");
            var cfg = new IntegrationConfig(p);
            try (var journal = new JsonLine(directory.resolve(scheduler + copy + ".jsonl")); var cache = new IntegratedCache(cfg, List.of(new Store(), new Store(), new Store()), journal)) {
                List<Bytes> keys = new ArrayList<>(); for (int i = 0; i < 24; i++) { Bytes k = Bytes.utf8("k" + i); keys.add(k); assertEquals(ResultStatus.OK, send(cache, new Command.Set(k, Bytes.utf8("v")), i + 1).status()); }
                cache.scaleTo(3).get(); drained(cache); assertFalse(cache.status().contains("SAFE_HOLD"), cache.status());
                cache.scaleTo(1).get(); drained(cache); assertTrue(cache.status().contains("\"activeGroups\":[0]"), cache.status());
                for (int i = 0; i < keys.size(); i++) assertEquals(Optional.of(Bytes.utf8("v")), send(cache, new Command.Get(keys.get(i)), i + 100).value());
            }
        }
    }
    @ParameterizedTest @MethodSource("scalers")
    void everyNodeCountPolicyObservesAndDecidesInTheIntegratedController(String scaler) throws Exception {
        var p = properties("O00", true); p.setProperty("scaler", scaler); p.setProperty("control.cooldown.windows", "0");
        p.setProperty("scaling.period", "4"); p.setProperty("scaling.forecast.horizon", "1");
        Path observed = directory.resolve("observed-rates.txt"); Files.write(observed, java.util.stream.IntStream.range(0, 32).mapToObj(i -> "" + (20 + i % 4)).toList()); p.setProperty("scaling.history.file", observed.toString());
        var cfg = new IntegrationConfig(p); Path events = directory.resolve(scaler + ".jsonl");
        try (var journal = new JsonLine(events); var cache = new IntegratedCache(cfg, List.of(new Store(), new Store(), new Store()), journal)) {
            assertEquals(ResultStatus.OK, send(cache, new Command.Set(Bytes.utf8("key"), Bytes.utf8("value")), 1).status());
            cache.balance().get(); drained(cache); assertTrue(Files.readString(events).contains("scaling-decision"), cache.status());
            assertTrue(Files.readString(events).contains("\"method\":\"" + scaler + "\""));
        }
    }
    @Test void resizeActuallyDoublesAndHalvesDuringMembershipChanges() throws Exception {
        var p = properties("O00", true); p.setProperty("groups.count", "5"); p.setProperty("distribution", "RESIZABLE");
        p.setProperty("group.3.hosts", "127.0.0.20,127.0.0.21,127.0.0.22"); p.setProperty("group.4.hosts", "127.0.0.23,127.0.0.24,127.0.0.25");
        try (var journal = new JsonLine(directory.resolve("resize.jsonl")); var c = new IntegratedCache(new IntegrationConfig(p), List.of(new Store(), new Store(), new Store(), new Store(), new Store()), journal)) {
            for (int i = 0; i < 16; i++) assertEquals(ResultStatus.OK, send(c, new Command.Set(Bytes.utf8("k" + i), Bytes.utf8("v")), i + 1).status());
            c.scaleTo(5).get(); drained(c); assertEquals(8, c.table().size(), c.status());
            c.scaleTo(1).get(); drained(c); assertEquals(4, c.table().size(), c.status());
            for (int i = 0; i < 16; i++) assertEquals(Optional.of(Bytes.utf8("v")), send(c, new Command.Get(Bytes.utf8("k" + i)), 100 + i).value());
        }
    }
    @Test void postCopySetAndDeleteBeforeLatePullPreserveNativeDestination() throws Exception {
        var p = properties("O00", false); p.setProperty("copy", "POST"); p.setProperty("migration.bytes.per.tick", "256"); p.setProperty("migration.tick.ms", "10");
        var cfg = new IntegrationConfig(p); Store source = new Store(), target = new Store();
        try (var journal = new JsonLine(directory.resolve("late-pull.jsonl")); var c = new IntegratedCache(cfg, List.of(source, target), journal)) {
            Tablet t = c.table().ownedBy(new NodeId(0)).getFirst(); Bytes a = key(cfg, t, 0), b = key(cfg, t, 100);
            assertEquals(ResultStatus.OK, send(c, new Command.Set(a, Bytes.copyOf(new byte[4096])), 1).status()); assertEquals(ResultStatus.OK, send(c, new Command.Set(b, Bytes.copyOf(new byte[4096])), 2).status());
            c.move(t.id().value(), 1); long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5); while (c.table().get(t.id()).owner().value() == 0 && System.nanoTime() < until) Thread.sleep(1);
            assertEquals(1, c.table().get(t.id()).owner().value());
            assertEquals(ResultStatus.OK, send(c, new Command.Set(a, Bytes.utf8("new")), 3).status());
            assertEquals(ResultStatus.OK, send(c, new Command.Delete(b), 4).status(), "unresolved source key logically exists even if native destination returns MISS");
            drained(c); assertEquals(Optional.of(Bytes.utf8("new")), send(c, new Command.Get(a), 5).value()); assertEquals(ResultStatus.MISS, send(c, new Command.Get(b), 6).status());
            assertEquals(Bytes.utf8("new"), target.data.get(a)); assertFalse(target.data.containsKey(b));
        }
    }
    @Test void automaticWindowsRepeatAndStopExplicitly() throws Exception {
        var p = properties("O00", true); p.setProperty("control.interval.ms", "25");
        try (var journal = new JsonLine(directory.resolve("auto.jsonl")); var c = new IntegratedCache(new IntegrationConfig(p), List.of(new Store(), new Store(), new Store()), journal)) {
            c.resetMetrics(); Thread.sleep(150); c.stopControl(); assertTrue(ReplicaDiagnostics.number(c.status(), "window") >= 3);
            long windows = ReplicaDiagnostics.number(c.status(), "window"); Thread.sleep(70); assertEquals(windows, ReplicaDiagnostics.number(c.status(), "window"));
        }
    }
    @Test void postCopyRetainsOwnershipAndCanResumeOnlyAfterSourceRecovery() throws Exception {
        var p = properties("O00", false); p.setProperty("copy", "POST"); p.setProperty("migration.bytes.per.tick", "256"); p.setProperty("migration.tick.ms", "10");
        var cfg = new IntegrationConfig(p); Store source = new Store(), target = new Store();
        try (var journal = new JsonLine(directory.resolve("resume.jsonl")); var c = new IntegratedCache(cfg, List.of(source, target), journal)) {
            Tablet t = c.table().ownedBy(new NodeId(0)).getFirst(); Bytes k = key(cfg, t, 0); assertEquals(ResultStatus.OK, send(c, new Command.Set(k, Bytes.copyOf(new byte[4096])), 1).status());
            source.failedGet.set(true); long job = c.move(t.id().value(), 1); drained(c); assertTrue(c.status().contains("FAILED_AFTER_PUBLICATION"), c.status());
            assertEquals(1, c.table().get(t.id()).owner().value()); assertEquals(ResultStatus.UNAVAILABLE, send(c, new Command.Get(k), 2).status());
            source.failedGet.set(false); assertTrue(c.resume(job).get()); drained(c); assertEquals(ResultStatus.OK, send(c, new Command.Get(k), 3).status()); assertFalse(c.status().contains("FAILED_AFTER_PUBLICATION"), c.status());
        }
    }
    private static final class Store implements ReplicatedBackend {
        final Map<Bytes, Bytes> data = new ConcurrentHashMap<>(); final AtomicBoolean failedGet = new AtomicBoolean();
        @Override public Result execute(Command command, long client, long operation, long deadline) {
            if (command instanceof Command.Set s) { data.put(s.key(), s.value()); return ReplicatedBackend.status(ResultStatus.OK); }
            if (command instanceof Command.Delete d) return ReplicatedBackend.status(data.remove(d.key()) == null ? ResultStatus.MISS : ResultStatus.OK);
            if (failedGet.get()) return ReplicatedBackend.status(ResultStatus.UNAVAILABLE);
            Bytes value = data.get(((Command.Get) command).key()); return value == null ? ReplicatedBackend.status(ResultStatus.MISS) : Result.ok(value, 0, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
        @Override public Result administrative(Command c) { return execute(c, 1048575, 1, Long.MAX_VALUE); }
        @Override public OptionalDouble cpu() { return OptionalDouble.of(.5); }
        @Override public boolean supportsDelete() { return true; }
        @Override public void close() { }
    }
}
