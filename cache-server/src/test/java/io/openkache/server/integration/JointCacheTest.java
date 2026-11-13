package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.tablet.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import static org.junit.jupiter.api.Assertions.*;

/** Joint-role tests: real A1 AND real A2, not an either/or native engine fixture. */
class JointCacheTest {
    @TempDir Path directory;
    static Properties properties(String a1, String a2) {
        Properties p = IntegratedCacheTest.properties(a2); p.remove("protocol");
        p.setProperty("a1.method", a1); p.setProperty("a2.method", a2);
        p.setProperty("replica.port", "27801"); p.setProperty("request.timeout.ms", "5000");
        return p;
    }
    static Stream<Arguments> pairs() {
        return IntegrationConfig.A1.stream().sorted().flatMap(a -> IntegrationConfig.A2.stream().sorted().flatMap(b -> Stream.of("STOP", "PRE", "POST").flatMap(copy -> (b.equals("V06") ? Stream.of("ADAPTIVE","ROUND_ROBIN") : Stream.of("ADAPTIVE")).map(read -> Arguments.of(a,b,copy,read)))));
    }
    private static Result send(IntegratedCache cache, Command command, long id) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
        Result result;
        do { result = cache.execute(new IntegrationWire.Request(command, 1, id, 0, 5000), 0); if (result.status() == ResultStatus.BUSY) Thread.sleep(20); }
        while (result.status() == ResultStatus.BUSY && System.nanoTime() < until);
        return result;
    }
    private static void ready(IntegratedCache cache) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!cache.ready() && System.nanoTime() < until) Thread.sleep(30);
        assertTrue(cache.ready(), cache.status());
    }
    private static void drained(IntegratedCache cache) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (!cache.status().contains("\"pendingJobs\":0") && System.nanoTime() < until) Thread.sleep(5);
        assertTrue(cache.status().contains("\"pendingJobs\":0"), cache.status());
        assertEquals(0, ReplicaDiagnostics.number(cache.status(), "unresolvedMigrations"), cache.status());
    }
    @ParameterizedTest(name="{0}+{1}/{2}/{3}") @MethodSource("pairs")
    void everyA1A2PairReallyProcessesDataAndPublishesB3Ownership(String a1, String a2, String copy, String readSelection) throws Exception {
        Properties p = properties(a1, a2);
        p.setProperty("copy", copy); p.setProperty("read.selection", readSelection);
        var cfg = new IntegrationConfig(p); List<AutoCloseable> replicas = new ArrayList<>();
        try (var journal = new JsonLine(directory.resolve("joint.jsonl"))) {
            for (int g = 0; g < 2; g++) for (int n = 0; n < 3; n++) {
                replicas.add(new A1Replica(cfg.layer(true), g, n, directory.resolve("a1g"+g+"n"+n)));
                replicas.add(IoniaProfile.replica(cfg.layer(false), g, n, directory.resolve("a2g"+g+"n"+n)));
            }
            try (var cache = new IntegratedCache(cfg, List.of(ReplicatedBackend.connect(cfg, 0), ReplicatedBackend.connect(cfg, 1)), journal)) {
                ready(cache); Tablet moving = cache.table().ownedBy(new NodeId(0)).getFirst(); Bytes key = null;
                for (int i = 0; key == null; i++) { Bytes k = Bytes.utf8("key"+i); if (moving.range().contains(cfg.scheme().keyHash(k))) key = k; }
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(key, Bytes.utf8("value")), 1).status());
                // PB has no native DELETE, but A1 PB + A2 data DOES support DELETE.
                assertEquals(ResultStatus.OK, send(cache, new Command.Delete(key), 2).status());
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(key, Bytes.utf8("moved")), 3).status());
                cache.move(moving.id().value(), 1); drained(cache);
                assertEquals(Optional.of(Bytes.utf8("moved")), send(cache, new Command.Get(key), 4).value());
                try (var control = new ReplicatedBackend.Native(cfg.layer(true), 0)) {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    assertEquals(ResultStatus.MISS, control.execute(new Command.Get(key), 91, 1, deadline).status(), "A1 is not a second user-value store");
                    Result snapshot = control.execute(new Command.Get(JointBackend.ROUTING), 91, 2, deadline);
                    assertEquals(Optional.of(Bytes.copyOf(io.openkache.core.tablet.routing.TableMessageCodec.encodeTable(cache.table()))), snapshot.value());
                    assertEquals(32, control.execute(new Command.Get(JointBackend.ADMISSION), 91, 3, deadline).value().orElseThrow().size());
                }
                assertTrue(cache.status().contains("\"jointCache\":true"));
                assertTrue(ReplicaDiagnostics.number(cache.status(), "authorityAdmissions") > 0);
                assertTrue(ReplicaDiagnostics.number(cache.status(), "dataCalls") > 0);
                assertTrue(ReplicaDiagnostics.number(cache.status(), "routingPublications") >= 2);
            }
        } finally { Collections.reverse(replicas); for (var r : replicas) r.close(); }
    }
    static Stream<Arguments> failures() { return Stream.of(Arguments.of("A1", "VR"), Arguments.of("A1", "PB"), Arguments.of("A1", "PB_FENCED"), Arguments.of("A2", "VR")); }
    @ParameterizedTest @MethodSource("failures")
    void separateLayerLeaderFailureRecoversWhileOtherLayerIsStillAlive(String layer, String a1) throws Exception {
        Properties p = properties(a1, "O00"); p.setProperty("failure.detector", "SWIM"); p.setProperty("failure.grace.ms", "100");
        var cfg = new IntegrationConfig(p); var replicas = new ArrayList<AutoCloseable>();
        AutoCloseable failed = null;
        try (var journal = new JsonLine(directory.resolve("failover.jsonl"))) {
            for (int g = 0; g < 2; g++) for (int n = 0; n < 3; n++) for (boolean authority : List.of(true, false)) {
                var node = IntegrationReplicaMain.startNative(cfg.layer(authority), g, n, directory.resolve("g"+g+"n"+n+authority), false);
                replicas.add(node); if (g == 0 && n == 0 && authority == layer.equals("A1")) failed = node;
            }
            try (var cache = new IntegratedCache(cfg, List.of(ReplicatedBackend.connect(cfg, 0), ReplicatedBackend.connect(cfg, 1)), journal)) {
                ready(cache); Bytes key = null; for (int i = 0; key == null; i++) { Bytes k=Bytes.utf8("failure"+i); if (cache.table().lookup(cfg.scheme().keyHash(k)).owner().value()==0) key=k; }
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(key, Bytes.utf8("before")), 1).status());
                failed.close(); Thread.sleep(2500);
                assertEquals(Optional.of(Bytes.utf8("before")), send(cache, new Command.Get(key), 2).value());
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(key, Bytes.utf8("after")), 3).status());
                assertEquals(Optional.of(Bytes.utf8("after")), send(cache, new Command.Get(key), 4).value());
                String survivor = ReplicaDiagnostics.request(cfg.layer(layer.equals("A1")), 0, 1, null);
                assertTrue(ReplicaDiagnostics.number(survivor, layer.equals("A1") ? "epoch" : "view") > 0, survivor);
            }
        } finally { Collections.reverse(replicas); for (var r : replicas) r.close(); }
    }
    static final class Store implements ReplicatedBackend {
        final Map<Bytes, Bytes> values = new HashMap<>(); int calls; boolean fail;
        @Override public Result execute(Command c, long client, long operation, long deadline) { return apply(c); }
        @Override public Result administrative(Command c) { return apply(c); }
        Result apply(Command c) { calls++; if (fail) return ReplicatedBackend.status(ResultStatus.UNAVAILABLE);
            if (c instanceof Command.Set s) { values.put(s.key(), s.value()); return ReplicatedBackend.status(ResultStatus.OK); }
            if (c instanceof Command.Delete d) { values.remove(d.key()); return ReplicatedBackend.status(ResultStatus.OK); }
            Bytes v = values.get(((Command.Get)c).key()); return v == null ? ReplicatedBackend.status(ResultStatus.MISS) : Result.ok(v, 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
        @Override public boolean supportsDelete() { return true; } @Override public void close() { }
    }
    @Test void lostA1AuthorityNeverSendsDataAndReturnsSafeRetry() throws Exception {
        var cfg = new IntegrationConfig(properties("RAFT", "O00")); var authority = new Store(); var data = new Store();
        try (var joint = new JointBackend(cfg, authority, data)) {
            authority.fail = true;
            assertEquals(ResultStatus.BUSY, joint.execute(new Command.Set(Bytes.utf8("k"), Bytes.utf8("v")), 1, 1, System.nanoTime()+TimeUnit.SECONDS.toNanos(2)).status());
            assertEquals(0, data.calls); assertTrue(data.values.isEmpty());
        }
    }
    @Test void unconfirmedRoutingCommitStopsAllRequestsAndDoesNotExposeNewOwner() throws Exception {
        var cfg = new IntegrationConfig(properties("RAFT", "O00")); var authority0 = new Store(); var data0 = new Store(); var authority1 = new Store(); var data1 = new Store();
        try (var journal = new JsonLine(directory.resolve("hold.jsonl")); var cache = new IntegratedCache(cfg, List.of(new JointBackend(cfg, authority0, data0), new JointBackend(cfg, authority1, data1)), journal)) {
            ready(cache); var t = cache.table().ownedBy(new NodeId(0)).getFirst(); authority0.fail = true;
            cache.move(t.id().value(), 1); long until = System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while (!cache.status().contains("\"routingUncertain\":true") && System.nanoTime()<until) Thread.sleep(5);
            assertTrue(cache.status().contains("\"routingUncertain\":true"), cache.status());
            assertEquals(0, cache.table().get(t.id()).owner().value());
            int calls = data0.calls + data1.calls;
            assertEquals(ResultStatus.UNAVAILABLE, send(cache, new Command.Set(Bytes.utf8("held"), Bytes.utf8("v")), 1).status());
            assertEquals(calls, data0.calls + data1.calls);
        }
    }
    @Test void rejectsAccidentalEitherOrConfiguration() {
        var p = properties("RAFT", "V07"); p.setProperty("protocol", "O00");
        assertThrows(IllegalArgumentException.class, () -> new IntegrationConfig(p));
    }
    @Test void internalMigrationRpcWaitsForTemporaryAuthorityRecovery() throws Exception {
        var cfg = new IntegrationConfig(properties("RAFT", "O00")); var data = new Store(); var storage = new Store();
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        ReplicatedBackend recovering = new ReplicatedBackend() {
            @Override public Result execute(Command c, long client, long operation, long deadline) { return storage.apply(c); }
            @Override public Result administrative(Command c) {
                return attempts.incrementAndGet() <= 2 ? ReplicatedBackend.status(ResultStatus.UNAVAILABLE) : storage.apply(c);
            }
            @Override public boolean supportsDelete() { return false; } @Override public void close() { }
        };
        try (var joint = new JointBackend(cfg, recovering, data)) {
            assertEquals(ResultStatus.OK, joint.administrative(new Command.Set(Bytes.utf8("key"), Bytes.utf8("value"))).status());
            assertEquals(1, data.calls, "only the admitted attempt reaches A2");
            assertTrue(attempts.get() >= 4);
        }
    }
    @Test void internalRpcDoesNotReplayAnAmbiguousA2Write() throws Exception {
        var cfg = new IntegrationConfig(properties("RAFT", "O00")); var authority = new Store();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        ReplicatedBackend recordedButBusy = new ReplicatedBackend() {
            @Override public Result execute(Command c, long client, long operation, long deadline) { calls.incrementAndGet(); return ReplicatedBackend.status(ResultStatus.BUSY); }
            @Override public boolean supportsDelete() { return true; } @Override public void close() { }
        };
        try (var joint = new JointBackend(cfg, authority, recordedButBusy)) {
            assertEquals(ResultStatus.TIMEOUT, joint.administrative(new Command.Set(Bytes.utf8("key"), Bytes.utf8("value"))).status());
            assertEquals(1, calls.get());
        }
    }
}
