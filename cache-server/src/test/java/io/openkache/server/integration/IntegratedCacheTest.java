package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.tablet.Tablet;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

class IntegratedCacheTest {
    @TempDir Path directory;
    static Stream<String> protocols() { return IntegrationConfig.PROTOCOLS.stream().sorted(); }
    static Properties properties(String protocol) {
        Properties p = new Properties();
        p.setProperty("run.id", "test" + UUID.randomUUID().toString().replace("-", "")); p.setProperty("protocol", protocol);
        p.setProperty("group.0.hosts", "127.0.0.11,127.0.0.12,127.0.0.13"); p.setProperty("group.1.hosts", "127.0.0.14,127.0.0.15,127.0.0.16");
        p.setProperty("replica.port", "27401"); p.setProperty("gateway.port", "27410"); p.setProperty("failure.timeout.ms", "1000");
        p.setProperty("migration.tick.ms", "1"); p.setProperty("migration.timeout.ms", "20000"); p.setProperty("tablets.per.group", "2");
        return p;
    }
    private static Result send(IntegratedCache cache, Command c, long operation) throws InterruptedException {
        return cache.execute(new IntegrationWire.Request(c, 1, operation, 0, 5000), 0);
    }
    private static Bytes movingKey(IntegrationConfig config, Tablet tablet, int skip) {
        for (int i = skip; i < 100000; i++) { Bytes key = Bytes.utf8("key-" + i); if (tablet.range().contains(config.scheme().keyHash(key))) return key; }
        throw new AssertionError("no key");
    }
    private static void awaitDone(IntegratedCache cache) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (cache.status().contains("\"pendingJobs\":0") == false && System.nanoTime() < until) Thread.sleep(10);
        assertTrue(cache.status().contains("\"pendingJobs\":0"), cache.status());
    }
    @ParameterizedTest @MethodSource("protocols")
    void realTcpFileReplicationMovesAndDeduplicatesAcrossOwners(String protocol) throws Exception {
        Properties p = properties(protocol); p.setProperty("copy", "PRE"); IntegrationConfig config = new IntegrationConfig(p);
        List<AutoCloseable> replicas = new ArrayList<>();
        try (var journal = new JsonLine(directory.resolve("journal.jsonl"))) {
            for (int g = 0; g < 2; g++) for (int n = 0; n < 3; n++) replicas.add(config.ionia()
                    ? IoniaProfile.replica(config, g, n, directory.resolve("g" + g + "n" + n))
                    : new A1Replica(config, g, n, directory.resolve("g" + g + "n" + n)));
            try (var cache = new IntegratedCache(config, List.of(new ReplicatedBackend.Native(config, 0), new ReplicatedBackend.Native(config, 1)), journal)) {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
                while (!cache.ready() && System.nanoTime() < until) Thread.sleep(50);
                assertTrue(cache.ready());
                Tablet t = cache.table().ownedBy(new NodeId(0)).getFirst(); Bytes key = movingKey(config, t, 0);
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(key, Bytes.utf8("before")), 1).status());
                assertEquals(Optional.of(Bytes.utf8("before")), send(cache, new Command.Get(key), 2).value());
                cache.move(t.id().value(), 1); awaitDone(cache);
                assertTrue(cache.status().contains("\"state\":\"DONE\""), cache.status());
                assertEquals(1, cache.table().get(t.id()).owner().value());
                assertEquals(Optional.of(Bytes.utf8("before")), send(cache, new Command.Get(key), 3).value());
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(key, Bytes.utf8("after")), 4).status());
                // 소유 그룹이 바뀌었어도 과거 성공 쓰기는 재적용되지 않는다.
                assertEquals(ResultStatus.OK, send(cache, new Command.Set(key, Bytes.utf8("before")), 1).status());
                assertEquals(Optional.of(Bytes.utf8("after")), send(cache, new Command.Get(key), 5).value());
                assertEquals(ResultStatus.BAD_REQUEST, send(cache, new Command.Set(key, Bytes.utf8("wrong-payload")), 1).status());
                Result stale = cache.execute(new IntegrationWire.Request(new Command.Get(key), 1, 6, t.epoch().value(), 5000), 0);
                assertEquals(ResultStatus.STALE_EPOCH, stale.status());
                if (!protocol.startsWith("PB")) {
                    assertEquals(ResultStatus.OK, send(cache, new Command.Delete(key), 7).status());
                    cache.move(t.id().value(), 0); awaitDone(cache);
                    assertEquals(0, cache.table().get(t.id()).owner().value());
                    assertEquals(ResultStatus.MISS, send(cache, new Command.Get(key), 8).status());
                }
            }
        } finally { Collections.reverse(replicas); for (var node : replicas) node.close(); }
    }
    @Test void stopCopyUsesRealBackendAndWaitsForDestinationReadBarrier() throws Exception {
        Properties p = properties("O00"); p.setProperty("copy", "STOP"); IntegrationConfig c = new IntegrationConfig(p);
        Fake source = new Fake(), target = new Fake(); target.blockRead = new CountDownLatch(1);
        try (var journal = new JsonLine(directory.resolve("barrier.jsonl")); var cache = new IntegratedCache(c, List.of(source, target), journal)) {
            Tablet t = cache.table().ownedBy(new NodeId(0)).getFirst(); Bytes key = movingKey(c, t, 0);
            send(cache, new Command.Set(key, Bytes.utf8("v")), 1);
            cache.move(t.id().value(), 1);
            assertTrue(target.bootstrap.await(5, TimeUnit.SECONDS));
            assertEquals(0, cache.table().get(t.id()).owner().value(), "Fast ACK alone cannot publish routing");
            target.blockRead.countDown(); awaitDone(cache);
            assertEquals(1, cache.table().get(t.id()).owner().value());
            assertEquals(Optional.of(Bytes.utf8("v")), send(cache, new Command.Get(key), 2).value());
        }
    }
    @Test void ambiguousWriteHoldsMigrationAndPreservesOwnership() throws Exception {
        var c = new IntegrationConfig(properties("O00")); Fake source = new Fake(), target = new Fake(); source.failWrites = true;
        try (var journal = new JsonLine(directory.resolve("uncertain.jsonl")); var cache = new IntegratedCache(c, List.of(source, target), journal)) {
            Tablet t = cache.table().ownedBy(new NodeId(0)).getFirst(); Bytes key = movingKey(c, t, 0);
            assertEquals(ResultStatus.UNAVAILABLE, send(cache, new Command.Set(key, Bytes.utf8("maybe-applied")), 1).status());
            cache.move(t.id().value(), 1); awaitDone(cache);
            assertTrue(cache.status().contains("\"state\":\"HELD\""), cache.status());
            assertEquals(0, cache.table().get(t.id()).owner().value()); assertTrue(target.data.isEmpty());
        }
    }
    @Test void failedBootstrapQuarantinesRangeAndCannotPublishOnRetry() throws Exception {
        var c = new IntegrationConfig(properties("O00")); Fake source = new Fake(), target = new Fake(); target.failAdmin = true;
        try (var journal = new JsonLine(directory.resolve("failed-bootstrap.jsonl")); var cache = new IntegratedCache(c, List.of(source, target), journal)) {
            Tablet t = cache.table().ownedBy(new NodeId(0)).getFirst(); Bytes key = movingKey(c, t, 0);
            send(cache, new Command.Set(key, Bytes.utf8("v")), 1); cache.move(t.id().value(), 1); awaitDone(cache);
            assertEquals(0, cache.table().get(t.id()).owner().value()); assertTrue(cache.status().contains("\"state\":\"ABORTED\""));
            target.failAdmin = false; cache.move(t.id().value(), 1); awaitDone(cache);
            assertEquals(0, cache.table().get(t.id()).owner().value()); assertTrue(cache.status().contains("\"state\":\"HELD\""));
            assertEquals(Optional.of(Bytes.utf8("v")), send(cache, new Command.Get(key), 2).value());
        }
    }
    @Test void incompatibleAndUnknownSettingsFailBeforeStartingProcesses() {
        var p = properties("O00"); p.setProperty("distribution", "SLOT"); p.setProperty("split", "HEAT");
        assertThrows(IllegalArgumentException.class, () -> new IntegrationConfig(p));
        p.setProperty("split", "NONE"); p.setProperty("distribution", "RING"); p.setProperty("placement", "GREEDY");
        assertThrows(IllegalArgumentException.class, () -> new IntegrationConfig(p));
        p.setProperty("placement", "NONE"); p.setProperty("typo", "1"); assertThrows(IllegalArgumentException.class, () -> new IntegrationConfig(p));
    }
    @Test void ledgerDoesNotReapplyEvictedWriteIds() {
        WriteLedger ledger = new WriteLedger(1); Bytes key = Bytes.utf8("k");
        var one = ledger.admit(1, 1, new Command.Set(key, Bytes.utf8("one"))); one.entry().result().complete(ReplicatedBackend.status(ResultStatus.OK));
        assertTrue(ledger.admit(1, 2, new Command.Set(key, Bytes.utf8("two"))).fresh());
        assertEquals(ResultStatus.BAD_REQUEST, ledger.admit(1, 1, new Command.Set(key, Bytes.utf8("one"))).rejection().status());
    }
    private static final class Fake implements ReplicatedBackend {
        final Map<Bytes, Bytes> data = new ConcurrentHashMap<>();
        final CountDownLatch bootstrap = new CountDownLatch(1);
        volatile CountDownLatch blockRead;
        volatile boolean failWrites, failAdmin;
        @Override public Result execute(Command c, long client, long op, long deadline) {
            if (c instanceof Command.Set s) { data.put(s.key(), s.value()); return ReplicatedBackend.status(failWrites ? ResultStatus.UNAVAILABLE : ResultStatus.OK); }
            if (c instanceof Command.Delete d) return ReplicatedBackend.status(data.remove(d.key()) == null ? ResultStatus.MISS : ResultStatus.OK);
            Bytes value = data.get(((Command.Get) c).key()); return value == null ? ReplicatedBackend.status(ResultStatus.MISS) : Result.ok(value, 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
        @Override public Result administrative(Command c) throws InterruptedException {
            if (c instanceof Command.Set) { bootstrap.countDown(); if (failAdmin) return ReplicatedBackend.status(ResultStatus.UNAVAILABLE); }
            if (c instanceof Command.Get && blockRead != null) assertTrue(blockRead.await(5, TimeUnit.SECONDS));
            return execute(c, 9, 1, Long.MAX_VALUE);
        }
        @Override public boolean supportsDelete() { return true; }
        @Override public void close() { }
    }
}
