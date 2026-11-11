package io.openkache.server.routing;

import static io.openkache.server.routing.B1ServerTest.Redirect;
import static io.openkache.server.routing.B1ServerTest.Reply;
import static io.openkache.server.routing.B1ServerTest.awaitVersion;
import static io.openkache.server.routing.B1ServerTest.command;
import static io.openkache.server.routing.B1ServerTest.port;
import static io.openkache.server.routing.B1ServerTest.request;
import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

/**
 * 2노드 실제 TCP에서 B3 이동(stop-and-copy, pre-copy)과 cutover를 검증한다. 쓰기를 계속 넣으면서 Tablet을 옮기고, 끝난 뒤
 * 쓰기 순서대로 기록한 마지막 값(oracle)과 새 주인의 값을 key 단위로 대조한다.
 */
class B1MigrationTest {

    private static final List<String> HOSTS = List.of("127.0.0.1", "127.0.0.2");
    private static final int CLIENT_REQUEST = 1;
    private static final int PEER = 3;

    private int client;
    private int peer;

    private List<B1Server> start() throws Exception {
        client = port();
        peer = port();
        while (client == peer) peer = port();
        List<B1Server> nodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) nodes.add(B1Server.start(new B1NodeConfig(i, HOSTS, client, peer, 2)));
        return nodes;
    }

    static List<String> keysInTablet(RoutingTable table, long tabletId, int count) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; keys.size() < count; i++) {
            String key = "key-" + i;
            if (table.lookup(KeyHash.of(Bytes.utf8(key))).id().value() == tabletId) keys.add(key);
        }
        return keys;
    }

    /** MOVED는 redirect를 따라가고 BUSY는 1ms 뒤 다시 보낸다. 최종 응답을 돌려준다. */
    private Reply send(int opcode, String key, String value, AtomicInteger busy) throws Exception {
        String host = HOSTS.get(0);
        for (int attempt = 0; attempt < 2000; attempt++) {
            Reply r = request(host, client, CLIENT_REQUEST, opcode, 0, command(attempt, key, value));
            if (r.status() == 3) {
                Redirect redirect = Redirect.parse(r.tail());
                host = redirect == null ? HOSTS.get((HOSTS.indexOf(host) + 1) % 2) : redirect.host();
                Thread.sleep(1);
            } else if (r.status() == 6) {
                if (busy != null) busy.incrementAndGet();
                Thread.sleep(1);
            } else {
                return r;
            }
        }
        throw new AssertionError("request did not settle: " + key);
    }

    private static byte[] migrate(int method, long tablet, long to, long rate, long threshold, int maxRounds) {
        return ByteBuffer.allocate(41).put((byte) method).putLong(tablet).putLong(to).putLong(rate)
                .putInt(16 * 1024).putLong(threshold).putInt(maxRounds).array();
    }

    /** MIGRATE를 보내고 DONE/FAILED가 될 때까지 상태를 조회해 마지막 JSON을 돌려준다. */
    private String runMigration(int sourceNode, byte[] body) throws Exception {
        Reply started = request(HOSTS.get(sourceNode), peer, PEER, MigrationWire.MIGRATE, 0, body);
        assertEquals(0, started.status(), started.detail());
        long jobId = ByteBuffer.wrap(started.value()).getLong();
        long limit = System.nanoTime() + 20_000_000_000L;
        while (System.nanoTime() < limit) {
            Reply status = request(HOSTS.get(sourceNode), peer, PEER, MigrationWire.MIGRATE_STATUS, 0,
                    ByteBuffer.allocate(8).putLong(jobId).array());
            assertEquals(0, status.status(), status.detail());
            int phase = status.value()[0];
            if (phase == MigrationDriver.Phase.DONE.ordinal()) return status.detail();
            if (phase == MigrationDriver.Phase.FAILED.ordinal()) fail("migration failed: " + status.detail());
            Thread.sleep(5);
        }
        fail("migration did not finish");
        return null;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void movesTabletUnderWritesWithoutLosingOrResurrectingValues(int method) throws Exception {
        List<B1Server> nodes = start();
        try {
            RoutingTable initial = new HashRangeScheme(2).initial(List.of(new NodeId(0), new NodeId(1)));
            // node 0(Controller 겸임)이 source인 경우와 node 1이 source(원격 CONTROL commit)인 경우를 모두 확인한다
            moveUnderLoad(method, initial, 1, 0, 1, 2);
            moveUnderLoad(method, initial, 2, 1, 0, 3);
        } finally {
            for (B1Server n : nodes) n.close();
        }
    }

    private void moveUnderLoad(int method, RoutingTable initial, long tablet, int from, int to, long expectedVersion)
            throws Exception {
        List<String> keys = keysInTablet(initial, tablet, 400);
        String filler = "v".repeat(200);
        Map<String, String> oracle = new HashMap<>();
        for (String key : keys) {
            assertEquals(0, send(2, key, key + filler, null).status());
            oracle.put(key, key + filler);
        }

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger busy = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        List<Throwable> errors = new ArrayList<>();
        Thread writer = new Thread(() -> {
            Random random = new Random(tablet);
            try {
                while (!stop.get()) {
                    String key = keys.get(random.nextInt(keys.size()));
                    if (random.nextInt(10) < 3) {
                        int status = send(3, key, null, busy).status();
                        assertTrue(status == 0 || status == 1, "delete status " + status);
                        oracle.put(key, null);
                    } else {
                        String value = key + "-w" + writes.get();
                        assertEquals(0, send(2, key, value, busy).status());
                        oracle.put(key, value);
                    }
                    writes.incrementAndGet();
                }
            } catch (Throwable e) {
                errors.add(e);
            }
        });
        writer.start();
        Thread.sleep(50);
        // 대역 1 MB/s: 약 90KB Tablet을 수십 ms 동안 옮기며 그 사이 쓰기가 dirty로 쌓이게 한다
        String report = runMigration(from, migrate(method, tablet, to, 1_000_000, 4 * 1024, 5));
        Thread.sleep(50);
        stop.set(true);
        writer.join();
        assertTrue(errors.isEmpty(), () -> "writer failed: " + errors);
        assertTrue(writes.get() > 0);
        System.out.println("tablet " + tablet + " writes=" + writes.get() + " busy=" + busy.get() + " " + report);

        RoutingTable after = awaitVersion(HOSTS.get(to), peer, expectedVersion);
        assertEquals(new NodeId(to), after.get(new TabletId(tablet)).owner());
        assertTrue(report.contains(method == 0 ? "\"stop-and-copy\"" : "\"pre-copy\""), report);
        assertTrue(report.contains("\"phase\":\"DONE\""), report);

        for (String key : keys) {
            Reply atNew = request(HOSTS.get(to), client, CLIENT_REQUEST, 1, 0, command(1, key, null));
            String expected = oracle.get(key);
            if (expected == null) {
                assertEquals(1, atNew.status(), "deleted key resurrected: " + key);
            } else {
                assertEquals(0, atNew.status(), "lost key: " + key);
                assertEquals(expected, new String(atNew.value(), StandardCharsets.UTF_8));
            }
            assertEquals(3, request(HOSTS.get(from), client, CLIENT_REQUEST, 1, 0, command(1, key, null)).status());
        }
    }

    @Test
    void cutoverCleansSourceSoReturningTabletDoesNotResurrectOldValues() throws Exception {
        List<B1Server> nodes = start();
        try {
            RoutingTable initial = new HashRangeScheme(2).initial(List.of(new NodeId(0), new NodeId(1)));
            String key = keysInTablet(initial, 3, 1).get(0);
            assertEquals(0, send(2, key, "old", null).status());

            runMigration(1, migrate(2, 3, 0, 0, 0, 0));
            awaitVersion(HOSTS.get(0), peer, 2);
            // 복사가 없으므로 새 주인에는 값이 없다
            assertEquals(1, request(HOSTS.get(0), client, CLIENT_REQUEST, 1, 0, command(1, key, null)).status());

            runMigration(0, migrate(2, 3, 1, 0, 0, 0));
            awaitVersion(HOSTS.get(1), peer, 3);
            // 원래 노드로 돌아와도 옛 값이 되살아나지 않는다
            assertEquals(1, request(HOSTS.get(1), client, CLIENT_REQUEST, 1, 0, command(1, key, null)).status());
        } finally {
            for (B1Server n : nodes) n.close();
        }
    }

    @Test
    void controlMoveAlsoCleansTheOldOwner() throws Exception {
        List<B1Server> nodes = start();
        try {
            RoutingTable initial = new HashRangeScheme(2).initial(List.of(new NodeId(0), new NodeId(1)));
            String key = keysInTablet(initial, 0, 1).get(0);
            assertEquals(0, send(2, key, "old", null).status());
            assertEquals(0, request(HOSTS.get(0), peer, PEER, B1Wire.CONTROL, 0,
                    B1ServerTest.moveControl(1, 0, 0, 1)).status());
            awaitVersion(HOSTS.get(1), peer, 2);
            assertEquals(0, request(HOSTS.get(0), peer, PEER, B1Wire.CONTROL, 0,
                    B1ServerTest.moveControl(2, 0, 1, 0)).status());
            awaitVersion(HOSTS.get(1), peer, 3);
            assertEquals(1, request(HOSTS.get(0), client, CLIENT_REQUEST, 1, 0, command(1, key, null)).status());
        } finally {
            for (B1Server n : nodes) n.close();
        }
    }

    @Test
    void rejectsMigrationOfTabletOwnedElsewhere() throws Exception {
        List<B1Server> nodes = start();
        try {
            Reply r = request(HOSTS.get(0), peer, PEER, MigrationWire.MIGRATE, 0, migrate(1, 2, 1, 0, 0, 0));
            assertEquals(2, r.status());
        } finally {
            for (B1Server n : nodes) n.close();
        }
    }
}
