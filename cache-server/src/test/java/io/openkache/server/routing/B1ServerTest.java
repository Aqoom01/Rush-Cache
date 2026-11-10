package io.openkache.server.routing;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.routing.TableMessageCodec;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 2노드 실제 TCP 검증. node 0(Controller 겸임)과 node 1을 loopback 별칭(127.0.0.1/127.0.0.2)에
 * 올리고, 라우팅 판정·MOVED redirect·표 이동 전파를 raw socket 클라이언트로 확인한다.
 */
class B1ServerTest {

    record Reply(int status, long routingVersion, long epoch, String detail, byte[] value, byte[] tail) {}

    static int port() throws IOException {
        try (var s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static Reply request(String host, int port, int messageClass, int opcode, long routingVersion, byte[] body)
            throws IOException {
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            socket.setSoTimeout(3000);
            var out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(24 + body.length);
            out.writeShort(0x4f4b); out.writeByte(1); out.writeByte(messageClass); out.writeByte(opcode);
            out.writeByte(0); out.writeShort(0); out.writeLong(1); out.writeLong(routingVersion);
            out.write(body); out.flush();
            var in = new DataInputStream(socket.getInputStream());
            int length = in.readInt();
            byte[] frame = in.readNBytes(length);
            var b = ByteBuffer.wrap(frame);
            b.position(6);
            int status = Short.toUnsignedInt(b.getShort());
            b.getLong(); // requestId
            long serverVersion = b.getLong();
            long epoch = b.getLong();
            b.getLong(); // valueVersion
            byte[] detail = new byte[b.getInt()]; b.get(detail);
            byte[] value = new byte[b.getInt()]; b.get(value);
            byte[] tail = new byte[b.remaining()]; b.get(tail);
            return new Reply(status, serverVersion, epoch, new String(detail, StandardCharsets.UTF_8), value, tail);
        }
    }

    static byte[] command(long operationId, String key, String value) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(32 + k.length + (v == null ? 0 : 4 + v.length));
        b.putLong(7).putLong(operationId).putLong(0).putInt(1000).putInt(k.length).put(k);
        if (v != null) b.putInt(v.length).put(v);
        return b.array();
    }

    static byte[] moveControl(long expectedVersion, long tabletId, long from, long to) {
        return ByteBuffer.allocate(33).put((byte) 0).putLong(expectedVersion)
                .putLong(tabletId).putLong(from).putLong(to).array();
    }

    static byte[] splitControl(long expectedVersion, long tabletId, long at) {
        return ByteBuffer.allocate(25).put((byte) 1).putLong(expectedVersion)
                .putLong(tabletId).putLong(at).array();
    }

    /** §6-1 redirect: hostLength(u32) + host + port(u16). hostLength 0이면 주소 없음. */
    record Redirect(String host, int port) {
        static Redirect parse(byte[] tail) {
            ByteBuffer b = ByteBuffer.wrap(tail);
            int hostLength = b.getInt();
            if (hostLength == 0) return null;
            byte[] host = new byte[hostLength];
            b.get(host);
            int port = Short.toUnsignedInt(b.getShort());
            return new Redirect(new String(host, StandardCharsets.UTF_8), port);
        }
    }

    static String keyOwnedBy(RoutingTable table, NodeId owner) {
        for (int i = 0; i < 100_000; i++) {
            String key = "key-" + i;
            if (table.lookup(KeyHash.of(Bytes.utf8(key))).owner().equals(owner)) return key;
        }
        throw new AssertionError("no key routed to " + owner);
    }

    static String keyInTablet(RoutingTable table, long tabletId) {
        for (int i = 0; i < 100_000; i++) {
            String key = "key-" + i;
            if (table.lookup(KeyHash.of(Bytes.utf8(key))).id().value() == tabletId) return key;
        }
        throw new AssertionError("no key in tablet " + tabletId);
    }

    @Test
    void routesMovedRedirectsAndPropagatesTableChanges() throws Exception {
        int client = port(), peer = port();
        while (client == peer) peer = port();
        List<String> hosts = List.of("127.0.0.1", "127.0.0.2");
        RoutingTable initial = new HashRangeScheme(2).initial(List.of(new NodeId(0), new NodeId(1)));
        List<B1Server> nodes = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                nodes.add(B1Server.start(new B1NodeConfig(i, hosts, client, peer, 2)));
            }

            // 1) 주인 노드에서 SET/GET이 성공한다
            String ownedBy0 = keyOwnedBy(initial, new NodeId(0));
            Reply set = request(hosts.get(0), client, 1, 2, 0, command(1, ownedBy0, "value-0"));
            assertEquals(0, set.status());
            assertEquals(1, set.routingVersion());
            Reply get = request(hosts.get(0), client, 1, 1, 0, command(2, ownedBy0, null));
            assertEquals(0, get.status());
            assertArrayEquals("value-0".getBytes(StandardCharsets.UTF_8), get.value());

            // 2) 다른 노드로 보내면 MOVED와 §6-1 redirect가 돌아온다
            Reply moved = request(hosts.get(1), client, 1, 2, 0, command(3, ownedBy0, "x"));
            assertEquals(3, moved.status());
            Redirect redirect = Redirect.parse(moved.tail());
            assertNotNull(redirect);
            assertEquals(hosts.get(0), redirect.host());
            assertEquals(client, redirect.port());

            // 3) CONTROL로 Tablet 0을 node0 -> node1 이동하면 push로 두 노드 표가 같아진다
            Reply control = request(hosts.get(0), peer, 3, B1Wire.CONTROL, 0, moveControl(1, 0, 0, 1));
            assertEquals(0, control.status());
            assertEquals(2, control.routingVersion());

            RoutingTable pushed = awaitVersion(hosts.get(1), peer, 2);
            assertEquals(new NodeId(1), pushed.get(new io.openkache.core.model.TabletId(0)).owner());

            // 4) 옮겨진 Tablet의 key는 node0에서 MOVED(새 주인 node1), node1에서 OK
            String movedKey = keyInTablet(initial, 0);
            Reply fromOld = request(hosts.get(0), client, 1, 2, 0, command(4, movedKey, "after-move"));
            assertEquals(3, fromOld.status());
            Redirect toNew = Redirect.parse(fromOld.tail());
            assertNotNull(toNew);
            assertEquals(hosts.get(1), toNew.host());
            Reply atNew = request(hosts.get(1), client, 1, 2, 0, command(5, movedKey, "after-move"));
            assertEquals(0, atNew.status());
            assertEquals(2, atNew.epoch());

            // 5) 낡은 expectedVersion의 CONTROL은 UNAVAILABLE로 거절된다
            Reply stale = request(hosts.get(0), peer, 3, B1Wire.CONTROL, 0, moveControl(1, 1, 0, 1));
            assertEquals(8, stale.status());

            // 6) SPLIT: node1 소유 Tablet 2를 가운데서 쪼개면 표가 5줄이 된다
            RoutingTable current = awaitVersion(hosts.get(0), peer, 2);
            Tablet target = current.get(new io.openkache.core.model.TabletId(2));
            long at = target.range().start() + 1;
            Reply split = request(hosts.get(0), peer, 3, B1Wire.CONTROL, 0, splitControl(2, 2, at));
            assertEquals(0, split.status());
            RoutingTable afterSplit = awaitVersion(hosts.get(1), peer, 3);
            assertEquals(5, afterSplit.size());
        } finally {
            for (var node : nodes) node.close();
        }
    }

    /** TABLE_DUMP를 반복 조회해 표 버전이 기대값 이상이 될 때까지 기다린다. */
    static RoutingTable awaitVersion(String host, int peerPort, long version) throws Exception {
        long limit = System.nanoTime() + 3_000_000_000L;
        RoutingTable last = null;
        while (System.nanoTime() < limit) {
            Reply dump = request(host, peerPort, 3, B1Wire.TABLE_DUMP, 0, new byte[0]);
            assertEquals(0, dump.status());
            last = TableMessageCodec.decodeTable(dump.value());
            if (last.version().value() >= version) return last;
            Thread.sleep(10);
        }
        fail("table version did not reach " + version + ": " + (last == null ? "none" : last.version()));
        return null;
    }
}
