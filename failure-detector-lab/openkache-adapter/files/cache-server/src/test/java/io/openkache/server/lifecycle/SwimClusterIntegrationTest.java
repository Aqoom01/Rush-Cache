package io.openkache.server.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.failure.MemberStatus;
import io.openkache.core.failure.SwimConfig;
import io.openkache.core.model.NodeId;
import io.openkache.server.config.PeerAddress;
import io.openkache.server.config.ServerConfig;
import java.net.DatagramSocket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 세 서버를 loopback UDP로 연결해 SWIM direct/indirect probe와 장애 전이를 검증한다. */
class SwimClusterIntegrationTest {

    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(8);
    private final List<ServerApplication> applications = new ArrayList<>();

    @AfterEach
    void stopApplications() {
        applications.forEach(ServerApplication::close);
    }

    @Test
    void threeNodesExchangeSwimMessagesAndDetectStoppedNode() throws Exception {
        int[] ports = freePorts(9);
        Map<NodeId, PeerAddress> members = new LinkedHashMap<>();
        members.put(new NodeId(1), new PeerAddress("127.0.0.1", ports[1]));
        members.put(new NodeId(2), new PeerAddress("127.0.0.1", ports[4]));
        members.put(new NodeId(3), new PeerAddress("127.0.0.1", ports[7]));
        SwimConfig swimConfig = new SwimConfig(
                Duration.ofMillis(500),
                Duration.ofMillis(200),
                Duration.ofMillis(200),
                Duration.ofMillis(1_000),
                2,
                3,
                8);

        ServerApplication node1 = start(config(1, ports[0], ports[1], ports[2], members, swimConfig));
        ServerApplication node2 = start(config(2, ports[3], ports[4], ports[5], members, swimConfig));
        ServerApplication node3 = start(config(3, ports[6], ports[7], ports[8], members, swimConfig));

        // 초기 ALIVE 값만 확인하지 않도록 여러 probe 주기 동안 실제 TCP 교환을 기다린다.
        Thread.sleep(2_000);
        assertAllAlive(node1);
        assertAllAlive(node2);
        assertAllAlive(node3);

        node3.close();

        await(() -> status(node1, 3) == MemberStatus.DEAD, WAIT_TIMEOUT);
        await(() -> status(node2, 3) == MemberStatus.DEAD, WAIT_TIMEOUT);
        assertEquals(MemberStatus.ALIVE, status(node1, 2));
        assertEquals(MemberStatus.ALIVE, status(node2, 1));
    }

    private ServerApplication start(ServerConfig config) throws InterruptedException {
        ServerApplication application = ServerApplication.start(config);
        applications.add(application);
        return application;
    }

    private static ServerConfig config(
            long nodeId,
            int clientPort,
            int peerPort,
            int adminPort,
            Map<NodeId, PeerAddress> members,
            SwimConfig swimConfig) {
        return new ServerConfig(
                new NodeId(nodeId),
                "swim-integration",
                "127.0.0.1",
                "127.0.0.1",
                clientPort,
                peerPort,
                adminPort,
                1,
                Duration.ofSeconds(2),
                members,
                swimConfig);
    }

    private static void assertAllAlive(ServerApplication application) throws Exception {
        assertTrue(application.membershipSnapshot().toCompletableFuture().get(1, TimeUnit.SECONDS).values().stream()
                .allMatch(member -> member.status() == MemberStatus.ALIVE));
    }

    private static MemberStatus status(ServerApplication application, long nodeId) {
        try {
            return application.membershipSnapshot()
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS)
                    .get(new NodeId(nodeId))
                    .status();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met within " + timeout);
    }

    private static int[] freePorts(int count) throws IOException {
        List<AutoCloseable> sockets = new ArrayList<>();
        try {
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                ServerSocket tcp = new ServerSocket(0);
                sockets.add(tcp);
                DatagramSocket udp = new DatagramSocket(null);
                udp.bind(new InetSocketAddress("127.0.0.1", tcp.getLocalPort()));
                sockets.add(udp);
                ports[i] = tcp.getLocalPort();
            }
            return ports;
        } finally {
            for (AutoCloseable socket : sockets) {
                try {
                    socket.close();
                } catch (Exception e) {
                    throw new IOException("failed to release test port", e);
                }
            }
        }
    }
}
