package io.openkache.server.ionia_third;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.server.ionia.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_third.*;
import java.io.IOException;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
class IoniaThirdProfileTest {
    @TempDir Path directory;
    private static final List<NodeId> IDS = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
    private static Map<NodeId, InetSocketAddress> addresses() throws IOException {
        List<ServerSocket> held = new ArrayList<>(); Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        try { for (NodeId id : IDS) { ServerSocket socket = new ServerSocket(0); held.add(socket); addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort())); } }
        finally { for (ServerSocket socket : held) socket.close(); }
        return addresses;
    }
    private static IoniaConfig membership() { return new IoniaConfig(IDS.getFirst(), IDS.getFirst(), IDS, 0); }
    @Test void V02_16_differentTargetDirectoryIsRejectedWithoutChangingStoredFiles() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaThirdNode node = new IoniaThirdNode(membership(), addresses, directory, 2, 5000, new IoniaHistorySettings(1))) {
            assertEquals(1, node.engine().historyDiagnostics().history().targetEntries());
        }
        Map<Path, byte[]> before = snapshot();
        assertThrows(IOException.class, () -> new IoniaThirdNode(membership(), addresses, directory, 2, 5000, new IoniaHistorySettings(2)));
        Map<Path, byte[]> after = snapshot(); assertEquals(before.keySet(), after.keySet());
        for (Path path : before.keySet()) assertArrayEquals(before.get(path), after.get(path), path.toString());
    }
    private Map<Path, byte[]> snapshot() throws IOException {
        Map<Path, byte[]> result = new HashMap<>();
        try (var files = Files.walk(directory)) { for (Path path : files.filter(Files::isRegularFile).toList()) result.put(path, Files.readAllBytes(path)); }
        return result;
    }
    @Test void V02_16_tcpRejectsO00AndOtherBudgetButAcceptsMatchingProfile() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaThirdNode node = new IoniaThirdNode(membership(), addresses, directory, 2, 5000, new IoniaHistorySettings(1))) {
            assertEquals(0, node.engine().progress().lastLogIndex());
            for (String wrong : List.of(membership().configurationId(), new IoniaHistorySettings(2).configurationId(membership())))
                try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), wrong, addresses, 1000)) {
                    assertThrows(ExecutionException.class, () -> transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS));
                }
            try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), new IoniaHistorySettings(1).configurationId(membership()), addresses, 1000)) {
                assertEquals(IoniaEngine.Status.NORMAL, transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS).status());
            }
        }
    }
    @Test void V02_02_mainRejectsMissingOrInvalidTargetBeforeOpeningData() throws Exception {
        for (String n : List.of("missing", "-1", "2147483648")) {
            Path config = directory.resolve(n + ".properties");
            Files.writeString(config, "data.directory=" + directory.resolve("unused").toString().replace('\\', '/') + "\n" + (n.equals("missing") ? "" : "history.target.entries=" + n + "\n"));
            assertThrows(IllegalArgumentException.class, () -> IoniaThirdMain.main(new String[]{config.toString()}));
            assertFalse(Files.exists(directory.resolve("unused")));
        }
    }
}
