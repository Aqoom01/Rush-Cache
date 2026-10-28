package io.openkache.server.ionia_fourth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.server.ionia.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_fourth.*;
import java.io.IOException;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
class IoniaFourthProfileTest {
    @TempDir Path directory;
    private static final List<NodeId> IDS = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
    private static Map<NodeId, InetSocketAddress> addresses() throws IOException {
        List<ServerSocket> held = new ArrayList<>(); Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        try { for (NodeId id : IDS) { ServerSocket socket = new ServerSocket(0); held.add(socket); addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort())); } }
        finally { for (ServerSocket socket : held) socket.close(); }
        return addresses;
    }
    private static IoniaConfig membership() { return new IoniaConfig(IDS.getFirst(), IDS.getFirst(), IDS, 0); }
    @Test void V03_16_differentTargetDirectoryIsRejectedWithoutChangingStoredFiles() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaFourthNode node = new IoniaFourthNode(membership(), addresses, directory, 2, 5000, new IoniaBatchSettings(2,65536,20_000_000))) {
            assertEquals(2, node.engine().batchDiagnostics().settings().maxEntries());
        }
        Map<Path, byte[]> before = snapshot();
        for(var wrong:List.of(new IoniaBatchSettings(3,65536,20_000_000),new IoniaBatchSettings(2,65537,20_000_000),new IoniaBatchSettings(2,65536,40_000_000)))
            assertThrows(IOException.class, () -> new IoniaFourthNode(membership(), addresses, directory, 2, 5000, wrong));
        Map<Path, byte[]> after = snapshot(); assertEquals(before.keySet(), after.keySet());
        for (Path path : before.keySet()) assertArrayEquals(before.get(path), after.get(path), path.toString());
    }
    private Map<Path, byte[]> snapshot() throws IOException {
        Map<Path, byte[]> result = new HashMap<>();
        try (var files = Files.walk(directory)) { for (Path path : files.filter(Files::isRegularFile).toList()) result.put(path, Files.readAllBytes(path)); }
        return result;
    }
    @Test void V03_16_tcpRejectsO00AndOtherBudgetButAcceptsMatchingProfile() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaFourthNode node = new IoniaFourthNode(membership(), addresses, directory, 2, 5000, new IoniaBatchSettings(2,65536,20_000_000))) {
            assertEquals(0, node.engine().progress().lastLogIndex());
            for (String wrong : List.of(membership().configurationId(), new IoniaBatchSettings(3,65536,20_000_000).configurationId(membership()),
                    new IoniaBatchSettings(2,65537,20_000_000).configurationId(membership()),
                    new IoniaBatchSettings(2,65536,40_000_000).configurationId(membership())))
                try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), wrong, addresses, 1000)) {
                    assertThrows(ExecutionException.class, () -> transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS));
                }
            try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), new IoniaBatchSettings(2,65536,20_000_000).configurationId(membership()), addresses, 1000)) {
                assertEquals(IoniaEngine.Status.NORMAL, transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS).status());
            }
        }
    }
    @Test void V03_02_mainRejectsMissingOrInvalidTargetBeforeOpeningData() throws Exception {
        for (String n : List.of("missing", "-1", "2147483648")) {
            Path config = directory.resolve(n + ".properties");
            Files.writeString(config, "data.directory=" + directory.resolve("unused").toString().replace('\\', '/') + "\n" + (n.equals("missing") ? "" : "prepare.max.entries=" + n + "\nprepare.max.encoded.bytes=65536\nprepare.max.delay.ms=20\n"));
            assertThrows(IllegalArgumentException.class, () -> IoniaFourthMain.main(new String[]{config.toString()}));
            assertFalse(Files.exists(directory.resolve("unused")));
        }
    }
}
