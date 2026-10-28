package io.openkache.server.ionia_fifth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.server.ionia.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_fifth.*;
import java.io.IOException;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
class IoniaFifthProfileTest {
    @TempDir Path directory;
    private static final List<NodeId> IDS = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
    private static Map<NodeId, InetSocketAddress> addresses() throws IOException {
        List<ServerSocket> held = new ArrayList<>(); Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        try { for (NodeId id : IDS) { ServerSocket socket = new ServerSocket(0); held.add(socket); addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort())); } }
        finally { for (ServerSocket socket : held) socket.close(); }
        return addresses;
    }
    private static IoniaConfig membership() { return new IoniaConfig(IDS.getFirst(), IDS.getFirst(), IDS, 0); }
    @Test void V04_08_differentConcurrencyDirectoryIsRejectedWithoutChangingStoredFiles() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaFifthNode node = new IoniaFifthNode(membership(), addresses, directory, 2, 5000, new IoniaApplySettings(1,2))) {
            assertEquals(1,node.engine().applyDiagnostics().settings().maxInFlightKeys());
        }
        Map<Path, byte[]> before = snapshot();
        assertThrows(IOException.class, () -> new IoniaFifthNode(membership(), addresses, directory, 2, 5000, new IoniaApplySettings(2,2)));
        Map<Path, byte[]> after = snapshot(); assertEquals(before.keySet(), after.keySet());
        for (Path path : before.keySet()) assertArrayEquals(before.get(path), after.get(path), path.toString());
    }
    private Map<Path, byte[]> snapshot() throws IOException {
        Map<Path, byte[]> result = new HashMap<>();
        try (var files = Files.walk(directory)) { for (Path path : files.filter(Files::isRegularFile).toList()) result.put(path, Files.readAllBytes(path)); }
        return result;
    }
    @Test void V04_08_tcpRejectsO00AndOtherConcurrencyButAcceptsMatchingProfile() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaFifthNode node = new IoniaFifthNode(membership(), addresses, directory, 2, 5000, new IoniaApplySettings(1,2))) {
            assertEquals(0, node.engine().progress().lastLogIndex());
            for (String wrong : List.of(membership().configurationId(), new IoniaApplySettings(2,2).configurationId(membership()),new IoniaApplySettings(1,3).configurationId(membership())))
                try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), wrong, addresses, 1000)) {
                    assertThrows(ExecutionException.class, () -> transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS));
                }
            try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), new IoniaApplySettings(1,2).configurationId(membership()), addresses, 1000)) {
                assertEquals(IoniaEngine.Status.NORMAL, transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS).status());
            }
        }
    }
    @Test void V04_08_mainRejectsMissingOrInvalidLimitBeforeOpeningData() throws Exception {
        for (String n : List.of("missing", "-1", "2147483648")) {
            Path config = directory.resolve(n + ".properties");
            Files.writeString(config, "data.directory=" + directory.resolve("unused").toString().replace('\\', '/') + "\n" + (n.equals("missing") ? "" : "apply.max.in.flight.keys=" + n + "\nstorage.workers=2\n"));
            assertThrows(IllegalArgumentException.class, () -> IoniaFifthMain.main(new String[]{config.toString()}));
            assertFalse(Files.exists(directory.resolve("unused")));
        }
    }
    @Test void V04_08_changedStorageLayoutOrAssemblyMismatchIsRejectedBeforeOpeningKv() throws Exception {
        Map<NodeId,InetSocketAddress> addresses=addresses();
        try(var node=new IoniaFifthNode(membership(),addresses,directory,2,5000,new IoniaApplySettings(1,2))) {
            assertEquals(2,node.engine().applyDiagnostics().settings().storageShards());
        }
        var before=snapshot();
        assertThrows(IOException.class,()->new IoniaFifthNode(membership(),addresses,directory,3,5000,new IoniaApplySettings(1,3)));
        Path unused=directory.resolve("unused");
        assertThrows(IllegalArgumentException.class,()->new IoniaFifthNode(membership(),addresses,unused,3,5000,new IoniaApplySettings(1,2)));
        assertFalse(Files.exists(unused));var after=snapshot();assertEquals(before.keySet(),after.keySet());
        for(Path file:before.keySet())assertArrayEquals(before.get(file),after.get(file));
    }

}
