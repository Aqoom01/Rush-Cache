package io.openkache.server.ionia_sixth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.server.ionia.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_sixth.*;
import java.io.IOException;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
class IoniaSixthProfileTest {
    @TempDir Path directory;
    private static final List<NodeId> IDS = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
    private static Map<NodeId, InetSocketAddress> addresses() throws IOException {
        List<ServerSocket> held = new ArrayList<>(); Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        try { for (NodeId id : IDS) { ServerSocket socket = new ServerSocket(0); held.add(socket); addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort())); } }
        finally { for (ServerSocket socket : held) socket.close(); }
        return addresses;
    }
    private static IoniaConfig membership() { return new IoniaConfig(IDS.getFirst(), IDS.getFirst(), IDS, 0); }
    @Test void V04B_08_differentWorkerDirectoryIsRejectedWithoutChangingStoredFiles() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaSixthNode node = new IoniaSixthNode(membership(), addresses, directory, 5000, new IoniaWorkerSettings(4,2))) {
            assertEquals(2,node.storageDiagnostics().ioWorkers());
        }
        Map<Path, byte[]> before = snapshot();
        assertThrows(IOException.class, () -> new IoniaSixthNode(membership(), addresses, directory, 5000, new IoniaWorkerSettings(4,1)));
        Map<Path, byte[]> after = snapshot(); assertEquals(before.keySet(), after.keySet());
        for (Path path : before.keySet()) assertArrayEquals(before.get(path), after.get(path), path.toString());
    }
    private Map<Path, byte[]> snapshot() throws IOException {
        Map<Path, byte[]> result = new HashMap<>();
        try (var files = Files.walk(directory)) { for (Path path : files.filter(Files::isRegularFile).toList()) result.put(path, Files.readAllBytes(path)); }
        return result;
    }
    @Test void V04B_08_tcpRejectsO00AndOtherWorkersButAcceptsMatchingProfile() throws Exception {
        Map<NodeId, InetSocketAddress> addresses = addresses();
        try (IoniaSixthNode node = new IoniaSixthNode(membership(), addresses, directory, 5000, new IoniaWorkerSettings(4,2))) {
            assertEquals(0, node.engine().progress().lastLogIndex());
            for (String wrong : List.of(membership().configurationId(), new IoniaWorkerSettings(4,1).configurationId(membership()),new IoniaWorkerSettings(2,2).configurationId(membership())))
                try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), wrong, addresses, 1000)) {
                    assertThrows(ExecutionException.class, () -> transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS));
                }
            try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), new IoniaWorkerSettings(4,2).configurationId(membership()), addresses, 1000)) {
                assertEquals(IoniaEngine.Status.NORMAL, transport.probe(IDS.getFirst()).toCompletableFuture().get(2, TimeUnit.SECONDS).status());
            }
        }
    }
    @Test void V04B_02_mainRejectsMissingOrInvalidWorkersBeforeOpeningData() throws Exception {
        for (String n : List.of("missing", "-1", "2147483648")) {
            Path config = directory.resolve(n + ".properties");
            Files.writeString(config, "data.directory=" + directory.resolve("unused").toString().replace('\\', '/') + "\n" + (n.equals("missing") ? "" : "storage.io.workers=" + n + "\nstorage.shards=4\n"));
            assertThrows(IllegalArgumentException.class, () -> IoniaSixthMain.main(new String[]{config.toString()}));
            assertFalse(Files.exists(directory.resolve("unused")));
        }
    }
    @Test void V04B_08_wrongShardCountProfileIsRejectedBeforeOpeningExistingFiles()throws Exception{
        var addresses=addresses();try(var node=new IoniaSixthNode(membership(),addresses,directory,5000,new IoniaWorkerSettings(4,2))){assertEquals(4,node.storageDiagnostics().storageShards());}
        var before=snapshot();assertThrows(IOException.class,()->new IoniaSixthNode(membership(),addresses,directory,5000,new IoniaWorkerSettings(2,2)));
        var after=snapshot();assertEquals(before.keySet(),after.keySet());for(Path p:before.keySet())assertArrayEquals(before.get(p),after.get(p));
    }

}
