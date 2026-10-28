package io.openkache.server.ionia_second;

import io.openkache.server.ionia.IoniaNode;
import io.openkache.server.ionia.IoniaTcpTransport;
import io.openkache.core.replication.ionia_second.IoniaLeaderReadClient;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import java.net.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class IoniaLeaderReadTcpConcurrencyTest {
    @TempDir Path directory;
    @Test void LR08_LR09_concurrentV01WritesConvergeAtAllDiskReplicas() throws Exception {
        List<NodeId> ids = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>(); List<IoniaNode> nodes = new ArrayList<>();
        List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (NodeId id : ids) { ServerSocket reservation = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()); reservations.add(reservation);
                addresses.put(id, new InetSocketAddress("127.0.0.1", reservation.getLocalPort())); }
            for (ServerSocket reservation : reservations) reservation.close();
            for (NodeId id : ids) nodes.add(new IoniaNode(new IoniaConfig(id, ids.getFirst(), ids, 0), addresses, directory.resolve("node" + id.value()), 2, 5000));
            IoniaConfig membership = new IoniaConfig(ids.getFirst(), ids.getFirst(), ids, 0);
            TimeSource time = TimeSource.system(); ClientId clientId = new ClientId(10); Bytes key = Bytes.utf8("hot-key");
            try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), membership.configurationId(), addresses, 3000);
                    IoniaLeaderReadClient client = new IoniaLeaderReadClient(clientId, ids, ids.getFirst(), 0, transport, time)) {
                List<CompletableFuture<Result>> writes = new ArrayList<>();
                for (int i = 1; i <= 32; i++) writes.add(client.execute(new Command.Set(key, Bytes.utf8("v" + i)), context(clientId, i, time)).toCompletableFuture());
                for (CompletableFuture<Result> write : writes) assertEquals(ResultStatus.OK, write.get(5, TimeUnit.SECONDS).status());
                Result leader = client.execute(new Command.Get(key), context(clientId, 40, time)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(32, leader.valueVersion());
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (nodes.stream().anyMatch(node -> node.engine().progress().appliedIndex() != 32) && System.nanoTime() - until < 0) Thread.sleep(10);
                for (NodeId id : ids) {
                    IoniaClientTransport.Request request = new IoniaClientTransport.Request(0, new Command.Get(key), clientId, new OperationId(50 + id.value()),
                            RoutingVersion.ZERO, OwnershipEpoch.ZERO, TimeUnit.SECONDS.toNanos(5));
                    IoniaClientTransport.ReadReply reply = transport.followerRead(id, request).toCompletableFuture().get(5, TimeUnit.SECONDS);
                    assertEquals(32, reply.appliedIndex()); assertEquals(leader.value(), reply.result().value()); assertEquals(32, reply.result().valueVersion());
                }
            }
        } finally {
            for (IoniaNode node : nodes) node.close();
            for (ServerSocket reservation : reservations) reservation.close();
        }
    }
    private static RequestContext context(ClientId id, long operation, TimeSource time) {
        return new RequestContext(id, new OperationId(operation), RoutingVersion.ZERO, OwnershipEpoch.ZERO, Deadline.after(time, Duration.ofSeconds(5)));
    }
}
