package io.openkache.server.ionia_fourth;

import io.openkache.server.ionia.*;
import io.openkache.core.replication.ionia_fourth.*;

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
class IoniaFourthTcpConcurrencyTest {
    @TempDir Path directory;
    @Test void T07_hotKeyConcurrentTcpWritesConvergeAtAllDiskReplicas() throws Exception {
        List<NodeId> ids = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>(); List<IoniaFourthNode> nodes = new ArrayList<>();
        List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (NodeId id : ids) { ServerSocket reservation = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()); reservations.add(reservation);
                addresses.put(id, new InetSocketAddress("127.0.0.1", reservation.getLocalPort())); }
            for (ServerSocket reservation : reservations) reservation.close();
            for (NodeId id : ids) nodes.add(new IoniaFourthNode(new IoniaConfig(id, ids.getFirst(), ids, 0), addresses, directory.resolve("node" + id.value()), 2, 5000, new IoniaBatchSettings(2,65536,20_000_000)));
            IoniaConfig membership = new IoniaConfig(ids.getFirst(), ids.getFirst(), ids, 0);
            TimeSource time = TimeSource.system(); ClientId clientId = new ClientId(10); Bytes key = Bytes.utf8("hot-key");
            try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), new IoniaBatchSettings(2,65536,20_000_000).configurationId(membership), addresses, 3000);
                    IoniaClient client = new IoniaClient(clientId, ids, ids.getFirst(), 0, transport, time, new IoniaClient.Options(ids.getLast(), true, 50_000_000))) {
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
                    IoniaClientTransport.ReadReply reply = readReplica(transport, id, request, nodes);
                    assertEquals(32, reply.appliedIndex()); assertEquals(leader.value(), reply.result().value()); assertEquals(32, reply.result().valueVersion());
                }
                var d=nodes.getFirst().engine().batchDiagnostics();
                assertEquals(0,d.orderedNotCommitted()); assertEquals(0,d.committedNotApplied());
                assertTrue(d.batches().attempts()>0);
                assertEquals(leader.value(),client.execute(new Command.Get(key),context(clientId,70,time)).toCompletableFuture().get(5,TimeUnit.SECONDS).value());
            }
        } finally {
            for (IoniaFourthNode node : nodes) node.close();
            for (ServerSocket reservation : reservations) reservation.close();
        }
    }
    // Applied-prefix convergence does not imply an empty TCP accept queue. Observation GETs may
    // retry a refused connection, retaining the request identity and one total 5-second budget.
    // A failed/fenced engine is never hidden by the retry.
    private static IoniaClientTransport.ReadReply readReplica(IoniaTcpTransport transport, NodeId id,
            IoniaClientTransport.Request request, List<IoniaFourthNode> nodes) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5); ExecutionException last = null;
        while (System.nanoTime() - deadline < 0) {
            for (IoniaFourthNode node : nodes) {
                assertTrue(node.engine().failureReason().isEmpty(), node.engine().failureReason().toString());
                assertEquals(IoniaEngine.Status.NORMAL, node.engine().viewInfo().status());
            }
            try { return transport.followerRead(id, request).toCompletableFuture().get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
            catch (ExecutionException error) {
                if (!(error.getCause() instanceof ConnectException)) throw error;
                last = error;
                TimeUnit.NANOSECONDS.sleep(Math.max(0, Math.min(TimeUnit.MILLISECONDS.toNanos(20), deadline - System.nanoTime())));
            }
        }
        throw new AssertionError("replica observation failed: target=" + id + " states=" + nodes.stream().map(n -> n.engine().progress()).toList(), last);
    }
    private static RequestContext context(ClientId id, long operation, TimeSource time) {
        return new RequestContext(id, new OperationId(operation), RoutingVersion.ZERO, OwnershipEpoch.ZERO, Deadline.after(time, Duration.ofSeconds(5)));
    }
}
