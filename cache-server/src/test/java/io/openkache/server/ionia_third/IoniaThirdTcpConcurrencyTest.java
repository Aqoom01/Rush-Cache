package io.openkache.server.ionia_third;

import io.openkache.server.ionia.*;
import io.openkache.core.replication.ionia_third.*;

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
class IoniaThirdTcpConcurrencyTest {
    @TempDir Path directory;
    private final List<String> events=Collections.synchronizedList(new ArrayList<>());
    @Test void T07_hotKeyConcurrentTcpWritesConvergeAtAllDiskReplicas() throws Exception {
        List<NodeId> ids = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>(); List<IoniaThirdNode> nodes = new ArrayList<>();
        List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (NodeId id : ids) { ServerSocket reservation = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()); reservations.add(reservation);
                addresses.put(id, new InetSocketAddress("127.0.0.1", reservation.getLocalPort())); }
            for (ServerSocket reservation : reservations) reservation.close();
            for (NodeId id : ids) nodes.add(new IoniaThirdNode(new IoniaConfig(id, ids.getFirst(), ids, 0), addresses, directory.resolve("node" + id.value()), 2, 5000, new IoniaHistorySettings(1)));
            IoniaConfig membership = new IoniaConfig(ids.getFirst(), ids.getFirst(), ids, 0);
            TimeSource time = TimeSource.system(); ClientId clientId = new ClientId(10); Bytes key = Bytes.utf8("hot-key");
            try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), new IoniaHistorySettings(1).configurationId(membership), addresses, 3000);
                    IoniaClient client = new IoniaClient(clientId, ids, ids.getFirst(), 0, new TraceTransport(transport), time, new IoniaClient.Options(ids.getLast(), true, 50_000_000))) {
                List<CompletableFuture<Result>> writes = new ArrayList<>();
                for (int i = 1; i <= 32; i++) writes.add(client.execute(new Command.Set(key, Bytes.utf8("v" + i)), context(clientId, i, time)).toCompletableFuture());
                for (CompletableFuture<Result> write : writes) assertEquals(ResultStatus.OK, write.get(5, TimeUnit.SECONDS).status());
                Result leader = client.execute(new Command.Get(key), context(clientId, 40, time)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(ResultStatus.OK, leader.status(),()->"GET="+leader+" nodes="+nodes.stream().map(n->n.engine().progress()+"/"+n.engine().viewInfo()+"/"+n.engine().failureReason()).toList()+" trace="+events);
                assertEquals(32, leader.valueVersion(),()->"GET="+leader+" trace="+events);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (nodes.stream().anyMatch(node -> node.engine().progress().appliedIndex() != 32) && System.nanoTime() - until < 0) Thread.sleep(10);
                for (NodeId id : ids) {
                    IoniaClientTransport.Request request = new IoniaClientTransport.Request(0, new Command.Get(key), clientId, new OperationId(50 + id.value()),
                            RoutingVersion.ZERO, OwnershipEpoch.ZERO, TimeUnit.SECONDS.toNanos(5));
                    IoniaClientTransport.ReadReply reply = readReplica(transport, id, request, nodes);
                    assertEquals(ResultStatus.OK, reply.result().status(),()->"replica="+id+" reply="+reply);
                    assertEquals(32, reply.appliedIndex()); assertEquals(leader.value(), reply.result().value()); assertEquals(32, reply.result().valueVersion());
                }
                for (int i = 0; i < 2; i++) assertEquals(ResultStatus.OK, client.execute(new Command.Set(Bytes.utf8("other" + i), Bytes.utf8("x")), context(clientId, 60 + i, time)).toCompletableFuture().get(5, TimeUnit.SECONDS).status());
                long trimDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (nodes.getFirst().engine().historyDiagnostics().history().entries() != 1 && System.nanoTime() - trimDeadline < 0) Thread.sleep(10);
                assertEquals(1, nodes.getFirst().engine().historyDiagnostics().history().entries());
                assertEquals(33, nodes.getFirst().engine().historyDiagnostics().history().lastTrimmedIndex());
                assertEquals(leader.value(), client.execute(new Command.Get(key), context(clientId, 70, time)).toCompletableFuture().get(5, TimeUnit.SECONDS).value());
            }
        } finally {
            for (IoniaThirdNode node : nodes) node.close();
            for (ServerSocket reservation : reservations) reservation.close();
        }
    }
    // Applied-prefix convergence does not imply an empty TCP accept queue. Observation GETs may
    // retry a refused connection, retaining the request identity and one total 5-second budget.
    // A failed/fenced engine is never hidden by the retry.
    private static IoniaClientTransport.ReadReply readReplica(IoniaTcpTransport transport, NodeId id,
            IoniaClientTransport.Request request, List<IoniaThirdNode> nodes) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5); ExecutionException last = null;
        while (System.nanoTime() - deadline < 0) {
            for (IoniaThirdNode node : nodes) {
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

    // Observation only: preserve failed RPCs and deadlines; never retry or turn failure into OK.
    private final class TraceTransport implements IoniaClientTransport {
        private final IoniaTcpTransport tcp;
        TraceTransport(IoniaTcpTransport tcp){this.tcp=tcp;}
        private <T> CompletionStage<T> trace(String kind,NodeId to,CompletionStage<T> stage){
            return stage.whenComplete((reply,error)->events.add(System.nanoTime()+" "+kind+" to="+to+" "+(error==null?reply:error)));
        }
        @Override public CompletionStage<WriteAck> record(NodeId to,Request r){return tcp.record(to,r);}
        @Override public CompletionStage<Reply> execute(NodeId to,Request r){return trace("EXECUTE "+r.operationId(),to,tcp.execute(to,r));}
        @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request r){return trace("READ "+r.operationId(),to,tcp.followerRead(to,r));}
        @Override public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){return trace("META "+r.operationId(),to,tcp.metaQuery(to,r));}
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){return trace("PROBE",to,tcp.probe(to));}
    }
}
