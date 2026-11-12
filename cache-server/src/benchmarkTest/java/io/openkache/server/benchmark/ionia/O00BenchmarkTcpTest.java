package io.openkache.server.benchmark.ionia;

import io.openkache.core.replication.ionia.IoniaCommands;
import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaConfig;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.server.ionia.IoniaNode;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(60)
class O00BenchmarkTcpTest {
    @TempDir Path directory;

    @ParameterizedTest @ValueSource(ints = {3, 5})
    void generatedRequestsUseRetainedO00ClientsAndCertifiedCrudOverRealTcp(int nodeCount) throws Exception {
        List<NodeId> ids = new ArrayList<>();
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        List<ServerSocket> reservations = new ArrayList<>();
        List<IoniaNode> nodes = new ArrayList<>();
        try {
            for (int i = 1; i <= nodeCount; i++) {
                NodeId id = new NodeId(i); ids.add(id);
                ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                reservations.add(socket);
                addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort()));
            }
            for (ServerSocket socket : reservations) { socket.close(); }
            for (NodeId id : ids) {
                nodes.add(new IoniaNode(new IoniaConfig(id, ids.getFirst(), ids, 0), addresses,
                        directory.resolve("node" + id.value()), 2, 10_000));
            }
            TraceGenerator generator = new TraceGenerator(new TraceConfig(101, 4, 24, 100,
                    0, 100, 0, 2, TraceConfig.Distribution.UNIFORM, 0));
            var settings = new O00BenchmarkClient.Settings("local-n" + nodeCount, "load-1",
                    2, 1_000_000, ids, ids.getFirst(), ids.get(1),
                    TimeUnit.SECONDS.toNanos(5), 3, TimeUnit.MILLISECONDS.toNanos(20),
                    64, 256, TimeUnit.MILLISECONDS.toNanos(10));
            Map<Bytes, Bytes> expected = new HashMap<>();
            try (var client = O00TcpClients.open(settings, addresses, 1500, TimeSource.system())) {
                for (TracePhase phase : TracePhase.values()) {
                    long count = phase == TracePhase.MEASURE ? 12 : 4;
                    for (long index = 0; index < count; index++) {
                        LogicalRequest request = generator.at(phase, index);
                        Command.Set set = (Command.Set) request.command();
                        ClientOutcome outcome = AdapterFixtures.get(client.submit(request));
                        assertEquals(ClientOutcome.Category.SUCCESS, outcome.category(), outcome.toString());
                        expected.put(set.key(), set.value());
                    }
                }
                long next = 12;
                for (long key = 0; key < 4; key++) {
                    Bytes bytes = KeyValueCodec.key(key, 24);
                    var outcome = AdapterFixtures.get(client.submit(audit(generator, next++, new Command.Get(bytes))));
                    assertEquals(ClientOutcome.Category.SUCCESS, outcome.category(), outcome.toString());
                    assertEquals(expected.get(bytes), outcome.result().value().orElseThrow());
                    KeyValueCodec.inspect(outcome.result().value().orElseThrow());
                }
                Bytes key = KeyValueCodec.key(0, 24);
                assertEquals(ResultStatus.OK, AdapterFixtures.get(client.submit(audit(generator, next++, new Command.Delete(key)))).result().status());
                assertEquals(ResultStatus.MISS, AdapterFixtures.get(client.submit(audit(generator, next++, new Command.Get(key)))).result().status());
                assertEquals(ResultStatus.MISS, AdapterFixtures.get(client.submit(audit(generator, next++, new Command.Delete(key)))).result().status());
                while (!IoniaCommands.key(generator.at(TracePhase.MEASURE, next).command()).equals(key)) { next++; }
                LogicalRequest reinsertion = generator.at(TracePhase.MEASURE, next++);
                Bytes reinsert = ((Command.Set) reinsertion.command()).value();
                assertEquals(ResultStatus.OK, AdapterFixtures.get(client.submit(reinsertion)).result().status());
                assertEquals(reinsert, AdapterFixtures.get(client.submit(audit(generator, next, new Command.Get(key)))).result().value().orElseThrow());
                var snapshot = client.snapshot();
                assertEquals(29, snapshot.admitted());
                assertEquals(29, snapshot.success());
                assertEquals(0, snapshot.errorResponse());
                assertEquals(0, snapshot.noResponse());
                assertEquals(0, snapshot.pending());
                assertEquals(6, client.diagnostics().clientSessions());
                assertEquals(21, snapshot.byOperation().get(ClientOutcome.Operation.SET).success());
                assertEquals(6, snapshot.byOperation().get(ClientOutcome.Operation.GET).success());
                assertEquals(2, snapshot.byOperation().get(ClientOutcome.Operation.DELETE).success());
                assertTrue(client.diagnostics().rpc().byKind().get(ObservedRpc.Kind.READ).dispatched() >= 6);
                assertTrue(client.diagnostics().rpc().byKind().get(ObservedRpc.Kind.META).dispatched() >= 6);
                for (IoniaNode node : nodes) { assertTrue(node.engine().failureReason().isEmpty()); }
                System.out.println("O00 local TCP fixture n=" + nodeCount + ": logical29, retained clients6, certified follower GET, SET ACK, DELETE OK/MISS; no performance claim.");
            }
        } finally {
            for (IoniaNode node : nodes) { node.close(); }
            for (ServerSocket socket : reservations) { socket.close(); }
        }
    }

    private static LogicalRequest audit(TraceGenerator generator, long index, Command command) {
        var base = generator.at(TracePhase.MEASURE, index);
        return new LogicalRequest(base.phase(), base.globalIndex(), base.streamId(),
                base.clientId(), base.sequence(), command);
    }

    @Test void nonRespondingLoopbackEndpointsCannotHangTheLogicalDeadline() throws Exception {
        List<ServerSocket> listeners = new ArrayList<>();
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        try {
            for (NodeId id : AdapterFixtures.IDS) {
                ServerSocket listener = new ServerSocket(0, 8, InetAddress.getLoopbackAddress());
                listeners.add(listener);
                addresses.put(id, new InetSocketAddress("127.0.0.1", listener.getLocalPort()));
            }
            var settings = new O00BenchmarkClient.Settings("local-timeout", "load-1", 1,
                    2_000_000, AdapterFixtures.IDS, AdapterFixtures.IDS.getFirst(), AdapterFixtures.IDS.get(1),
                    TimeUnit.MILLISECONDS.toNanos(300), 2, TimeUnit.MILLISECONDS.toNanos(5),
                    16, 64, TimeUnit.MILLISECONDS.toNanos(5));
            try (var client = O00TcpClients.open(settings, addresses, 5000, TimeSource.system())) {
                var outcome = AdapterFixtures.get(client.submit(AdapterFixtures.request(0, AdapterFixtures.delete())));
                assertEquals(ClientOutcome.Category.NO_RESPONSE, outcome.category());
                assertEquals(ClientOutcome.Reason.DEADLINE, outcome.reason());
                assertTrue(outcome.attempts().isEmpty());
                assertTrue(client.diagnostics().rpc().outstanding() > 0);
                assertEquals(1, client.snapshot().noResponse());
                assertEquals(0, client.snapshot().pending());
            }
        } finally { for (ServerSocket listener : listeners) { listener.close(); } }
    }
}

