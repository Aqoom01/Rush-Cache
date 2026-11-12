package io.openkache.server.benchmark.ionia;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.server.ionia.IoniaNode;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Timeout(60)
class ClosedLoopO00TcpTest {
    @TempDir Path directory;
    private static long ms(long n) { return TimeUnit.MILLISECONDS.toNanos(n); }

    @ParameterizedTest
    @CsvSource({"3,100,0,0", "5,100,0,0", "3,80,15,5", "5,80,15,5"})
    void closedLoopSeparatesPhasesAndWaitsForAllLocalReplicaApplies(int nodeCount,
            int get, int set, int delete) throws Exception {
        List<NodeId> ids = new ArrayList<>();
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        List<ServerSocket> reservations = new ArrayList<>();
        Map<NodeId, IoniaNode> nodes = new LinkedHashMap<>();
        try {
            for (int i = 1; i <= nodeCount; i++) {
                NodeId id = new NodeId(i); ids.add(id);
                ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                reservations.add(socket); addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort()));
            }
            for (ServerSocket socket : reservations) { socket.close(); }
            for (NodeId id : ids) {
                nodes.put(id, new IoniaNode(new IoniaConfig(id, ids.getFirst(), ids, 0), addresses,
                        directory.resolve("node" + id.value()), 2, 10_000));
            }
            var generator = new TraceGenerator(new TraceConfig(101, 8, 24, 100, get, set, delete,
                    4, TraceConfig.Distribution.UNIFORM, 0));
            var settings = new O00BenchmarkClient.Settings("local-closed-loop", "load-1",
                    4, 3_000_000, ids, ids.getFirst(), ids.get(1), ms(2000), 3, ms(10), 2, 128, ms(5));
            var client = O00TcpClients.open(settings, addresses, 1000, TimeSource.system());
            Map<NodeId, IoniaEngine> engines = new LinkedHashMap<>();
            nodes.forEach((id, node) -> engines.put(id, node.engine()));
            var probe = new LocalO00DrainProbe(client, engines, ids.getFirst(), 0);
            var plan = new ClosedLoopPlan(settings.runId(), settings.loadSlot(), 2, 7, 19, 0, 0,
                    new ClosedLoopPlan.Limits(ms(20_000), ms(3000), ms(1000), ms(5000), ms(3000), ms(1), 1000));
            var result = new ClosedLoopRunner(generator, plan, client, probe).run();
            assertTrue(result.passed(), result.toString());
            assertEquals(34, result.finalClient().admitted());
            assertEquals(34, result.finalClient().success());
            assertEquals(0, result.finalClient().pending());
            assertTrue(result.workersTerminated());
            assertEquals(DrainProbe.Status.READY, result.cleanupObservation().status());
            assertEquals(8, result.phases().get(TracePhase.LOAD).success());
            assertEquals(7, result.phases().get(TracePhase.WARMUP).success());
            assertEquals(19, result.phases().get(TracePhase.MEASURE).success());
            for (var phase : result.phases().values()) {
                assertTrue(phase.peakDriverSlots() <= 2);
                assertEquals(0, phase.unresolved());
            }
            for (var node : nodes.values()) {
                var p = node.engine().progress();
                assertEquals(result.expectedWrites(), p.recordedCount());
                assertEquals(result.expectedWrites(), p.lastLogIndex());
                assertEquals(result.expectedWrites(), p.commitIndex());
                assertEquals(result.expectedWrites(), p.appliedIndex());
                assertTrue(node.engine().failureReason().isEmpty());
            }
            assertEquals(0, client.diagnostics().rpc().outstanding());
            assertEquals(0, client.diagnostics().occupiedSlots());
            assertTrue(client.diagnostics().closed());
            if (set == 0 && delete == 0) {
                assertEquals(8, result.expectedWrites());
                var auditSettings = new O00BenchmarkClient.Settings("local-audit", "load-1",
                        4, 4_000_000, ids, ids.getFirst(), ids.get(1), ms(3000), 3, ms(10), 16, 128, ms(5));
                try (var audit = O00TcpClients.open(auditSettings, addresses, 1000, TimeSource.system())) {
                    for (int i = 0; i < 8; i++) {
                        var base = generator.at(TracePhase.MEASURE, i);
                        var key = KeyValueCodec.key(i, 24);
                        var request = new LogicalRequest(base.phase(), base.globalIndex(), base.streamId(),
                                base.clientId(), base.sequence(), new Command.Get(key));
                        var value = audit.submit(request).toCompletableFuture().get(5, TimeUnit.SECONDS);
                        assertEquals(ClientOutcome.Category.SUCCESS, value.category());
                        assertEquals(((Command.Set) generator.at(TracePhase.LOAD, i).command()).value(),
                                value.result().value().orElseThrow());
                    }
                }
            }
            System.out.println("Closed-loop local TCP n=" + nodeCount + ", mix=" + get + "/" + set + "/" + delete
                    + ", phases=8/7/19, concurrency2/streams4, all replica applies=" + result.expectedWrites()
                    + "; local functional fixture, no performance comparison.");
        } finally {
            for (var node : nodes.values()) { node.close(); }
            for (var socket : reservations) { socket.close(); }
        }
    }
}

