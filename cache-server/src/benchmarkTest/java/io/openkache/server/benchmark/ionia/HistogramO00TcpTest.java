package io.openkache.server.benchmark.ionia;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.*;
import io.openkache.server.ionia.IoniaNode;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Timeout(60)
class HistogramO00TcpTest {
    @TempDir Path directory;
    private static long ms(long value) { return TimeUnit.MILLISECONDS.toNanos(value); }
    @ParameterizedTest
    @CsvSource({"3,true", "5,true", "3,false", "5,false"})
    void realMixedTcpOutcomesAreCollectedAndExportedInBothModes(int count, boolean open) throws Exception {
        List<NodeId> ids = new ArrayList<>();
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        List<ServerSocket> reservations = new ArrayList<>();
        Map<NodeId, IoniaNode> nodes = new LinkedHashMap<>();
        try {
            for (int i = 1; i <= count; i++) {
                NodeId id = new NodeId(i); ids.add(id);
                var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()); reservations.add(socket);
                addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort()));
            }
            for (var socket : reservations) { socket.close(); }
            for (var id : ids) { nodes.put(id, new IoniaNode(new IoniaConfig(id, ids.getFirst(), ids, 0),
                    addresses, directory.resolve("node" + id.value()), 2, 10_000)); }
            var generator = new TraceGenerator(new TraceConfig(101, 4, 24, 100, 40, 30, 30,
                    4, TraceConfig.Distribution.UNIFORM, 0));
            var settings = new O00BenchmarkClient.Settings("histogram-local", "slot", 4, 7_000_000,
                    ids, ids.getFirst(), ids.get(1), ms(3000), 3, ms(10), 32, 256, ms(5));
            var client = O00TcpClients.open(settings, addresses, 1500, TimeSource.system());
            Map<NodeId, IoniaEngine> engines = new LinkedHashMap<>(); nodes.forEach((id, node) -> engines.put(id, node.engine()));
            var probe = new LocalO00DrainProbe(client, engines, ids.getFirst(), 0);
            var collector = new LatencyCollector(settings.runId(), settings.loadSlot(), "local-clock", ms(30_000));
            var metadata = new BenchmarkRunWriter.Metadata("O00", "histogram-local-functional", count,
                    null, null, "local-clock", Set.of("M01", "M02", "M03", "M04", "M10", "M11"));
            Path parent = Path.of("build", "benchmark-b5-fixtures"); Files.createDirectories(parent);
            Path output = parent.resolve("tcp-n" + count + "-" + (open ? "open" : "closed") + "-" + UUID.randomUUID());
            BenchmarkRunWriter.Export exported;
            long writes;
            if (open) {
                var plan = new OpenLoopPlan(settings.runId(), settings.loadSlot(), 3, 30, 20, 20, 50,
                        8, ms(2000), 0, 0, new OpenLoopPlan.Limits(ms(20_000), ms(4000), ms(5000), ms(3000), ms(1)));
                var result = new OpenLoopRunner(generator, plan, client, probe, collector).run();
                assertTrue(result.passed(), result.toString()); writes = result.expectedWrites();
                exported = BenchmarkRunWriter.write(output, metadata, generator.config(), plan, result, collector.freeze());
            } else {
                var plan = new ClosedLoopPlan(settings.runId(), settings.loadSlot(), 2, 3, 30, 0, 0,
                        new ClosedLoopPlan.Limits(ms(20_000), ms(3000), ms(1000), ms(5000), ms(3000), ms(1), 1000));
                var result = new ClosedLoopRunner(generator, plan, client, probe, collector).run();
                assertTrue(result.passed(), result.toString()); writes = result.expectedWrites();
                exported = BenchmarkRunWriter.write(output, metadata, generator.config(), plan, result, collector.freeze());
            }
            assertTrue(exported.valid(), exported.toString()); assertTrue(Files.exists(output.resolve("COMPLETE")));
            for (var op : ClientOutcome.Operation.values()) {
                var tally = collector.freeze().counts().get(new LatencyCollector.Key(TracePhase.MEASURE, op));
                assertNotNull(tally); assertTrue(tally.success() > 0, op.toString());
                for (var kind : LatencyCollector.Kind.values()) {
                    assertEquals(tally.success(), collector.freeze().series().stream().filter(s -> s.population().phase() == TracePhase.MEASURE
                            && s.population().operation() == op && s.population().kind() == kind).mapToLong(s -> s.histogram().count()).sum());
                }
            }
            assertEquals(37, client.snapshot().success()); assertEquals(0, client.diagnostics().rpc().outstanding());
            for (var node : nodes.values()) { assertEquals(writes, node.engine().progress().appliedIndex()); }
            System.out.println("B5_EXPORT_FIXTURE=" + output.toAbsolutePath());
            System.out.println("B5 TCP n=" + count + ", mode=" + (open ? "open" : "closed")
                    + ", phases4/3/30, mix40/30/30, all CRUD populations, applied=" + writes + "; functional only");
        } finally {
            for (var node : nodes.values()) { node.close(); }
            for (var socket : reservations) { socket.close(); }
        }
    }
}
