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
class OpenLoopO00TcpTest {
    @TempDir Path directory;
    private static long ms(long n) { return TimeUnit.MILLISECONDS.toNanos(n); }

    @ParameterizedTest
    @CsvSource({"3,100,0,0", "5,100,0,0", "3,80,15,5", "5,80,15,5"})
    void realTcpPreservesFixedRatePhasesAndReplicaApplyQualification(int nodeCount,
            int get, int set, int delete) throws Exception {
        List<NodeId> ids = new ArrayList<>();
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        List<ServerSocket> reservations = new ArrayList<>();
        Map<NodeId, IoniaNode> nodes = new LinkedHashMap<>();
        try {
            for (int i = 1; i <= nodeCount; i++) {
                NodeId id = new NodeId(i); ids.add(id);
                ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                reservations.add(socket);
                addresses.put(id, new InetSocketAddress("127.0.0.1", socket.getLocalPort()));
            }
            for (ServerSocket socket : reservations) { socket.close(); }
            for (NodeId id : ids) {
                nodes.put(id, new IoniaNode(new IoniaConfig(id, ids.getFirst(), ids, 0), addresses,
                        directory.resolve("node" + id.value()), 2, 10_000));
            }
            var generator = new TraceGenerator(new TraceConfig(101, 4, 24, 100, get, set, delete,
                    4, TraceConfig.Distribution.UNIFORM, 0));
            var settings = new O00BenchmarkClient.Settings("local-open-loop", "load-1",
                    4, 5_000_000, ids, ids.getFirst(), ids.get(1), ms(3000), 3, ms(10),
                    32, 256, ms(5));
            var client = O00TcpClients.open(settings, addresses, 1500, TimeSource.system());
            Map<NodeId, IoniaEngine> engines = new LinkedHashMap<>();
            nodes.forEach((id, node) -> engines.put(id, node.engine()));
            var probe = new LocalO00DrainProbe(client, engines, ids.getFirst(), 0);
            var plan = new OpenLoopPlan(settings.runId(), settings.loadSlot(), 3, 10,
                    20, 20, 30, 8, ms(2000), 0, 0,
                    new OpenLoopPlan.Limits(ms(20_000), ms(4000), ms(5000), ms(3000), ms(1)));
            List<OpenLoopResult.Decision> decisions = new ArrayList<>();
            var observer = new OpenLoopRunner.Observer() {
                @Override public void decision(OpenLoopResult.Decision d) { decisions.add(d); }
            };
            var result = new OpenLoopRunner(generator, plan, client, probe, observer).run();
            assertTrue(result.passed(), result.toString());
            assertEquals(17, result.finalClient().admitted()); assertEquals(17, result.finalClient().success());
            assertEquals(0, result.finalClient().pending()); assertEquals(0, result.outstandingCallbacks());
            assertEquals(DrainProbe.Status.READY, result.cleanupObservation().status());
            assertEquals(4, result.phases().get(TracePhase.LOAD).considered());
            assertEquals(3, result.phases().get(TracePhase.WARMUP).considered());
            assertEquals(10, result.phases().get(TracePhase.MEASURE).considered());
            for (var phase : result.phases().values()) {
                assertEquals(0, phase.unissued()); assertEquals(0, phase.unresolved());
                assertEquals(0, phase.capacityDrops()); assertEquals(0, phase.timingMisses());
                assertEquals(phase.schedule().requests(), phase.submissionCalls());
                assertEquals(phase.schedule().requests(), phase.admittedLag().count());
                assertTrue(phase.peakInFlight() <= 8);
            }
            for (var d : decisions) {
                var phase = result.phases().get(d.phase());
                assertEquals(phase.schedule().offsetNanos(d.index()), d.dueNanos() - phase.startedNanos());
            }
            for (var node : nodes.values()) {
                var p = node.engine().progress();
                assertEquals(result.expectedWrites(), p.recordedCount());
                assertEquals(result.expectedWrites(), p.lastLogIndex());
                assertEquals(result.expectedWrites(), p.commitIndex());
                assertEquals(result.expectedWrites(), p.appliedIndex());
                assertTrue(node.engine().failureReason().isEmpty());
            }
            assertTrue(client.diagnostics().closed());
            assertEquals(0, client.diagnostics().rpc().outstanding());
            if (set == 0 && delete == 0) {
                var auditSettings = new O00BenchmarkClient.Settings("local-open-loop-audit", "load-1",
                        4, 6_000_000, ids, ids.getFirst(), ids.get(1), ms(3000), 3, ms(10), 16, 128, ms(5));
                try (var audit = O00TcpClients.open(auditSettings, addresses, 1500, TimeSource.system())) {
                    for (int i = 0; i < 4; i++) {
                        var base = generator.at(TracePhase.MEASURE, i);
                        var request = new LogicalRequest(base.phase(), base.globalIndex(), base.streamId(),
                                base.clientId(), base.sequence(), new Command.Get(KeyValueCodec.key(i, 24)));
                        var outcome = audit.submit(request).toCompletableFuture().get(5, TimeUnit.SECONDS);
                        assertEquals(ClientOutcome.Category.SUCCESS, outcome.category());
                        assertEquals(((Command.Set) generator.at(TracePhase.LOAD, i).command()).value(),
                                outcome.result().value().orElseThrow());
                    }
                }
            }
            System.out.println("Open-loop local TCP n=" + nodeCount + ", mix=" + get + "/" + set + "/" + delete
                    + ", phases=4/3/10, rates=20/20/30ops-s, bound8, considered17, healthy17, applied="
                    + result.expectedWrites() + "; functional fixture only, no capacity/performance claim.");
        } finally {
            for (IoniaNode node : nodes.values()) { node.close(); }
            for (ServerSocket socket : reservations) { socket.close(); }
        }
    }
}

