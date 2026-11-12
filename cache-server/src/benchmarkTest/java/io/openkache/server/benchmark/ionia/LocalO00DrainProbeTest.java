package io.openkache.server.benchmark.ionia;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.runner.benchmark.client.ClientAccounting;
import io.openkache.runner.benchmark.execution.DrainProbe;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LocalO00DrainProbeTest {
    private static final Set<NodeId> IDS = Set.of(new NodeId(1), new NodeId(2), new NodeId(3));
    private static O00BenchmarkClient.Diagnostics diagnostics(int active, int occupied, int raw, boolean closed) {
        return new O00BenchmarkClient.Diagnostics(new ClientAccounting().snapshot(),
                new ObservedRpc.Snapshot(Map.of(), raw, raw, closed),
                0, active, occupied, 0, closed, null);
    }
    private Map<NodeId, LocalO00DrainProbe.State> states(long recorded, long ordered, long commit, long applied) {
        Map<NodeId, LocalO00DrainProbe.State> result = new LinkedHashMap<>();
        for (NodeId id : IDS) {
            result.put(id, new LocalO00DrainProbe.State(
                    new IoniaEngine.Progress(id, Math.toIntExact(recorded), ordered, commit, applied),
                    new IoniaEngine.ViewInfo(0, new NodeId(1), IoniaEngine.Status.NORMAL), null));
        }
        return result;
    }
    private LocalO00DrainProbe probe(AtomicReference<O00BenchmarkClient.Diagnostics> client,
            AtomicReference<Map<NodeId, LocalO00DrainProbe.State>> nodes) {
        return new LocalO00DrainProbe(client::get, nodes::get, IDS, new NodeId(1), 0);
    }

    @Test void ackRecordsAloneAreNotAnApplyFrontier() {
        var client = new AtomicReference<>(diagnostics(0, 0, 0, false));
        var nodes = new AtomicReference<>(states(10, 9, 8, 7));
        var probe = probe(client, nodes);
        assertEquals(DrainProbe.Status.WAIT, probe.observe(10).status());
        nodes.set(states(10, 10, 10, 9));
        assertEquals(DrainProbe.Status.WAIT, probe.observe(10).status());
        nodes.set(states(10, 10, 10, 10));
        assertEquals(DrainProbe.Status.READY, probe.observe(10).status());
    }

    @Test void everyReplicaMustApplyAndRawPublicationMustDrain() {
        var client = new AtomicReference<>(diagnostics(0, 0, 1, false));
        var nodes = new AtomicReference<>(states(10, 10, 10, 10));
        var probe = probe(client, nodes);
        assertEquals(DrainProbe.Status.WAIT, probe.observe(10).status());
        client.set(diagnostics(0, 1, 0, false));
        assertEquals(DrainProbe.Status.WAIT, probe.observe(10).status());
        client.set(diagnostics(0, 0, 0, false));
        var copy = new HashMap<>(nodes.get());
        copy.put(new NodeId(3), new LocalO00DrainProbe.State(
                new IoniaEngine.Progress(new NodeId(3), 10, 10, 10, 9),
                new IoniaEngine.ViewInfo(0, new NodeId(1), IoniaEngine.Status.NORMAL), null));
        nodes.set(copy);
        assertEquals(DrainProbe.Status.WAIT, probe.observe(10).status());
    }

    @Test void dirtyOrExtraWritesFailInsteadOfMovingTheTarget() {
        var client = new AtomicReference<>(diagnostics(0, 0, 0, false));
        var nodes = new AtomicReference<>(states(1, 1, 1, 1));
        var probe = probe(client, nodes);
        assertEquals(DrainProbe.Status.FAILED, probe.observe(0).status());
        assertEquals("unexpected_write_frontier", probe.observe(0).reason());
        nodes.set(states(11, 10, 10, 10));
        assertEquals(DrainProbe.Status.FAILED, probe.observe(10).status());
    }

    @Test void missingReplicaViewChangeAndRecoveryCannotPass() {
        var client = new AtomicReference<>(diagnostics(0, 0, 0, false));
        var nodes = new AtomicReference<>(states(0, 0, 0, 0));
        var probe = probe(client, nodes);
        var missing = new HashMap<>(nodes.get()); missing.remove(new NodeId(3)); nodes.set(missing);
        assertEquals(DrainProbe.Status.FAILED, probe.observe(0).status());
        for (var view : List.of(new IoniaEngine.ViewInfo(1, new NodeId(2), IoniaEngine.Status.NORMAL),
                new IoniaEngine.ViewInfo(0, new NodeId(1), IoniaEngine.Status.RECOVERING))) {
            var changed = states(0, 0, 0, 0);
            changed.put(new NodeId(2), new LocalO00DrainProbe.State(changed.get(new NodeId(2)).progress(), view, null));
            nodes.set(changed);
            assertEquals(DrainProbe.Status.FAILED, probe.observe(0).status());
        }
    }

    @Test void closeAdmissionIsDistinctFromSuccessfulRawAndApplyCleanup() {
        var client = new AtomicReference<>(diagnostics(0, 0, 0, true));
        var nodes = new AtomicReference<>(states(0, 0, 0, 0));
        var probe = probe(client, nodes);
        assertEquals(DrainProbe.Status.FAILED, probe.observe(0).status());
        assertEquals(DrainProbe.Status.READY, probe.afterClose(0).status());
        client.set(diagnostics(0, 0, 1, true));
        assertEquals(DrainProbe.Status.WAIT, probe.afterClose(0).status());
    }
}

