package io.openkache.server.benchmark.ionia;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.runner.benchmark.execution.DrainProbe;
import java.util.*;
import java.util.function.Supplier;

/**
 * 같은 JVM에 명시적으로 연결한 빈/격리 O00 그룹의 공개 snapshot만 관찰한다.
 * 원격 progress RPC/SSD fsync/전체 네트워크 drain을 제공하지 않는다.
 */
public final class LocalO00DrainProbe implements DrainProbe {
    record State(IoniaEngine.Progress progress, IoniaEngine.ViewInfo view, String failure) {}
    private final Supplier<O00BenchmarkClient.Diagnostics> client;
    private final Supplier<Map<NodeId, State>> nodes;
    private final Set<NodeId> members;
    private final NodeId leader;
    private final long view;

    public LocalO00DrainProbe(O00BenchmarkClient client, Map<NodeId, IoniaEngine> engines,
            NodeId leader, long view) {
        this(Objects.requireNonNull(client)::diagnostics, snapshots(engines),
                Set.copyOf(engines.keySet()), leader, view);
    }

    LocalO00DrainProbe(Supplier<O00BenchmarkClient.Diagnostics> client,
            Supplier<Map<NodeId, State>> nodes, Set<NodeId> members, NodeId leader, long view) {
        this.client = Objects.requireNonNull(client); this.nodes = Objects.requireNonNull(nodes);
        this.members = Set.copyOf(members); this.leader = Objects.requireNonNull(leader); this.view = view;
        if ((members.size() != 3 && members.size() != 5) || !members.contains(leader) || view < 0) {
            throw new IllegalArgumentException("invalid local membership/view");
        }
    }

    private static Supplier<Map<NodeId, State>> snapshots(Map<NodeId, IoniaEngine> engines) {
        Map<NodeId, IoniaEngine> copy = Map.copyOf(engines);
        return () -> {
            Map<NodeId, State> result = new LinkedHashMap<>();
            copy.forEach((id, engine) -> result.put(id,
                    new State(engine.progress(), engine.viewInfo(), engine.failureReason().orElse(null))));
            return result;
        };
    }

    @Override public Observation observe(long expectedWrites) { return sample(expectedWrites, false); }
    @Override public Observation afterClose(long expectedWrites) { return sample(expectedWrites, true); }

    private Observation sample(long expected, boolean closing) {
        if (expected < 0) { throw new IllegalArgumentException("negative frontier"); }
        var d = client.get();
        Map<NodeId, State> snapshots = nodes.get();
        Map<String, Long> evidence = new LinkedHashMap<>();
        evidence.put("expected_writes", expected);
        evidence.put("client_pending", d.logical().pending());
        evidence.put("client_active", (long) d.active());
        evidence.put("client_occupied", (long) d.occupiedSlots());
        evidence.put("client_raw_outstanding", (long) d.rpc().outstanding());
        evidence.put("client_closed", d.closed() ? 1L : 0L);
        if (!snapshots.keySet().equals(members)) { return new Observation(Status.FAILED, "membership_mismatch", evidence); }
        String failed = !closing && d.closed() ? "client_closed"
                : d.closeError() != null ? "client_close_error" : null;
        boolean waiting = d.logical().pending() != 0 || d.active() != 0
                || d.occupiedSlots() != 0 || d.rpc().outstanding() != 0;
        for (var entry : snapshots.entrySet()) {
            State state = entry.getValue();
            var p = state.progress();
            String prefix = "node_" + entry.getKey().value() + "_";
            evidence.put(prefix + "recorded", (long) p.recordedCount());
            evidence.put(prefix + "last", p.lastLogIndex());
            evidence.put(prefix + "commit", p.commitIndex());
            evidence.put(prefix + "applied", p.appliedIndex());
            evidence.put(prefix + "view", state.view().view());
            if (!p.nodeId().equals(entry.getKey()) || state.failure() != null
                    || state.view().status() != IoniaEngine.Status.NORMAL
                    || state.view().view() != view || !state.view().leaderId().equals(leader)) {
                failed = "node_health_or_view";
            }
            if (p.recordedCount() > expected || p.lastLogIndex() > expected
                    || p.commitIndex() > expected || p.appliedIndex() > expected) {
                failed = "unexpected_write_frontier";
            }
            if (p.recordedCount() != expected || p.lastLogIndex() != expected
                    || p.commitIndex() != expected || p.appliedIndex() != expected) { waiting = true; }
        }
        return new Observation(failed != null ? Status.FAILED : waiting ? Status.WAIT : Status.READY,
                failed != null ? failed : waiting ? "pending_rpc_or_apply" : "all_replicas_applied", evidence);
    }
}

