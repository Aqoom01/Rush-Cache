package io.openkache.server.benchmark.variants;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.runner.benchmark.execution.DrainProbe;
import java.util.*;
import java.util.function.Supplier;

/** 명시적으로 연결한 같은 JVM의 파생 엔진 snapshot과 client raw/내부 대기를 확인한다. 원격 관찰이 아니다. */
public final class LocalVariantDrainProbe implements DrainProbe {
    public record NodeSnapshot(IoniaEngine.Progress progress, IoniaEngine.ViewInfo view, String failure, long observedPending) {}
    private final VariantBenchmarkClient client;
    private final Map<NodeId, Supplier<NodeSnapshot>> nodes;
    private final NodeId leader;
    private final long view;
    public LocalVariantDrainProbe(VariantBenchmarkClient client, Map<NodeId, Supplier<NodeSnapshot>> nodes, NodeId leader, long view) {
        this.client = Objects.requireNonNull(client); this.nodes = Map.copyOf(nodes); this.leader = Objects.requireNonNull(leader); this.view = view;
        if (view < 0 || !client.settings().replicas().contains(leader)) { throw new IllegalArgumentException("invalid local view"); }
    }
    @Override public Observation observe(long expected) { return sample(expected, false); }
    @Override public Observation afterClose(long expected) { return sample(expected, true); }
    private Observation sample(long expected, boolean closing) {
        if (expected < 0) { throw new IllegalArgumentException("negative frontier"); }
        var d = client.diagnostics(); Map<String, Long> evidence = new TreeMap<>();
        evidence.put("expected_writes", expected); evidence.put("client_pending", d.logical().pending());
        evidence.put("client_raw", (long)d.rpc().outstanding()); evidence.put("client_active", (long)d.active());
        evidence.put("client_occupied", (long)d.occupiedSlots()); evidence.put("client_closed", d.closed()?1L:0L);
        if (!nodes.keySet().equals(Set.copyOf(client.settings().replicas()))) { return new Observation(Status.FAILED,"membership_mismatch",evidence); }
        String failed = d.closeError()!=null ? "client_close_error" : !closing && d.closed() ? "client_closed" : null;
        boolean waiting = d.logical().pending()!=0 || d.rpc().outstanding()!=0 || d.active()!=0 || d.occupiedSlots()!=0;
        for (var p : client.protocolPending().entrySet()) {
            evidence.put("protocol_"+p.getKey(),p.getValue()); if(p.getValue()<0){failed="invalid_protocol_pending";} if(p.getValue()!=0){waiting=true;}
        }
        for (var entry : nodes.entrySet()) {
            var s = Objects.requireNonNull(entry.getValue().get()); var p=s.progress();
            String prefix="node_"+entry.getKey().value()+"_";
            evidence.put(prefix+"recorded",(long)p.recordedCount()); evidence.put(prefix+"last",p.lastLogIndex());
            evidence.put(prefix+"commit",p.commitIndex()); evidence.put(prefix+"applied",p.appliedIndex());
            evidence.put(prefix+"view",s.view().view()); evidence.put(prefix+"observed_pending",s.observedPending());
            if(!p.nodeId().equals(entry.getKey()) || s.failure()!=null || s.view().status()!=IoniaEngine.Status.NORMAL
                    || s.view().view()!=view || !s.view().leaderId().equals(leader) || s.observedPending()<0){failed="node_health_or_view";}
            if(p.recordedCount()>expected || p.lastLogIndex()>expected || p.commitIndex()>expected || p.appliedIndex()>expected){failed="unexpected_write_frontier";}
            if(p.recordedCount()!=expected || p.lastLogIndex()!=expected || p.commitIndex()!=expected || p.appliedIndex()!=expected || s.observedPending()!=0){waiting=true;}
        }
        return new Observation(failed!=null?Status.FAILED:waiting?Status.WAIT:Status.READY,
                failed!=null?failed:waiting?"pending_rpc_protocol_or_apply":"all_replicas_applied",evidence);
    }
}
