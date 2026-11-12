package io.openkache.core.replication.ionia;

import io.openkache.core.model.NodeId;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** O00 정적 복제 그룹. leaderId는 최초 view의 Leader이며 이후 고정 그룹 순서로 회전한다. */
public record IoniaConfig(NodeId nodeId, NodeId leaderId, List<NodeId> replicas, long view) {
    /** 기본 실험 크기. 실제 정족수는 설정된 복제본 수로 계산한다. */
    public static final int REPLICA_COUNT = 3;
    public IoniaConfig {
        Objects.requireNonNull(nodeId, "nodeId"); Objects.requireNonNull(leaderId, "leaderId");
        replicas = List.copyOf(Objects.requireNonNull(replicas, "replicas"));
        if (replicas.size() < 3 || replicas.size() % 2 == 0) throw new IllegalArgumentException("replicas must contain an odd number of at least three nodes");
        if (new HashSet<>(replicas).size() != replicas.size()) throw new IllegalArgumentException("replicas must contain distinct nodes");
        if (!replicas.contains(nodeId) || !replicas.contains(leaderId)) throw new IllegalArgumentException("node and leader must belong to replicas");
        if (view < 0) throw new IllegalArgumentException("view must be non-negative");
    }
    public int failures() { return replicas.size() / 2; }
    public int recoveryQuorumSize() { return (failures() + 1) / 2 + 1; }
    public int fastWriteQuorumSize() { return failures() + (failures() + 1) / 2 + 1; }
    public int orderingQuorumSize() { return failures() + 1; }
    /** 최초 View의 역할이며 현재 역할은 IoniaEngine.viewInfo()로 확인한다. */
    public boolean isLeader() { return nodeId.equals(leaderId); }
    public NodeId leaderFor(long targetView) {
        int offset = (int) Math.floorMod(targetView - view, (long) replicas.size());
        return replicas.get((replicas.indexOf(leaderId) + offset) % replicas.size());
    }
    /** 그룹 순서와 최초 Leader/View도 설정 ID에 포함해 다른 설정의 메시지를 섞지 않는다. */
    public String configurationId() {
        return "ionia:" + view + ":" + leaderId.value() + ":" + replicas.stream().map(node -> Long.toString(node.value())).toList();
    }
}
