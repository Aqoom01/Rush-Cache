package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.Tablet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 정책이 계획을 세우는 동안 쓰는 작업용 배치. 입력 표는 그대로 두고 이 사본만 바꾼다. 한 정책 호출 안에서만 쓰고 공유하지 않는다.
 *
 * <p>이용률은 노드 부하 / 용량이다. 평균 이용률은 전체 부하 / 전체 용량이라 용량이 다른 노드도 같은 기준으로 비교된다.
 */
final class Assignment {

    private final PlacementInput input;
    private final Map<TabletId, NodeId> original = new LinkedHashMap<>();
    private final Map<TabletId, NodeId> owner = new LinkedHashMap<>();
    private final Map<NodeId, Long> nodeLoad = new TreeMap<>(Comparator.comparingLong(NodeId::value));
    private final Set<TabletId> moved = new HashSet<>();
    private long movedBytes;

    Assignment(PlacementInput input) {
        this.input = input;
        for (NodeId node : input.nodes()) {
            nodeLoad.put(node, 0L);
        }
        for (Tablet tablet : input.tablets()) {
            original.put(tablet.id(), tablet.owner());
            owner.put(tablet.id(), tablet.owner());
            nodeLoad.computeIfPresent(tablet.owner(), (n, l) -> l + input.load(tablet.id()));
        }
    }

    /** 허용되지 않은 노드의 Tablet을 부하 큰 것부터 "옮긴 뒤 이용률이 가장 낮은" 허용 노드로 옮긴다. 모든 정책의 첫 단계다. */
    Assignment drainDisallowed() {
        List<TabletId> orphans = new ArrayList<>();
        for (Map.Entry<TabletId, NodeId> e : owner.entrySet()) {
            if (!nodeLoad.containsKey(e.getValue())) {
                orphans.add(e.getKey());
            }
        }
        orphans.sort(byLoadDescending());
        for (TabletId id : orphans) {
            move(id, leastUtilizedAfterAdding(input.load(id)));
        }
        return this;
    }

    void move(TabletId id, NodeId to) {
        NodeId from = owner.get(id);
        if (from.equals(to)) {
            return;
        }
        long load = input.load(id);
        nodeLoad.computeIfPresent(from, (n, l) -> l - load);
        nodeLoad.merge(to, load, Long::sum);
        owner.put(id, to);
        if (moved.add(id)) {
            movedBytes += input.bytes(id);
        }
    }

    NodeId owner(TabletId id) {
        return owner.get(id);
    }

    long load(NodeId node) {
        return nodeLoad.get(node);
    }

    double utilization(NodeId node) {
        return nodeLoad.get(node) / input.nodeCapacity().get(node);
    }

    /** {@code delta}만큼 더하거나 뺀 뒤의 이용률. */
    double utilizationAfter(NodeId node, long delta) {
        return (nodeLoad.get(node) + delta) / input.nodeCapacity().get(node);
    }

    /** 전체 부하 / 전체 용량. */
    double averageUtilization() {
        long total = nodeLoad.values().stream().mapToLong(Long::longValue).sum();
        double capacity = input.nodeCapacity().values().stream().mapToDouble(Double::doubleValue).sum();
        return total / capacity;
    }

    double maxUtilization() {
        return nodeLoad.keySet().stream().mapToDouble(this::utilization).max().orElse(0);
    }

    /** {@code id}를 지금 주인에서 {@code to}로 옮긴다고 가정했을 때의 최대 이용률. 실제로 옮기지 않는다. */
    double maxUtilizationAfterMove(TabletId id, NodeId to) {
        NodeId from = owner.get(id);
        if (from.equals(to)) {
            return maxUtilization();
        }
        long load = input.load(id);
        double max = 0;
        for (NodeId node : nodeLoad.keySet()) {
            long delta = node.equals(from) ? -load : node.equals(to) ? load : 0;
            max = Math.max(max, utilizationAfter(node, delta));
        }
        return max;
    }

    /** 최대 이용률 / 평균 이용률. 부하가 없으면 1. */
    double imbalance() {
        double average = averageUtilization();
        return average == 0 ? 1.0 : maxUtilization() / average;
    }

    NodeId mostUtilized() {
        NodeId best = null;
        for (NodeId node : nodeLoad.keySet()) {
            if (best == null || utilization(node) > utilization(best)) {
                best = node;
            }
        }
        return best;
    }

    NodeId leastUtilized() {
        return leastUtilizedAfterAdding(0);
    }

    /** {@code load}를 더했을 때 이용률이 가장 낮아지는 노드. 같으면 NodeId가 작은 쪽. */
    NodeId leastUtilizedAfterAdding(long load) {
        NodeId best = null;
        for (NodeId node : nodeLoad.keySet()) {
            if (best == null || utilizationAfter(node, load) < utilizationAfter(best, load)) {
                best = node;
            }
        }
        return best;
    }

    /** 허용 노드. NodeId 오름차순. */
    List<NodeId> nodes() {
        return List.copyOf(nodeLoad.keySet());
    }

    /** 지금 {@code node}에 있는 Tablet. 표 순서. */
    List<TabletId> tabletsOn(NodeId node) {
        List<TabletId> result = new ArrayList<>();
        owner.forEach((id, n) -> {
            if (n.equals(node)) {
                result.add(id);
            }
        });
        return result;
    }

    List<TabletId> tablets() {
        return List.copyOf(owner.keySet());
    }

    /** 이 계획에서 한 번이라도 옮긴 Tablet의 크기 합. */
    long movedBytes() {
        return movedBytes;
    }

    Comparator<TabletId> byLoadDescending() {
        return Comparator.comparingLong((TabletId id) -> input.load(id)).reversed().thenComparingLong(TabletId::value);
    }

    /** 처음과 주인이 달라진 Tablet만 이동으로 만든다. 같은 Tablet을 여러 번 옮겼어도 한 건이다. */
    PlacementDecision toDecision() {
        List<Change> changes = new ArrayList<>();
        original.forEach((id, from) -> {
            NodeId to = owner.get(id);
            if (!from.equals(to)) {
                changes.add(new Change(id, from, to));
            }
        });
        return new PlacementDecision(changes);
    }
}
