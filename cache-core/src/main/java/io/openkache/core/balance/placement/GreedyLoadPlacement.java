package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Baseline: 현재 배치를 무시하고, 부하가 큰 Tablet부터 그 시점에 "넣은 뒤 이용률이 가장 낮은" 노드에 다시 놓는다(LPT greedy).
 *
 * <p>균형만 보고 이동 비용·현재 위치·cooldown을 보지 않는다. 그래서 균형은 좋지만 부하가 조금만 바뀌어도 많은 Tablet이 움직일 수
 * 있다. 다른 배치 정책은 이 정책 대비로 평가한다.
 */
public final class GreedyLoadPlacement implements PlacementPolicy {

    @Override
    public String variant() {
        return "greedy-load-placement";
    }

    @Override
    public PlacementDecision decide(PlacementInput input) {
        Assignment working = new Assignment(input);
        List<TabletId> order = working.tablets().stream().sorted(working.byLoadDescending()).toList();
        Map<NodeId, Long> filled = new TreeMap<>(Comparator.comparingLong(NodeId::value));
        input.nodes().forEach(node -> filled.put(node, 0L));
        for (TabletId id : order) {
            long load = input.load(id);
            NodeId best = null;
            for (NodeId node : input.nodes()) {
                if (best == null || after(input, filled, node, load) < after(input, filled, best, load)) {
                    best = node;
                }
            }
            filled.merge(best, load, Long::sum);
            working.move(id, best);
        }
        return working.toDecision();
    }

    private static double after(PlacementInput input, Map<NodeId, Long> filled, NodeId node, long load) {
        return (filled.get(node) + load) / input.nodeCapacity().get(node);
    }
}
