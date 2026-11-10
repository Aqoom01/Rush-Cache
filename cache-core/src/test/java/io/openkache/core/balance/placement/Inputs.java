package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntToLongFunction;
import java.util.stream.LongStream;

/** 배치 정책 테스트용 입력을 만든다. 처음 표는 B1 {@link HashRangeScheme}로 노드당 균등 구간을 연속 블록으로 둔다. */
final class Inputs {

    private Inputs() {}

    static List<NodeId> nodes(long... ids) {
        return LongStream.of(ids).mapToObj(NodeId::new).toList();
    }

    /** 노드 {@code tableNodes}에 노드당 {@code perNode}개씩 놓은 표. */
    static RoutingTable table(List<NodeId> tableNodes, int perNode) {
        return new HashRangeScheme(perNode).initial(tableNodes);
    }

    /**
     * @param loadOf 표 순서 {@code i}번 Tablet의 부하
     * @param capacity 배치 가능 노드와 용량
     */
    static PlacementInput input(RoutingTable table, IntToLongFunction loadOf, Map<NodeId, Double> capacity) {
        return input(table, loadOf, capacity, 0, Map.of());
    }

    static PlacementInput input(
            RoutingTable table, IntToLongFunction loadOf, Map<NodeId, Double> capacity, long window, Map<TabletId, Long> lastMoved) {
        Map<TabletId, Long> load = new HashMap<>();
        Map<TabletId, Long> bytes = new HashMap<>();
        List<Tablet> tablets = table.tablets();
        for (int i = 0; i < tablets.size(); i++) {
            load.put(tablets.get(i).id(), loadOf.applyAsLong(i));
            bytes.put(tablets.get(i).id(), 1_000_000L);
        }
        return new PlacementInput(table, load, bytes, capacity, window, lastMoved);
    }

    /** 같은 용량 {@code capacity}의 노드들. */
    static Map<NodeId, Double> capacity(List<NodeId> nodes, double capacity) {
        Map<NodeId, Double> result = new LinkedHashMap<>();
        nodes.forEach(node -> result.put(node, capacity));
        return result;
    }
}
