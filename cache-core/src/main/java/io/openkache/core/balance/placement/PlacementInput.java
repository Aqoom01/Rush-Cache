package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 배치 정책의 입력: 현재 라우팅 표와 window 하나의 Tablet 부하, 노드 용량, 이동 이력.
 *
 * <p>불변이다. 정책은 이 값을 바꾸지 않는다. 맵의 순회 순서에 의존하지 않도록 {@link #tablets()}·{@link #nodes()}는 정렬된 목록을
 * 준다.
 *
 * @param table 현재 라우팅 표(B1). 주인이 {@code nodeCapacity}에 없는 Tablet은 그 노드가 빠지는 중이라는 뜻이고, 정책은 이 Tablet을
 *     허용된 노드로 옮겨야 한다
 * @param tabletLoad Tablet별 이 window의 요청 수(0 이상). 표의 모든 Tablet이 있어야 한다
 * @param tabletBytes Tablet별 데이터 크기(byte, 0 이상). 이동 비용 추정에 쓴다. 표의 모든 Tablet이 있어야 한다
 * @param nodeCapacity 배치할 수 있는 노드와 window당 처리 용량(요청 수, 0 초과). 이 목록 밖의 노드로는 옮기지 않는다
 * @param window 지금 window 번호(논리 시간, 0 이상)
 * @param lastMovedWindow Tablet별 마지막으로 옮긴 window. 옮긴 적 없으면 없다. cooldown 판단에 쓴다
 */
public record PlacementInput(
        RoutingTable table,
        Map<TabletId, Long> tabletLoad,
        Map<TabletId, Long> tabletBytes,
        Map<NodeId, Double> nodeCapacity,
        long window,
        Map<TabletId, Long> lastMovedWindow) {

    public PlacementInput {
        Objects.requireNonNull(table, "table");
        tabletLoad = Map.copyOf(tabletLoad);
        tabletBytes = Map.copyOf(tabletBytes);
        nodeCapacity = Map.copyOf(nodeCapacity);
        lastMovedWindow = Map.copyOf(lastMovedWindow);
        if (nodeCapacity.isEmpty()) {
            throw new IllegalArgumentException("nodeCapacity must not be empty");
        }
        for (Map.Entry<NodeId, Double> e : nodeCapacity.entrySet()) {
            if (!(e.getValue() > 0)) {
                throw new IllegalArgumentException("capacity must be > 0: " + e);
            }
        }
        for (Tablet tablet : table.tablets()) {
            Long load = tabletLoad.get(tablet.id());
            Long bytes = tabletBytes.get(tablet.id());
            if (load == null || load < 0 || bytes == null || bytes < 0) {
                throw new IllegalArgumentException("load and bytes must be >= 0 for every tablet: " + tablet.id());
            }
        }
        if (window < 0) {
            throw new IllegalArgumentException("window must be >= 0: " + window);
        }
    }

    /** 시작점 오름차순의 Tablet 목록(표 순서). */
    public List<Tablet> tablets() {
        return table.tablets();
    }

    /** 배치할 수 있는 노드. NodeId 오름차순. */
    public List<NodeId> nodes() {
        return nodeCapacity.keySet().stream().sorted(Comparator.comparingLong(NodeId::value)).toList();
    }

    public long load(TabletId id) {
        return tabletLoad.get(id);
    }

    public long bytes(TabletId id) {
        return tabletBytes.get(id);
    }

    /** {@code id}를 {@code window} 기준 최근 {@code cooldownWindows} window 안에 옮겼으면 {@code true}. */
    public boolean inCooldown(TabletId id, long cooldownWindows) {
        Long last = lastMovedWindow.get(id);
        return last != null && window - last < cooldownWindows;
    }
}
