package io.openkache.core.balance.placement;

import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import java.util.List;

/**
 * 배치 정책의 결정: 옮길 Tablet 목록. 계획일 뿐이며 데이터 이동은 B3({@code migration})가, 표 반영은 B1
 * {@link RoutingTable#apply(Change)}가 한다.
 *
 * @param moves Tablet당 많아야 한 건. {@code from}은 입력 표의 현재 주인이다
 */
public record PlacementDecision(List<Change> moves) {

    public PlacementDecision {
        moves = List.copyOf(moves);
    }

    /** 경계를 바꾸지 않는 이동 계획(B1 {@link Plan}). */
    public Plan toPlan(RoutingTable table) {
        return Plan.movesOnly(table, moves);
    }
}
