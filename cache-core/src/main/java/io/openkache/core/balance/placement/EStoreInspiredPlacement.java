package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import java.util.List;

/**
 * E-Store에서 착안한 배치: 과부하 노드에서 hot Tablet을 먼저 옮기고, 그래도 넘치면 cold Tablet을 옮긴다.
 *
 * <p>참고: Taft et al., "E-Store", PVLDB 8(3), 2015. E-Store는 hot tuple을 먼저 개별로 재배치하고 cold 데이터는 큰 블록으로
 * 옮긴다(greedy, first-fit 계열). 이 정책은 "hot 우선, cold는 나중"의 순서만 가져왔다. 아래 기준값은 논문에 없는 프로젝트
 * 자체 결정이다({@code experiments/b2-load-balancing/references/estore.md}).
 *
 * <ul>
 *   <li>hot Tablet: 부하가 Tablet 평균의 {@code hotFactor}배(기본 2.0) 이상
 *   <li>과부하 노드: 이용률이 평균 이용률 × {@code tolerance}(기본 1.05)를 넘는 노드
 *   <li>1단계: 가장 바쁜 과부하 노드의 hot Tablet을 부하 큰 것부터, 옮긴 뒤 최대 이용률이 줄어들 때만 가장 한가한 노드로 옮긴다
 *   <li>2단계: 아직 과부하 노드가 있으면 같은 규칙으로 cold Tablet을 옮긴다
 * </ul>
 */
public final class EStoreInspiredPlacement implements PlacementPolicy {

    public static final double DEFAULT_HOT_FACTOR = 2.0;
    public static final double DEFAULT_TOLERANCE = 1.05;

    private final double hotFactor;
    private final double tolerance;

    public EStoreInspiredPlacement() {
        this(DEFAULT_HOT_FACTOR, DEFAULT_TOLERANCE);
    }

    public EStoreInspiredPlacement(double hotFactor, double tolerance) {
        if (!(hotFactor > 1) || !(tolerance >= 1)) {
            throw new IllegalArgumentException("hotFactor > 1, tolerance >= 1: " + hotFactor + ", " + tolerance);
        }
        this.hotFactor = hotFactor;
        this.tolerance = tolerance;
    }

    @Override
    public String variant() {
        return "estore-inspired";
    }

    @Override
    public PlacementDecision decide(PlacementInput input) {
        Assignment working = new Assignment(input).drainDisallowed();
        List<TabletId> tablets = working.tablets();
        double meanLoad = tablets.stream().mapToLong(input::load).average().orElse(0);
        for (boolean hotPhase : new boolean[] {true, false}) {
            while (working.maxUtilization() > working.averageUtilization() * tolerance) {
                NodeId from = working.mostUtilized();
                NodeId to = working.leastUtilized();
                double current = working.maxUtilization();
                TabletId chosen = null;
                for (TabletId id : working.tabletsOn(from).stream().sorted(working.byLoadDescending()).toList()) {
                    boolean hot = input.load(id) >= hotFactor * meanLoad && input.load(id) > 0;
                    if (hot == hotPhase && working.maxUtilizationAfterMove(id, to) < current - 1e-12) {
                        chosen = id;
                        break;
                    }
                }
                if (chosen == null) {
                    break;
                }
                working.move(chosen, to);
            }
        }
        return working.toDecision();
    }
}
