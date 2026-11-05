package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;

/**
 * Google Slicer에서 착안한 배치: 현재 배치에서 출발해, 이동량(churn) 예산 안에서 "byte당 최대 이용률 감소"가 가장 큰 이동을
 * 반복한다.
 *
 * <p>참고: Adya et al., "Slicer: Auto-Sharding for Datacenter Applications", OSDI 2016. Slicer는 부하 균형을 맞추되 한 번에
 * 움직이는 key 공간(churn)에 상한을 두고(weighted-move, §4.4.1: 조정 한 번에 key 이동 9%), 이득이 큰 이동부터 고른다. 이 정책은
 * 그 두 가지만 가져왔다. 예산을 key 공간이 아니라 데이터 byte로 재는 것과 아래 점수는 프로젝트 자체 결정이다
 * ({@code experiments/b2-load-balancing/references/slicer.md}).
 *
 * <ul>
 *   <li>이동 예산: 한 결정에서 옮길 수 있는 byte 합 = 전체 Tablet byte × {@code churnBudget}(기본 0.09, Slicer의 9%)
 *   <li>후보: 가장 바쁜 노드의 Tablet을 가장 한가한 노드로 옮기는 이동. 점수 = 최대 이용률 감소 / max(byte, 1)
 *   <li>최대 이용률이 줄지 않거나 예산을 넘으면 멈춘다
 * </ul>
 */
public final class SlicerInspiredPlacement implements PlacementPolicy {

    public static final double DEFAULT_CHURN_BUDGET = 0.09;

    private final double churnBudget;

    public SlicerInspiredPlacement() {
        this(DEFAULT_CHURN_BUDGET);
    }

    /** @param churnBudget 전체 byte 대비 한 결정의 이동 상한. 0 초과 1 이하 */
    public SlicerInspiredPlacement(double churnBudget) {
        if (!(churnBudget > 0 && churnBudget <= 1)) {
            throw new IllegalArgumentException("churnBudget must be in (0, 1]: " + churnBudget);
        }
        this.churnBudget = churnBudget;
    }

    @Override
    public String variant() {
        return "slicer-inspired";
    }

    @Override
    public PlacementDecision decide(PlacementInput input) {
        Assignment working = new Assignment(input).drainDisallowed();
        long totalBytes = input.tabletBytes().values().stream().mapToLong(Long::longValue).sum();
        double budget = totalBytes * churnBudget;
        while (true) {
            NodeId from = working.mostUtilized();
            NodeId to = working.leastUtilized();
            double current = working.maxUtilization();
            TabletId best = null;
            double bestScore = 0;
            for (TabletId id : working.tabletsOn(from)) {
                if (working.movedBytes() + input.bytes(id) > budget) {
                    continue;
                }
                double gain = current - working.maxUtilizationAfterMove(id, to);
                double score = gain / Math.max(input.bytes(id), 1);
                if (gain > 1e-12 && score > bestScore) {
                    best = id;
                    bestScore = score;
                }
            }
            if (best == null) {
                return working.toDecision();
            }
            working.move(best, to);
        }
    }
}
