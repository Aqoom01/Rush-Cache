package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;

/**
 * 비용 고려 재배치: 예상 이득(줄어드는 초과 부하 × 지속 window 수)이 이동 비용(byte × 단가)보다 클 때만 옮긴다. 프로젝트 자체 설계다.
 *
 * <ul>
 *   <li>초과 부하(노드): max(0, 부하 − 용량 × 평균 이용률 × {@code tolerance}). 단위는 요청/window
 *   <li>이득: 이동 전후 두 노드의 초과 부하 합 감소 × {@code horizonWindows}(기본 5). 부하 패턴이 그 기간 유지된다는 가정이다
 *   <li>비용: Tablet byte × {@code costPerByte}(기본 0.001 요청/byte, 1MB를 옮기는 비용 ≈ 요청 1,000건으로 본다)
 *   <li>모든 (Tablet, 대상 노드) 중 이득 − 비용이 가장 큰 이동을 양수인 동안 반복한다
 * </ul>
 *
 * <p>단가와 기간은 실측 근거가 없는 가정값이다. 결과는 이 값에 따라 달라지므로 실험에서 함께 보고한다.
 */
public final class CostAwareRebalancing implements PlacementPolicy {

    public static final double DEFAULT_COST_PER_BYTE = 0.001;
    public static final int DEFAULT_HORIZON_WINDOWS = 5;
    public static final double DEFAULT_TOLERANCE = 1.05;

    private final double costPerByte;
    private final int horizonWindows;
    private final double tolerance;

    public CostAwareRebalancing() {
        this(DEFAULT_COST_PER_BYTE, DEFAULT_HORIZON_WINDOWS, DEFAULT_TOLERANCE);
    }

    public CostAwareRebalancing(double costPerByte, int horizonWindows, double tolerance) {
        if (costPerByte < 0 || horizonWindows < 1 || !(tolerance >= 1)) {
            throw new IllegalArgumentException("costPerByte >= 0, horizonWindows >= 1, tolerance >= 1");
        }
        this.costPerByte = costPerByte;
        this.horizonWindows = horizonWindows;
        this.tolerance = tolerance;
    }

    @Override
    public String variant() {
        return "cost-aware-rebalancing";
    }

    @Override
    public PlacementDecision decide(PlacementInput input) {
        Assignment working = new Assignment(input).drainDisallowed();
        while (true) {
            double target = working.averageUtilization() * tolerance;
            TabletId bestTablet = null;
            NodeId bestTo = null;
            double bestNet = 0;
            for (NodeId from : working.nodes()) {
                if (excess(input, working, from, 0, target) <= 0) {
                    continue;
                }
                for (TabletId id : working.tabletsOn(from)) {
                    long load = input.load(id);
                    for (NodeId to : working.nodes()) {
                        if (to.equals(from)) {
                            continue;
                        }
                        double before = excess(input, working, from, 0, target) + excess(input, working, to, 0, target);
                        double after = excess(input, working, from, -load, target) + excess(input, working, to, load, target);
                        double net = (before - after) * horizonWindows - input.bytes(id) * costPerByte;
                        if (net > bestNet) {
                            bestTablet = id;
                            bestTo = to;
                            bestNet = net;
                        }
                    }
                }
            }
            if (bestTablet == null) {
                return working.toDecision();
            }
            working.move(bestTablet, bestTo);
        }
    }

    private static double excess(PlacementInput input, Assignment working, NodeId node, long delta, double target) {
        return Math.max(0, working.load(node) + delta - input.nodeCapacity().get(node) * target);
    }
}
