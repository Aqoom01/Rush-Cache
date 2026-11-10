package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;

/**
 * 히스테리시스 재배치: 불균형(최대 이용률 / 평균 이용률)이 {@code high}를 넘을 때만 재배치를 시작하고 {@code low} 아래가 되면 멈춘다.
 * 최근 {@code cooldownWindows} 안에 옮긴 Tablet은 다시 옮기지 않는다. 프로젝트 자체 설계다.
 *
 * <ul>
 *   <li>기본값: high 1.25, low 1.10, cooldown 3 window. 분할 후 유예(design.md §4.1, 3 window)와 같은 길이로 맞췄다
 *   <li>재배치: 가장 바쁜 노드에서 cooldown이 아닌 Tablet 중 가장 한가한 노드로 옮겼을 때 최대 이용률이 가장 많이 줄어드는 것을 고른다
 *   <li>빠지는 노드의 Tablet은 불균형·cooldown과 관계없이 옮긴다
 * </ul>
 *
 * <p>작은 흔들림에는 움직이지 않아 이동이 적은 대신, 불균형이 high 아래에서 오래 남을 수 있다.
 */
public final class HysteresisRebalancing implements PlacementPolicy {

    public static final double DEFAULT_HIGH = 1.25;
    public static final double DEFAULT_LOW = 1.10;
    public static final long DEFAULT_COOLDOWN_WINDOWS = 3;

    private final double high;
    private final double low;
    private final long cooldownWindows;

    public HysteresisRebalancing() {
        this(DEFAULT_HIGH, DEFAULT_LOW, DEFAULT_COOLDOWN_WINDOWS);
    }

    public HysteresisRebalancing(double high, double low, long cooldownWindows) {
        if (!(low >= 1) || !(high >= low) || cooldownWindows < 0) {
            throw new IllegalArgumentException("1 <= low <= high, cooldownWindows >= 0");
        }
        this.high = high;
        this.low = low;
        this.cooldownWindows = cooldownWindows;
    }

    @Override
    public String variant() {
        return "hysteresis-rebalancing";
    }

    @Override
    public PlacementDecision decide(PlacementInput input) {
        Assignment working = new Assignment(input).drainDisallowed();
        if (working.imbalance() <= high) {
            return working.toDecision();
        }
        while (working.imbalance() > low) {
            NodeId from = working.mostUtilized();
            NodeId to = working.leastUtilized();
            double current = working.maxUtilization();
            TabletId best = null;
            double bestMax = current;
            for (TabletId id : working.tabletsOn(from)) {
                if (input.inCooldown(id, cooldownWindows)) {
                    continue;
                }
                double after = working.maxUtilizationAfterMove(id, to);
                if (after < bestMax - 1e-12) {
                    best = id;
                    bestMax = after;
                }
            }
            if (best == null) {
                break;
            }
            working.move(best, to);
        }
        return working.toDecision();
    }
}
