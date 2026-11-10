package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import java.util.List;

/**
 * Power of two choices: 과부하 노드의 Tablet마다 후보 노드 두 개를 골라, 옮긴 뒤 이용률이 낮은 쪽으로 옮긴다(나아질 때만).
 *
 * <p>참고: Mitzenmacher, "The Power of Two Choices in Randomized Load Balancing", IEEE TPDS 2001. 전체 노드를 다 보지 않고 두
 * 곳만 비교한다. 후보는 난수 대신 (Tablet 식별자, window, {@code seed})의 해시로 골라 같은 입력이면 같은 결정을 낸다. 과부하 기준
 * {@code tolerance}(기본 1.05)와 한 번의 훑기는 프로젝트 자체 결정이다.
 *
 * <ul>
 *   <li>부하 큰 Tablet부터 한 번 훑는다. 주인의 이용률이 평균 × tolerance를 넘을 때만 후보를 본다
 *   <li>후보 두 개 중 옮긴 뒤 이용률이 낮은 쪽이, 지금 주인의 이용률보다 낮으면 옮긴다
 * </ul>
 */
public final class PowerOfTwoChoicesPlacement implements PlacementPolicy {

    public static final double DEFAULT_TOLERANCE = 1.05;

    private final long seed;
    private final double tolerance;

    public PowerOfTwoChoicesPlacement() {
        this(0, DEFAULT_TOLERANCE);
    }

    public PowerOfTwoChoicesPlacement(long seed, double tolerance) {
        if (!(tolerance >= 1)) {
            throw new IllegalArgumentException("tolerance must be >= 1: " + tolerance);
        }
        this.seed = seed;
        this.tolerance = tolerance;
    }

    @Override
    public String variant() {
        return "power-of-two-choices";
    }

    @Override
    public PlacementDecision decide(PlacementInput input) {
        Assignment working = new Assignment(input).drainDisallowed();
        List<NodeId> nodes = working.nodes();
        for (TabletId id : working.tablets().stream().sorted(working.byLoadDescending()).toList()) {
            NodeId owner = working.owner(id);
            if (nodes.size() < 2 || working.utilization(owner) <= working.averageUtilization() * tolerance) {
                continue;
            }
            List<NodeId> others = nodes.stream().filter(n -> !n.equals(owner)).toList();
            long h = mix(id.value() * 0x9E3779B97F4A7C15L ^ input.window() * 0xC2B2AE3D27D4EB4FL ^ seed);
            NodeId a = others.get((int) Long.remainderUnsigned(h, others.size()));
            NodeId b = others.get((int) Long.remainderUnsigned(mix(h), others.size()));
            long load = input.load(id);
            NodeId to = working.utilizationAfter(b, load) < working.utilizationAfter(a, load) ? b : a;
            if (working.utilizationAfter(to, load) < working.utilization(owner)) {
                working.move(id, to);
            }
        }
        return working.toDecision();
    }

    /** SplitMix64의 마무리 단계. 입력이 같으면 같은 값을 낸다. */
    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
