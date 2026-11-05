package io.openkache.core.balance.placement;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;

/**
 * 용량 가중 Rendezvous(HRW) 해시: Tablet마다 노드별 점수 {@code -capacity / ln(u)}가 가장 큰 노드를 주인으로 정한다. {@code u}는
 * (Tablet, 노드) 해시를 (0, 1)로 바꾼 값이다.
 *
 * <p>참고: Thaler and Ravishankar, "Using Name-Based Mappings to Increase Hit Rates", IEEE/ACM ToN 1998(HRW). 용량 가중은
 * 가중 HRW에서 쓰는 로그 점수 형태를 따랐다(주인이 될 확률이 용량에 비례). 부하를 전혀 보지 않으며 Tablet 개수가 용량 비율에 따라
 * 기대값으로만 나뉜다.
 * 대신 노드가 바뀌어도 그 노드와 관련된 Tablet만 움직인다. 이동 비용·cooldown을 보지 않는다.
 */
public final class WeightedRendezvousPlacement implements PlacementPolicy {

    @Override
    public String variant() {
        return "weighted-rendezvous";
    }

    @Override
    public PlacementDecision decide(PlacementInput input) {
        Assignment working = new Assignment(input);
        for (TabletId id : working.tablets()) {
            NodeId best = null;
            double bestScore = Double.NEGATIVE_INFINITY;
            for (NodeId node : input.nodes()) {
                long h = PowerOfTwoChoicesPlacement.mix(id.value() * 0x9E3779B97F4A7C15L ^ PowerOfTwoChoicesPlacement.mix(node.value()));
                // 상위 53비트로 (0, 1) 사이 값을 만든다. 0이 나오지 않게 0.5를 더한다.
                double u = ((h >>> 11) + 0.5) / (1L << 53);
                double score = -input.nodeCapacity().get(node) / Math.log(u);
                if (score > bestScore) {
                    best = node;
                    bestScore = score;
                }
            }
            working.move(id, best);
        }
        return working.toDecision();
    }
}
