package io.openkache.core.balance.placement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 정책별 성질: 균형 개선, 이동 예산, cooldown, 이동 비용, 노드 변화 시 이동 범위. */
@DisplayName("배치 정책별 성질")
class PlacementPolicyTest {

    @Test
    void singleAllowedNodeWithNoActualMoveTerminatesForEveryPolicy() {
        List<NodeId> one = Inputs.nodes(1);
        PlacementInput input = Inputs.input(Inputs.table(one, 16), i -> 10 + i, Inputs.capacity(one, 1000));
        for (PlacementPolicy policy : List.of(new GreedyLoadPlacement(), new SlicerInspiredPlacement(), new EStoreInspiredPlacement(),
                new PowerOfTwoChoicesPlacement(), new CostAwareRebalancing(), new HysteresisRebalancing(), new WeightedRendezvousPlacement())) {
            var decision = assertTimeoutPreemptively(Duration.ofSeconds(1), () -> policy.decide(input), policy.variant());
            assertTrue(decision.moves().isEmpty(), policy.variant());
        }
    }

    @Test
    void everyPolicyDrainsDisallowedNodesIntoTheLastRemainingNode() {
        List<NodeId> nodes = Inputs.nodes(1, 2, 3);
        RoutingTable table = Inputs.table(nodes, 8);
        PlacementInput input = Inputs.input(table, i -> 10 + i, Inputs.capacity(Inputs.nodes(1), 1000));
        var orphans = table.tablets().stream().filter(t -> !t.owner().equals(new NodeId(1))).map(Tablet::id).collect(java.util.stream.Collectors.toSet());
        for (PlacementPolicy policy : List.of(new GreedyLoadPlacement(), new SlicerInspiredPlacement(), new EStoreInspiredPlacement(),
                new PowerOfTwoChoicesPlacement(), new CostAwareRebalancing(), new HysteresisRebalancing(), new WeightedRendezvousPlacement())) {
            var decision = assertTimeoutPreemptively(Duration.ofSeconds(1), () -> policy.decide(input), policy.variant());
            assertEquals(orphans, decision.moves().stream().map(Change::unit).collect(java.util.stream.Collectors.toSet()), policy.variant());
            assertTrue(decision.moves().stream().allMatch(c -> c.to().equals(new NodeId(1))), policy.variant());
        }
    }

    private static final List<NodeId> FOUR = Inputs.nodes(1, 2, 3, 4);
    private static final RoutingTable TABLE = Inputs.table(FOUR, 8);
    /** 노드 1(Tablet 0-7)만 400, 나머지 50. 전체 4,400 → 노드 1 이용률 3.2, 평균 1.1. */
    private static final PlacementInput SKEWED = Inputs.input(TABLE, i -> i < 8 ? 400 : 50, Inputs.capacity(FOUR, 1000));

    @Test
    @DisplayName("부하 기반 정책은 모두 몰린 부하에서 최대 이용률을 낮춘다")
    void loadAwarePoliciesReduceMaxUtilization() {
        double before = maxUtilization(SKEWED, List.of());
        for (PlacementPolicy policy : List.of(
                new GreedyLoadPlacement(), new SlicerInspiredPlacement(1.0), new EStoreInspiredPlacement(),
                new PowerOfTwoChoicesPlacement(), new CostAwareRebalancing(), new HysteresisRebalancing())) {
            double after = maxUtilization(SKEWED, policy.decide(SKEWED).moves());
            assertTrue(after < before, policy.variant() + ": " + before + " -> " + after);
        }
    }

    @Test
    @DisplayName("greedy는 몰린 부하를 거의 완전히 고르게 만든다(Baseline 성질)")
    void greedyBalances() {
        double after = maxUtilization(SKEWED, new GreedyLoadPlacement().decide(SKEWED).moves());
        assertEquals(1.1, after, 0.001); // 노드마다 400 x 2 + 50 x 6 = 1,100
    }

    @Test
    @DisplayName("slicer-inspired는 이동 byte가 예산(전체의 churnBudget)을 넘지 않는다")
    void slicerRespectsChurnBudget() {
        // 32개 x 1MB, 예산 10% = 3.2MB → 많아야 3개
        List<Change> moves = new SlicerInspiredPlacement(0.1).decide(SKEWED).moves();
        assertTrue(moves.size() <= 3, "moves=" + moves.size());
        assertTrue(!moves.isEmpty());
    }

    @Test
    @DisplayName("hysteresis는 불균형이 high 이하면 옮기지 않고, cooldown 중인 Tablet은 옮기지 않는다")
    void hysteresisHonorsThresholdAndCooldown() {
        PlacementInput mild = Inputs.input(TABLE, i -> i < 8 ? 120 : 100, Inputs.capacity(FOUR, 1000));
        assertTrue(new HysteresisRebalancing().decide(mild).moves().isEmpty(), "불균형 약 1.14 < 1.25");

        Map<TabletId, Long> lastMoved = new HashMap<>();
        TABLE.ownedBy(new NodeId(1)).forEach(t -> lastMoved.put(t.id(), 9L));
        PlacementInput cooling = Inputs.input(TABLE, i -> i < 8 ? 400 : 50, Inputs.capacity(FOUR, 1000), 10, lastMoved);
        assertTrue(new HysteresisRebalancing().decide(cooling).moves().isEmpty(), "노드 1 Tablet은 모두 cooldown");

        PlacementInput cooled = Inputs.input(TABLE, i -> i < 8 ? 400 : 50, Inputs.capacity(FOUR, 1000), 12, lastMoved);
        assertTrue(!new HysteresisRebalancing().decide(cooled).moves().isEmpty(), "3 window가 지나면 다시 옮긴다");
    }

    @Test
    @DisplayName("cost-aware는 이동 비용이 이득보다 크면 옮기지 않는다")
    void costAwareSkipsExpensiveMoves() {
        assertTrue(new CostAwareRebalancing(1.0, 5, 1.05).decide(SKEWED).moves().isEmpty(), "1MB x 1.0 >> 이득");
        assertTrue(!new CostAwareRebalancing(0.0001, 5, 1.05).decide(SKEWED).moves().isEmpty());
    }

    @Test
    @DisplayName("weighted-rendezvous는 노드 하나를 빼면 그 노드의 Tablet만 움직인다")
    void rendezvousMovesOnlyDepartingNodesTablets() {
        PlacementPolicy hrw = new WeightedRendezvousPlacement();
        RoutingTable settled = Inputs.input(TABLE, i -> 100, Inputs.capacity(FOUR, 1000)).table();
        settled = hrw.decide(Inputs.input(settled, i -> 100, Inputs.capacity(FOUR, 1000))).toPlan(settled).applyAll();

        PlacementInput withoutFour = Inputs.input(settled, i -> 100, Inputs.capacity(Inputs.nodes(1, 2, 3), 1000));
        for (Change move : hrw.decide(withoutFour).moves()) {
            assertEquals(new NodeId(4), move.from(), "노드 4가 아닌 Tablet이 움직였다: " + move);
        }
    }

    @Test
    @DisplayName("weighted-rendezvous는 용량에 비례해 Tablet 개수를 나눈다(기대값)")
    void rendezvousFollowsCapacity() {
        List<NodeId> two = Inputs.nodes(1, 2);
        RoutingTable many = Inputs.table(two, 500);
        Map<NodeId, Double> capacity = Map.of(new NodeId(1), 1000.0, new NodeId(2), 3000.0);
        RoutingTable after = new WeightedRendezvousPlacement()
                .decide(Inputs.input(many, i -> 1, capacity)).toPlan(many).applyAll();
        double shareOfTwo = after.ownedBy(new NodeId(2)).size() / (double) after.size();
        assertEquals(0.75, shareOfTwo, 0.05);
    }

    private static double maxUtilization(PlacementInput input, List<Change> moves) {
        RoutingTable after = applied(input, moves);
        Map<NodeId, Long> load = new HashMap<>();
        for (Tablet t : after.tablets()) {
            load.merge(t.owner(), input.load(t.id()), Long::sum);
        }
        return load.entrySet().stream().mapToDouble(e -> e.getValue() / input.nodeCapacity().get(e.getKey())).max().orElse(0);
    }

    private static RoutingTable applied(PlacementInput input, List<Change> moves) {
        return new PlacementDecision(moves).toPlan(input.table()).applyAll();
    }
}
