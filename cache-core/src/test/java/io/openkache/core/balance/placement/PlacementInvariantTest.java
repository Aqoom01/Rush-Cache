package io.openkache.core.balance.placement;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 모든 배치 정책이 지켜야 하는 규칙(design.md §9.3)을 "정책 × 입력" 조합마다 검사한다. 새 정책은 {@link #POLICIES}에 추가한다.
 *
 * <p>여기서는 결정이 "올바른 모양"인지만 본다. 균형이 좋은지는 정책별 테스트와 Benchmark B에서 본다.
 */
@DisplayName("모든 배치 정책 공통 불변식 (design.md §9.3)")
class PlacementInvariantTest {

    static final List<PlacementPolicy> POLICIES = List.of(
            new GreedyLoadPlacement(),
            new SlicerInspiredPlacement(),
            new EStoreInspiredPlacement(),
            new PowerOfTwoChoicesPlacement(),
            new WeightedRendezvousPlacement(),
            new CostAwareRebalancing(),
            new HysteresisRebalancing());

    private static final List<NodeId> FOUR = Inputs.nodes(1, 2, 3, 4);
    private static final RoutingTable TABLE = Inputs.table(FOUR, 8);

    private static final List<Named<PlacementInput>> INPUTS = List.of(
            Named.of("균등 부하", Inputs.input(TABLE, i -> 100, Inputs.capacity(FOUR, 1000))),
            Named.of("노드 1에 부하 몰림", Inputs.input(TABLE, i -> i < 8 ? 400 : 50, Inputs.capacity(FOUR, 1000))),
            Named.of("Tablet 하나가 매우 뜨거움", Inputs.input(TABLE, i -> i == 3 ? 3000 : 20, Inputs.capacity(FOUR, 2000))),
            Named.of("요청 0건", Inputs.input(TABLE, i -> 0, Inputs.capacity(FOUR, 1000))),
            Named.of("용량이 다른 노드(1:1:2:4)", Inputs.input(TABLE, i -> 100 + i * 10L, heterogeneous())),
            Named.of("노드 4 제거(scale-in)", Inputs.input(TABLE, i -> 100, Inputs.capacity(Inputs.nodes(1, 2, 3), 1500))),
            Named.of("노드 5 추가(scale-out)", Inputs.input(TABLE, i -> 100, Inputs.capacity(Inputs.nodes(1, 2, 3, 4, 5), 1000))));

    static Stream<Arguments> everyPolicyWithEveryInput() {
        return POLICIES.stream().flatMap(policy -> INPUTS.stream().map(input -> Arguments.of(Named.of(policy.variant(), policy), input)));
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithEveryInput")
    @DisplayName("이동마다 from은 현재 주인, to는 허용 노드이고 둘은 다르며, Tablet당 많아야 한 건이다")
    void movesAreWellFormed(PlacementPolicy policy, PlacementInput input) {
        List<Change> moves = policy.decide(input).moves();

        Set<TabletId> seen = new HashSet<>();
        for (Change move : moves) {
            assertEquals(input.table().get(move.unit()).owner(), move.from(), "from은 현재 주인");
            assertTrue(input.nodeCapacity().containsKey(move.to()), "허용되지 않은 노드로 옮김: " + move.to());
            assertNotEquals(move.from(), move.to());
            assertTrue(seen.add(move.unit()), "같은 Tablet을 두 번 옮김: " + move.unit());
        }
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithEveryInput")
    @DisplayName("적용 후 표가 공간 전체를 빈틈·겹침 없이 덮고, 모든 Tablet의 주인이 정확히 하나의 허용 노드다")
    void appliedTableKeepsOwnershipValid(PlacementPolicy policy, PlacementInput input) {
        RoutingTable after = assertDoesNotThrow(() -> policy.decide(input).toPlan(input.table()).applyAll());

        assertEquals(input.table().size(), after.size(), "Tablet이 생기거나 사라지면 안 된다");
        for (Tablet tablet : after.tablets()) {
            assertTrue(input.nodeCapacity().containsKey(tablet.owner()), tablet.id() + "의 주인이 허용 노드가 아니다");
        }
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithEveryInput")
    @DisplayName("같은 입력에는 같은 결정을 내고, 입력을 바꾸지 않는다")
    void deterministicAndDoesNotMutateInput(PlacementPolicy policy, PlacementInput input) {
        List<Tablet> before = List.copyOf(input.table().tablets());
        Map<TabletId, Long> loadBefore = Map.copyOf(input.tabletLoad());

        assertEquals(policy.decide(input), policy.decide(input));
        assertEquals(before, input.table().tablets());
        assertEquals(loadBefore, input.tabletLoad());
    }

    private static Map<NodeId, Double> heterogeneous() {
        Map<NodeId, Double> capacity = new LinkedHashMap<>();
        capacity.put(new NodeId(1), 500.0);
        capacity.put(new NodeId(2), 500.0);
        capacity.put(new NodeId(3), 1000.0);
        capacity.put(new NodeId(4), 2000.0);
        return capacity;
    }
}
