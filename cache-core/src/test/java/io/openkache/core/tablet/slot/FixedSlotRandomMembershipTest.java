package io.openkache.core.tablet.slot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * 노드 추가·제거를 무작위 순서로 반복하며 fixed-slot의 규칙을 검사한다.
 *
 * <p>검사: 적용 후 노드 간 슬롯 수 차이 ≤ 1, 추가 시 이동 수 == S / (n+1) 이고 모두 새 노드로, 기존 노드끼리 이동 없음, 제거 시 떠나는
 * 노드 슬롯 수만큼 이동하고 모두 그 노드에서 나감, 이동하지 않은 슬롯은 주인·epoch 불변, 슬롯 경계는 절대 안 바뀜.
 */
class FixedSlotRandomMembershipTest {

    @Test
    void randomJoinsAndLeavesKeepBalanceAndRules() {
        for (int slots : new int[] {64, 1024, 4096}) {
            for (long seed = 1; seed <= 3; seed++) {
                run(new FixedSlotScheme(slots), new Random(seed), 60);
            }
        }
    }

    private static void run(FixedSlotScheme scheme, Random rnd, int steps) {
        List<NodeId> members = new ArrayList<>(List.of(new NodeId(1)));
        int nextId = 2;
        RoutingTable t = scheme.initial(members);
        List<Tablet> boundaries = t.tablets();

        for (int step = 0; step < steps; step++) {
            boolean add = members.size() == 1 || (members.size() < 12 && rnd.nextBoolean());
            Plan plan;
            if (add) {
                NodeId node = new NodeId(nextId++);
                plan = scheme.onNodeAdded(t, node);
                assertEquals(scheme.slots() / (members.size() + 1), plan.moves().size());
                for (Change c : plan.moves()) {
                    assertEquals(node, c.to());
                    assertTrue(members.contains(c.from()));
                }
                members.add(node);
            } else {
                NodeId node = members.remove(rnd.nextInt(members.size()));
                plan = scheme.onNodeRemoved(t, node);
                assertEquals(t.ownedBy(node).size(), plan.moves().size());
                for (Change c : plan.moves()) {
                    assertEquals(node, c.from());
                    assertTrue(members.contains(c.to()));
                }
            }
            assertTrue(plan.prepared() == t, "fixed-slot never changes boundaries");

            Set<Long> moved = new HashSet<>();
            for (Change c : plan.moves()) {
                assertTrue(moved.add(c.unit().value()), "a slot must not be moved twice in one plan");
            }
            RoutingTable next = plan.applyAll();

            for (int i = 0; i < scheme.slots(); i++) {
                Tablet before = t.tablets().get(i);
                Tablet after = next.tablets().get(i);
                assertEquals(boundaries.get(i).range(), after.range(), "slot boundaries must never change");
                if (!moved.contains((long) i)) {
                    assertEquals(before, after, "untouched slot changed: " + i);
                } else {
                    assertEquals(before.epoch().value() + 1, after.epoch().value());
                }
            }
            assertEquals(new HashSet<>(members), next.nodes());
            int min = Integer.MAX_VALUE;
            int max = 0;
            for (NodeId m : members) {
                int c = next.ownedBy(m).size();
                min = Math.min(min, c);
                max = Math.max(max, c);
            }
            assertTrue(max - min <= 1, "imbalance " + (max - min) + " with " + members.size() + " nodes, S=" + scheme.slots());
            assertFalse(members.isEmpty());
            t = next;
        }
    }
}
