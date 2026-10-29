package io.openkache.core.tablet.resize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import io.openkache.core.tablet.resize.ResizableSlotScheme.Resize;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * 노드 추가·제거를 무작위로 반복하고, {@link ResizableSlotScheme#decide}가 시키는 대로 2배·절반을 하면서 규칙을 검사한다.
 *
 * <p>검사: 슬롯 수는 항상 2의 거듭제곱이고 경계는 균등 분할, 모든 연산 뒤 노드 간 슬롯 수 차이 ≤ 1, 2배·절반 뒤 어느 해시의 주인도 안 바뀜,
 * 추가 이동은 모두 새 노드로, 제거 이동은 모두 떠나는 노드에서, 절반 이동 뒤 짝은 모두 같은 주인, 노드당 슬롯 수는 resize 뒤 [min, max] 안,
 * id는 겹치지 않고 nextTabletId보다 작다.
 */
class ResizableSlotRandomOpsTest {

    @Test
    void randomMembershipWithAutoResizeKeepsInvariants() {
        for (int t : new int[] {2, 8, 64}) {
            for (long seed = 1; seed <= 3; seed++) {
                run(new ResizableSlotScheme(t), new Random(seed), 80);
            }
        }
    }

    private static void run(ResizableSlotScheme scheme, Random rnd, int steps) {
        List<NodeId> members = new ArrayList<>(List.of(new NodeId(1)));
        int nextId = 2;
        RoutingTable t = scheme.initial(members);
        check(scheme, t, members);
        int doubles = 0;
        int halves = 0;
        boolean growing = true;

        for (int step = 0; step < steps; step++) {
            // 20대까지 늘렸다가 1대까지 줄이기를 반복하되 순서는 무작위로 흔든다. 늘 때 2배, 줄 때 절반이 나온다
            if (members.size() >= 20) {
                growing = false;
            } else if (members.size() <= 1) {
                growing = true;
            }
            boolean add = members.size() == 1 || (growing ? rnd.nextInt(5) != 0 : rnd.nextInt(5) == 0);
            if (add) {
                NodeId node = new NodeId(nextId++);
                Plan plan = scheme.onNodeAdded(t, node);
                if (plan.prepared() != t) {
                    doubles++;
                    assertSameOwners(t, plan.prepared(), rnd);
                    assertTrue(plan.prepared().size() > t.size());
                }
                for (Change c : plan.moves()) {
                    assertEquals(node, c.to());
                    assertTrue(members.contains(c.from()));
                }
                members.add(node);
                t = plan.applyAll();
            } else {
                NodeId node = members.remove(rnd.nextInt(members.size()));
                Plan plan = scheme.onNodeRemoved(t, node);
                assertTrue(plan.prepared() == t, "removal never resizes by itself");
                assertEquals(t.ownedBy(node).size(), plan.moves().size());
                for (Change c : plan.moves()) {
                    assertEquals(node, c.from());
                    assertTrue(members.contains(c.to()));
                }
                t = plan.applyAll();
            }
            check(scheme, t, members);

            Resize r = scheme.decide(t);
            if (r == Resize.HALVE) {
                halves++;
                Plan h = scheme.halving(t);
                assertTrue(h.prepared() == t);
                Set<Long> moved = new HashSet<>();
                for (Change c : h.moves()) {
                    assertTrue(moved.add(c.unit().value()), "a slot is moved at most once in a halving plan");
                    assertTrue(members.contains(c.from()) && members.contains(c.to()));
                }
                RoutingTable colocated = h.applyAll();
                assertEquals(0, ResizableSlotSchemeTest.countMixedPairs(colocated));
                RoutingTable m = scheme.merged(colocated);
                assertSameOwners(colocated, m, rnd);
                assertEquals(t.size() / 2, m.size());
                t = m;
                check(scheme, t, members);
            }
            assertTrue(r != Resize.DOUBLE, "doubling is handled inside onNodeAdded");
            int perNode = t.size() / members.size();
            assertTrue(perNode >= scheme.minPerNode() || scheme.decide(t) == Resize.NONE, "per node " + perNode);
            assertTrue(perNode <= scheme.maxPerNode() || t.size() / 2 < members.size(), "per node " + perNode);
        }
        assertTrue(doubles > 0, "the walk should have doubled at least once (T=" + scheme.slotsPerNode() + ")");
        assertTrue(halves > 0, "the walk should have halved at least once (T=" + scheme.slotsPerNode() + ")");
    }

    private static void check(ResizableSlotScheme scheme, RoutingTable t, List<NodeId> members) {
        assertEquals(1, Integer.bitCount(t.size()), "slot count " + t.size());
        assertEquals(TabletRange.uniform(t.size()), t.tablets().stream().map(Tablet::range).toList());
        assertEquals(new HashSet<>(members), t.nodes());
        Set<Long> ids = new HashSet<>();
        for (Tablet tablet : t.tablets()) {
            assertTrue(ids.add(tablet.id().value()));
            assertTrue(tablet.id().value() < t.nextTabletId());
        }
        assertTrue(ResizableSlotSchemeTest.imbalance(t, members) <= 1,
                "imbalance " + ResizableSlotSchemeTest.imbalance(t, members) + " with " + members.size() + " nodes, S=" + t.size());
    }

    private static void assertSameOwners(RoutingTable before, RoutingTable after, Random rnd) {
        for (int i = 0; i < 2_000; i++) {
            long h = rnd.nextLong();
            assertEquals(before.lookup(h).owner(), after.lookup(h).owner());
        }
        assertEquals(before.lookup(0).owner(), after.lookup(0).owner());
        assertEquals(before.lookup(-1L).owner(), after.lookup(-1L).owner());
    }
}
