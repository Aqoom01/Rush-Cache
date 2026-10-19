package io.openkache.core.tablet.slot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.DistributionSchemeContract;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class FixedSlotSchemeTest extends DistributionSchemeContract {

    private static final int SLOTS = 64;

    @Override
    protected DistributionScheme scheme() {
        return new FixedSlotScheme(SLOTS);
    }

    @Test
    void initialAssignsSlotsRoundRobin() {
        RoutingTable t = scheme().initial(nodes(3));

        assertEquals(SLOTS, t.size());
        assertEquals(SLOTS, t.nextTabletId());
        for (int i = 0; i < SLOTS; i++) {
            Tablet tablet = t.tablets().get(i);
            assertEquals(i, tablet.id().value());
            assertEquals(new NodeId(1 + i % 3), tablet.owner());
        }
        assertEquals(22, t.ownedBy(new NodeId(1)).size());
        assertEquals(21, t.ownedBy(new NodeId(2)).size());
        assertEquals(21, t.ownedBy(new NodeId(3)).size());
    }

    @Test
    void addedNodeTakesExactlySlotsOverNodeCountAndNothingElseMoves() {
        RoutingTable t0 = scheme().initial(nodes(3));
        NodeId n4 = new NodeId(4);

        Plan plan = scheme().onNodeAdded(t0, n4);

        assertEquals(SLOTS / 4, plan.moves().size());
        assertTrue(plan.prepared() == t0, "fixed-slot never changes boundaries");
        RoutingTable t1 = plan.applyAll();
        assertEquals(SLOTS / 4, t1.ownedBy(n4).size());
        // 이동하지 않은 슬롯은 주인과 epoch가 그대로
        List<Long> moved = new ArrayList<>();
        for (Change c : plan.moves()) {
            moved.add(c.unit().value());
        }
        for (int i = 0; i < SLOTS; i++) {
            if (!moved.contains((long) i)) {
                assertEquals(t0.tablets().get(i), t1.tablets().get(i));
            }
        }
    }

    @Test
    void addingManyNodesKeepsImbalanceAtMostOne() {
        RoutingTable t = scheme().initial(nodes(1));
        for (int n = 2; n <= 10; n++) {
            t = scheme().onNodeAdded(t, new NodeId(n)).applyAll();
            int min = Integer.MAX_VALUE;
            int max = 0;
            for (NodeId node : t.nodes()) {
                int c = t.ownedBy(node).size();
                min = Math.min(min, c);
                max = Math.max(max, c);
            }
            assertTrue(max - min <= 1, "n=" + n + " max=" + max + " min=" + min);
            assertEquals(n, t.nodes().size());
        }
    }

    @Test
    void removedNodeGivesSlotsToLeastLoadedNodes() {
        RoutingTable t0 = scheme().initial(nodes(4));
        NodeId leaving = new NodeId(3);

        Plan plan = scheme().onNodeRemoved(t0, leaving);

        assertEquals(t0.ownedBy(leaving).size(), plan.moves().size());
        RoutingTable t1 = plan.applyAll();
        assertFalse(t1.nodes().contains(leaving));
        assertEquals(3, t1.nodes().size());
    }

    @Test
    void metadataShrinksWhenOwnersAreContiguous() {
        FixedSlotScheme s = new FixedSlotScheme(SLOTS);
        RoutingTable roundRobin = s.initial(nodes(2)); // 주인이 번갈아 가므로 구간 64개
        RoutingTable single = s.initial(nodes(1)); // 구간 1개

        assertEquals(16 + 64 * 20, s.metadataBytes(roundRobin));
        assertEquals(16 + 20, s.metadataBytes(single));
    }

    @Test
    void rejectsNonPositiveSlotCount() {
        assertThrows(IllegalArgumentException.class, () -> new FixedSlotScheme(0));
    }

    @Test
    void rejectsDuplicateNodesAndMoreNodesThanSlots() {
        FixedSlotScheme tiny = new FixedSlotScheme(2);

        assertThrows(IllegalArgumentException.class, () -> tiny.initial(List.of(new NodeId(1), new NodeId(1))));
        assertThrows(IllegalArgumentException.class, () -> tiny.initial(nodes(3)));
        RoutingTable full = tiny.initial(nodes(2));
        assertThrows(IllegalArgumentException.class, () -> tiny.onNodeAdded(full, new NodeId(3)));
    }

    @Test
    void plansAreIndependentOfNodeIdOrder() {
        RoutingTable a = scheme().initial(List.of(new NodeId(1), new NodeId(2), new NodeId(3)));
        RoutingTable b = scheme().initial(List.of(new NodeId(3), new NodeId(1), new NodeId(2)));

        // 같은 표(주인 배치만 다름)에 같은 노드를 넣으면 donor 선택 규칙(최다 보유 → 작은 NodeId)이 결정적이어야 한다
        Plan pa = scheme().onNodeAdded(a, new NodeId(4));
        Plan pb = scheme().onNodeAdded(b, new NodeId(4));
        assertEquals(pa.moves().size(), pb.moves().size());
        assertEquals(pa.moves(), scheme().onNodeAdded(a, new NodeId(4)).moves());
        assertEquals(pb.moves(), scheme().onNodeAdded(b, new NodeId(4)).moves());
    }

    @Test
    void defaultSlotCountIs4096() {
        assertEquals(4096, new FixedSlotScheme().slots());
        assertEquals("fixed-slot", new FixedSlotScheme().name());
    }
}
