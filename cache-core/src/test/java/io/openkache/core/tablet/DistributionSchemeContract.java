package io.openkache.core.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 모든 {@link DistributionScheme} 구현이 지켜야 하는 계약. variant 테스트가 이 클래스를 상속해 {@link #scheme()}만 구현한다.
 *
 * <p>design.md §5의 불변식 가운데 variant에 걸리는 것(5, 6)과 인터페이스 Javadoc의 계약을 검사한다. 표 자체의 불변식(1~4, 7)은
 * {@link RoutingTableTest}가 검사한다.
 */
public abstract class DistributionSchemeContract {

    protected abstract DistributionScheme scheme();

    /** 이 variant에서 균형으로 보는 노드 간 Tablet 수 차이의 상한. 고정·가변 구간 variant는 1이다. */
    protected int allowedImbalance() {
        return 1;
    }

    protected static List<NodeId> nodes(int n) {
        List<NodeId> l = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            l.add(new NodeId(i));
        }
        return l;
    }

    @Test
    void initialHasVersionOneAndEpochOneAndCoversAllNodes() {
        RoutingTable t = scheme().initial(nodes(3));

        assertEquals(1, t.version().value());
        assertEquals(3, t.nodes().size());
        for (Tablet tablet : t.tablets()) {
            assertEquals(1, tablet.epoch().value());
        }
        assertBalanced(t, nodes(3));
    }

    @Test
    void initialRejectsNoNodes() {
        assertThrows(IllegalArgumentException.class, () -> scheme().initial(List.of()));
    }

    @Test
    void initialIsDeterministic() {
        assertEquals(scheme().initial(nodes(4)).tablets(), scheme().initial(nodes(4)).tablets());
    }

    @Test
    void addedNodeReceivesOnlyMovesToItselfAndTableBecomesBalanced() {
        RoutingTable t0 = scheme().initial(nodes(3));
        NodeId added = new NodeId(4);

        Plan plan = scheme().onNodeAdded(t0, added);

        assertEquals(1, t0.version().value(), "plan must not change the input table");
        assertOwnersUnchanged(t0, plan.prepared());
        for (Change c : plan.moves()) {
            assertEquals(added, c.to());
            assertEquals(plan.prepared().get(c.unit()).owner(), c.from());
        }
        RoutingTable t1 = plan.applyAll();
        assertEquals(4, t1.nodes().size());
        assertBalanced(t1, nodes(4));
        assertEquals(plan.moves(), scheme().onNodeAdded(t0, added).moves(), "plan must be deterministic");
    }

    @Test
    void addingExistingNodeIsRejected() {
        RoutingTable t = scheme().initial(nodes(3));

        assertThrows(IllegalArgumentException.class, () -> scheme().onNodeAdded(t, new NodeId(2)));
    }

    @Test
    void removedNodeGivesAwayAllItsTabletsAndTableStaysBalanced() {
        List<NodeId> four = nodes(4);
        RoutingTable t0 = scheme().initial(four);
        NodeId leaving = new NodeId(2);

        Plan plan = scheme().onNodeRemoved(t0, leaving);

        assertOwnersUnchanged(t0, plan.prepared());
        for (Change c : plan.moves()) {
            assertEquals(leaving, c.from());
        }
        RoutingTable t1 = plan.applyAll();
        assertEquals(0, t1.ownedBy(leaving).size());
        List<NodeId> remaining = new ArrayList<>(four);
        remaining.remove(leaving);
        assertBalanced(t1, remaining);
    }

    @Test
    void removingUnknownOrLastNodeIsRejected() {
        RoutingTable t = scheme().initial(nodes(1));

        assertThrows(IllegalArgumentException.class, () -> scheme().onNodeRemoved(t, new NodeId(9)));
        assertThrows(IllegalStateException.class, () -> scheme().onNodeRemoved(t, new NodeId(1)));
    }

    @Test
    void addThenRemoveReturnsToBalancedState() {
        RoutingTable t0 = scheme().initial(nodes(5));
        NodeId n6 = new NodeId(6);

        RoutingTable t1 = scheme().onNodeAdded(t0, n6).applyAll();
        RoutingTable t2 = scheme().onNodeRemoved(t1, n6).applyAll();

        assertEquals(0, t2.ownedBy(n6).size());
        assertBalanced(t2, nodes(5));
    }

    @Test
    void metadataBytesIsPositive() {
        assertTrue(scheme().metadataBytes(scheme().initial(nodes(2))) > 0);
    }

    /** {@code prepared}는 경계만 바뀔 수 있고 주인은 못 바꾼다: 모든 해시 지점의 주인이 같아야 한다. */
    private static void assertOwnersUnchanged(RoutingTable before, RoutingTable prepared) {
        for (Tablet t : prepared.tablets()) {
            assertEquals(before.lookup(t.range().start()).owner(), t.owner(), "owner changed in prepared table at " + t);
        }
    }

    private void assertBalanced(RoutingTable t, List<NodeId> nodes) {
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (NodeId n : nodes) {
            int count = t.ownedBy(n).size();
            min = Math.min(min, count);
            max = Math.max(max, count);
        }
        assertTrue(max - min <= allowedImbalance(), "imbalance " + (max - min) + " exceeds " + allowedImbalance());
    }
}
