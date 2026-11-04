package io.openkache.core.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.Bytes;
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
        assertEquals(plan.moves(), scheme().onNodeRemoved(t0, leaving).moves(), "plan must be deterministic");
    }

    /**
     * 떠났던 NodeId가 다시 들어오면 몫을 돌려받아야 한다. Tablet 수만이 아니라 해시 공간 길이로도 검사한다. 링처럼 경계가 남는
     * variant가 "이미 있는 경계"를 한 칸 밀어 폭 1짜리 Tablet만 주는 퇴화를 막기 위해서다.
     */
    @Test
    void nodeThatLeftCanRejoinAndGetsARealShareBack() {
        RoutingTable t0 = scheme().initial(nodes(4));
        NodeId n3 = new NodeId(3);
        RoutingTable t1 = scheme().onNodeRemoved(t0, n3).applyAll();

        Plan plan = scheme().onNodeAdded(t1, n3);

        assertOwnersUnchanged(t1, plan.prepared());
        for (Change c : plan.moves()) {
            assertEquals(n3, c.to());
        }
        RoutingTable t2 = plan.applyAll();
        assertBalanced(t2, nodes(4));
        assertTrue(hashShare(t2, n3) > 0.01, "rejoined node owns a negligible part of the hash space: " + hashShare(t2, n3));
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

    @Test
    void keyHashIsDeterministicAndIndependentOfTheTable() {
        Bytes key = Bytes.utf8("{user1}:profile");
        long h = scheme().keyHash(key);

        assertEquals(h, scheme().keyHash(key));
        assertEquals(h, scheme().keyHash(Bytes.copyOf(key.toByteArray())));
        RoutingTable t = scheme().initial(nodes(3));
        assertEquals(t.lookup(h), t.lookup(scheme().keyHash(key)));
        // 해시 기반 variant는 tag 없는 key에 key 전체의 KeyHash를 써야 한다(비교 실험 S1~S5가 같은 해시열을 쓴다는 전제)
        if (usesCommonKeyHash()) {
            Bytes plain = Bytes.utf8("user:42");
            assertEquals(KeyHash.of(plain), scheme().keyHash(plain));
        }
    }

    /** tag 없는 key를 공통 {@link KeyHash}로 바꾸는 variant인가. 순서 보존 변환을 쓰는 key-order variant만 {@code false}다. */
    protected boolean usesCommonKeyHash() {
        return true;
    }

    /** {@code prepared}는 경계만 바뀔 수 있고 주인·epoch는 못 바꾼다: 모든 해시 지점의 주인과 epoch가 같아야 한다. */
    private static void assertOwnersUnchanged(RoutingTable before, RoutingTable prepared) {
        for (Tablet t : prepared.tablets()) {
            Tablet was = before.lookup(t.range().start());
            assertEquals(was.owner(), t.owner(), "owner changed in prepared table at " + t);
            assertEquals(was.epoch(), t.epoch(), "epoch changed in prepared table at " + t);
        }
    }

    /** 노드가 맡은 해시 공간의 비율(0~1). 길이를 4로 나눠 더하므로 전체 합이 long에 들어간다. */
    private static double hashShare(RoutingTable t, NodeId node) {
        long quarter = 0;
        for (Tablet tablet : t.ownedBy(node)) {
            quarter += (tablet.range().end() - tablet.range().start()) >>> 2; // end 0(2^64)은 -start로 wrap
        }
        return quarter / Math.pow(2, 62);
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
