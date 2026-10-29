package io.openkache.core.tablet.ring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.DistributionSchemeContract;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class RingSchemeTest extends DistributionSchemeContract {

    private static final int V = 8;

    @Override
    protected DistributionScheme scheme() {
        return new RingScheme(V);
    }

    /** 링은 토큰 수만 보장한다. 제거 뒤에는 떠난 노드의 Tablet이 시계 방향 이웃에게 몰릴 수 있어 개수 차이가 V를 넘을 수 있다. */
    @Override
    protected int allowedImbalance() {
        return 2 * V + 2;
    }

    @Test
    void initialHasOneTabletPerTokenPlusSeam() {
        RoutingTable t = scheme().initial(nodes(3));

        assertEquals(3 * V + 1, t.size());
        assertEquals(3 * V + 1, t.nextTabletId());
        List<Tablet> all = t.tablets();
        Tablet first = all.get(0);
        Tablet last = all.get(all.size() - 1);
        assertEquals(0, first.range().start());
        assertEquals(0, last.range().end());
        assertEquals(first.owner(), last.owner(), "seam tablets share the first token's owner");
        for (int i = 0; i < all.size(); i++) {
            assertEquals(i, all.get(i).id().value());
        }
        for (NodeId n : nodes(3)) {
            int expected = n.equals(first.owner()) ? V + 1 : V;
            assertEquals(expected, t.ownedBy(n).size(), "node " + n);
        }
    }

    @Test
    void lookupMatchesClockwiseWalkOverTokens() {
        List<NodeId> members = nodes(5);
        RoutingTable t = scheme().initial(members);
        TreeMap<Long, NodeId> ring = ringOf(members);

        assertRingLookup(ring, t, new Random(7), 20_000);
    }

    @Test
    void addMovesOnlyThePieceBeforeEachNewTokenAndOldNodesKeepTheirCounts() {
        RoutingTable t0 = scheme().initial(nodes(3));
        NodeId added = new NodeId(4);

        Plan plan = scheme().onNodeAdded(t0, added);
        RoutingTable t1 = plan.applyAll();

        assertEquals(t0.size() + V, plan.prepared().size(), "one split per new token");
        assertEquals(t0.version().value() + V, plan.prepared().version().value());
        int lost = 0;
        for (NodeId n : nodes(3)) {
            int diff = t0.ownedBy(n).size() - t1.ownedBy(n).size();
            assertTrue(diff == 0 || diff == 1, "old node " + n + " lost " + diff);
            lost += diff;
        }
        assertTrue(lost <= 1, "only the seam tablet may leave an old node (when the new node gets the smallest token)");
        int mine = t1.ownedBy(added).size();
        assertTrue(mine >= V && mine <= 2 * V + 1, "new node owns between V and 2V+1 tablets: " + mine);
        assertRingLookup(ringOf(nodes(4)), t1, new Random(8), 20_000);
    }

    @Test
    void removeHandsEachTabletToTheClockwiseSuccessor() {
        List<NodeId> four = nodes(4);
        RoutingTable t0 = scheme().initial(four);
        NodeId leaving = new NodeId(2);

        Plan plan = scheme().onNodeRemoved(t0, leaving);
        RoutingTable t1 = plan.applyAll();

        assertEquals(t0, plan.prepared(), "removal does not change boundaries");
        assertEquals(t0.ownedBy(leaving).size(), plan.moves().size());
        List<Tablet> all = t0.tablets();
        for (Change c : plan.moves()) {
            int i = indexOf(all, c);
            int j = i;
            do {
                j = (j + 1) % all.size();
            } while (all.get(j).owner().equals(leaving));
            assertEquals(all.get(j).owner(), c.to(), "tablet " + c.unit() + " goes to its clockwise successor");
        }
        List<NodeId> remaining = List.of(new NodeId(1), new NodeId(3), new NodeId(4));
        assertRingLookup(ringOf(remaining), t1, new Random(9), 20_000);
    }

    @Test
    void rejoiningNodeReclaimsItsOwnTokensExactly() {
        List<NodeId> four = nodes(4);
        RoutingTable t0 = scheme().initial(four);
        NodeId n3 = new NodeId(3);
        RoutingTable t1 = scheme().onNodeRemoved(t0, n3).applyAll();

        // 같은 NodeId가 다시 들어오면 토큰 V개가 전부 "이미 있는 경계"와 겹친다. 그 경계에서 끝나는 Tablet을 통째로 되찾아야
        // 떠나기 전과 같은 링이 된다(폭 1짜리 Tablet만 받는 퇴화 금지)
        Plan plan = scheme().onNodeAdded(t1, n3);
        RoutingTable t2 = plan.applyAll();

        assertEquals(t1.size(), plan.prepared().size(), "no boundary is added: the tokens already exist");
        // 토큰마다 Tablet 하나. 되찾은 토큰이 링에서 가장 작으면 이음새 Tablet도 따라온다(= 떠나기 전 보유 수)
        assertEquals(t0.ownedBy(n3).size(), plan.moves().size(), "one tablet per reclaimed token (+ seam if smallest)");
        for (Tablet t : t0.tablets()) {
            assertEquals(t.owner(), t2.lookup(t.range().start()).owner(), "ownership returns to the pre-removal ring at " + t);
        }
        assertRingLookup(ringOf(four), t2, new Random(11), 20_000);
    }

    @Test
    void compactMergesBoundariesLeftByARemovedNode() {
        RoutingTable t0 = scheme().initial(nodes(4));
        RoutingTable t1 = scheme().onNodeRemoved(t0, new NodeId(2)).applyAll();
        assertEquals(4 * V + 1, t1.size(), "boundaries stay until compacted");

        RoutingTable c = RingScheme.compact(t1);

        // 떠난 토큰 자리뿐 아니라 같은 노드의 이웃한 토큰도 합쳐지므로 남는 Tablet은 "남은 토큰 + 1" 이하다
        assertTrue(c.size() <= 3 * V + 1, "size " + c.size());
        for (int i = 0; i + 1 < c.size(); i++) {
            assertNotEquals(c.tablets().get(i).owner(), c.tablets().get(i + 1).owner(), "no adjacent tablets share an owner");
        }
        assertEquals(c.tablets().get(0).owner(), c.tablets().get(c.size() - 1).owner(), "seam still shares one owner");
        Random rnd = new Random(10);
        for (int k = 0; k < 20_000; k++) {
            long h = rnd.nextLong();
            assertEquals(t1.lookup(h).owner(), c.lookup(h).owner());
        }
        assertTrue(scheme().metadataBytes(c) < scheme().metadataBytes(t1), "compacted table reports fewer tokens");
    }

    @Test
    void newSmallestTokenTakesTheSeamTablet() {
        // 토큰이 하나뿐인 노드 1 (토큰 t1). 새 노드의 토큰 t2 < t1 이면 [0, t2)와 [t1, 2^64) 둘 다 새 노드 것이다 (링에서 t1 다음은 t2)
        RingScheme one = new RingScheme(1);
        RoutingTable t0 = one.initial(List.of(new NodeId(1)));
        NodeId added = findNodeWithTokenBelow(RingScheme.token(new NodeId(1), 0));

        RoutingTable t1 = one.onNodeAdded(t0, added).applyAll();

        long t2 = RingScheme.token(added, 0);
        assertEquals(added, t1.lookup(0).owner());
        assertEquals(added, t1.lookup(t2 - 1).owner());
        assertEquals(new NodeId(1), t1.lookup(t2).owner());
        assertEquals(added, t1.lookup(-1L).owner(), "wrap arc belongs to the new smallest token");
        assertEquals(t1.tablets().get(0).owner(), t1.tablets().get(t1.size() - 1).owner());
    }

    private static NodeId findNodeWithTokenBelow(long limit) {
        for (long n = 2; n < 10_000; n++) {
            if (Long.compareUnsigned(RingScheme.token(new NodeId(n), 0), limit) < 0) {
                return new NodeId(n);
            }
        }
        throw new AssertionError("no node id with a token below " + Long.toUnsignedString(limit));
    }

    @Test
    void metadataCountsTokensNotTablets() {
        RoutingTable t = scheme().initial(nodes(3));
        assertEquals(16 + 3 * V * RingScheme.BYTES_PER_TOKEN, scheme().metadataBytes(t));
    }

    @Test
    void balancedPlacementGivesTheNewNodeItsFairShareTakenFromTheRichestNodes() {
        RingScheme alloc = new RingScheme(16, RingScheme.Placement.BALANCED);
        List<NodeId> eight = nodes(8);
        RoutingTable t0 = alloc.initial(eight);
        NodeId added = new NodeId(9);

        Plan plan = alloc.onNodeAdded(t0, added);
        RoutingTable t1 = plan.applyAll();

        assertEquals(plan.moves(), alloc.onNodeAdded(t0, added).moves(), "deterministic");
        Map<NodeId, Double> before = lengths(t0);
        Map<NodeId, Double> after = lengths(t1);
        double fair = Math.pow(2, 64) / 9;
        assertTrue(Math.abs(after.get(added) / fair - 1) < 0.02, "new node share / fair = " + after.get(added) / fair);
        for (NodeId n : eight) {
            assertTrue(after.get(n) <= before.get(n), "old node " + n + " only gives");
        }
        assertEquals(16, plan.prepared().size() - t0.size(), "one split per token");
        for (Change c : plan.moves()) {
            assertEquals(added, c.to());
        }
    }

    @Test
    void balancedPlacementBalancesLengthWhereRandomTokensDoNot() {
        List<NodeId> eight = nodes(8);
        double random = lengthMaxMean(new RingScheme(16, RingScheme.Placement.RANDOM).initial(eight), eight);
        double alloc = lengthMaxMean(new RingScheme(16, RingScheme.Placement.BALANCED).initial(eight), eight);

        assertTrue(alloc < random, "alloc " + alloc + " should beat random " + random);
        assertTrue(alloc < 1.05, "alloc max/mean " + alloc);
    }

    @Test
    void namesAndCapabilities() {
        assertEquals("ring-vnode", new RingScheme(4).name());
        assertEquals("ring-vnode-alloc", new RingScheme(4, RingScheme.Placement.BALANCED).name());
        assertFalse(scheme().supportsSplit());
        assertFalse(scheme().supportsMove());
    }

    @Test
    void tokenIsNeverZero() {
        for (long n = 1; n <= 50; n++) {
            for (int i = 0; i < 64; i++) {
                assertNotEquals(0L, RingScheme.token(new NodeId(n), i));
            }
        }
    }

    // ---- helpers ----

    /** 노드들의 무작위 토큰으로 만든 "진짜" 링. 키는 부호 비트를 뒤집은 토큰(unsigned 순서). */
    static TreeMap<Long, NodeId> ringOf(List<NodeId> members) {
        TreeMap<Long, NodeId> ring = new TreeMap<>();
        for (NodeId n : members) {
            for (int i = 0; i < V; i++) {
                long t = RingScheme.token(n, i);
                while (ring.containsKey(t ^ Long.MIN_VALUE)) {
                    t += 1;
                    if (t == 0) {
                        t = 1;
                    }
                }
                ring.put(t ^ Long.MIN_VALUE, n);
            }
        }
        return ring;
    }

    /** 시계 방향으로 처음 만나는 토큰의 노드. 토큰 t는 (이전 토큰, t]를 맡지만 우리 표는 [이전, t)라 경계 자체는 다음 토큰 것이다. */
    static NodeId clockwise(TreeMap<Long, NodeId> ring, long hash) {
        Map.Entry<Long, NodeId> e = ring.higherEntry(hash ^ Long.MIN_VALUE);
        return e != null ? e.getValue() : ring.firstEntry().getValue();
    }

    static void assertRingLookup(TreeMap<Long, NodeId> ring, RoutingTable t, Random rnd, int samples) {
        for (long h : new long[] {0, 1, Long.MIN_VALUE, -1L}) {
            assertEquals(clockwise(ring, h), t.lookup(h).owner(), "hash " + Long.toUnsignedString(h));
        }
        for (long k : ring.keySet()) {
            long token = k ^ Long.MIN_VALUE;
            assertEquals(clockwise(ring, token), t.lookup(token).owner(), "at token " + Long.toUnsignedString(token));
            assertEquals(clockwise(ring, token - 1), t.lookup(token - 1).owner(), "before token");
        }
        for (int i = 0; i < samples; i++) {
            long h = rnd.nextLong();
            assertEquals(clockwise(ring, h), t.lookup(h).owner(), "hash " + Long.toUnsignedString(h));
        }
    }

    private static int indexOf(List<Tablet> all, Change c) {
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i).id().equals(c.unit())) {
                return i;
            }
        }
        throw new AssertionError("unknown tablet " + c.unit());
    }

    private static Map<NodeId, Double> lengths(RoutingTable t) {
        Map<NodeId, Double> len = new HashMap<>();
        for (Tablet tablet : t.tablets()) {
            len.merge(tablet.owner(), unsignedToDouble(tablet.range().end() - tablet.range().start()), Double::sum);
        }
        return len;
    }

    private static double lengthMaxMean(RoutingTable t, List<NodeId> members) {
        Map<NodeId, Double> len = lengths(t);
        double max = 0;
        double sum = 0;
        for (NodeId n : members) {
            double l = len.getOrDefault(n, 0.0);
            max = Math.max(max, l);
            sum += l;
        }
        return max / (sum / members.size());
    }

    private static double unsignedToDouble(long v) {
        double d = (double) (v >>> 1) * 2.0;
        return d + (v & 1);
    }
}
