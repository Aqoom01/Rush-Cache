package io.openkache.core.tablet.range;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.DistributionSchemeContract;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HashRangeSchemeTest extends DistributionSchemeContract {

    private static final int T = 8;

    @Override
    protected DistributionScheme scheme() {
        return new HashRangeScheme(T);
    }

    @Test
    void initialHasTabletsPerNodeInContiguousBlocks() {
        RoutingTable t = scheme().initial(nodes(3));

        assertEquals(3 * T, t.size());
        assertEquals(3 * T, t.nextTabletId());
        for (int i = 0; i < 3 * T; i++) {
            Tablet tablet = t.tablets().get(i);
            assertEquals(i, tablet.id().value());
            assertEquals(new NodeId(i / T + 1), tablet.owner(), "tablet " + i);
        }
        assertEquals(TabletRange.uniform(3 * T), t.tablets().stream().map(Tablet::range).toList());
    }

    @Test
    void matchesFixedSlotWhenNoTabletWasSplit() {
        // 길이가 거의 같으면(균등 분할의 마지막 구간만 나머지만큼 김) 길이 기준 균형 = 개수 기준 균형.
        // 같은 단위 수의 fixed-slot과 표가 같고, 노드 증감 후 노드별 개수도 같아야 한다. donor 순서는 마지막 구간 때문에 다를 수 있다
        FixedSlotScheme fixed = new FixedSlotScheme(3 * T);
        RoutingTable a = scheme().initial(nodes(3));
        RoutingTable b = fixed.initial(nodes(3));
        assertEquals(b.tablets(), a.tablets());

        Plan addA = scheme().onNodeAdded(a, new NodeId(4));
        Plan addB = fixed.onNodeAdded(b, new NodeId(4));
        assertEquals(addB.moves().size(), addA.moves().size());
        assertCountsEqual(addB.applyAll(), addA.applyAll(), nodes(4));

        Plan remA = scheme().onNodeRemoved(a, new NodeId(2));
        Plan remB = fixed.onNodeRemoved(b, new NodeId(2));
        assertEquals(remB.moves().size(), remA.moves().size());
        assertCountsEqual(remB.applyAll(), remA.applyAll(), nodes(3));
    }

    private static void assertCountsEqual(RoutingTable expected, RoutingTable actual, List<NodeId> nodes) {
        for (NodeId n : nodes) {
            assertEquals(expected.ownedBy(n).size(), actual.ownedBy(n).size(), "count of " + n);
        }
    }

    @Test
    void supportsSplitAndNeverChangesBoundariesItself() {
        assertTrue(scheme().supportsSplit());
        assertTrue(scheme().supportsMove());
        RoutingTable t = scheme().initial(nodes(2));
        assertTrue(scheme().onNodeAdded(t, new NodeId(3)).prepared() == t);
        assertTrue(scheme().onNodeRemoved(t, new NodeId(1)).prepared() == t);
    }

    @Test
    void addedNodeIsBalancedByLengthNotByCountAfterSplits() {
        RoutingTable t = scheme().initial(nodes(2)); // 노드1: id 0~7, 노드2: id 8~15
        // 노드1의 Tablet 0을 세 번 쪼개 조각 4개로 만든다. 노드1은 개수 11, 길이는 여전히 절반
        t = split(t, 0);
        t = split(t, 0);
        t = split(t, (int) t.nextTabletId() - 1);
        assertEquals(11, t.ownedBy(new NodeId(1)).size());
        assertEquals(8, t.ownedBy(new NodeId(2)).size());

        Plan plan = scheme().onNodeAdded(t, new NodeId(3));
        RoutingTable after = plan.applyAll();

        long w1 = weightOf(after, new NodeId(1));
        long w2 = weightOf(after, new NodeId(2));
        long w3 = weightOf(after, new NodeId(3));
        long unit = HashRangeScheme.weight(t.tablets().get(t.size() - 1).range()); // 쪼개지 않은 Tablet 하나의 길이
        long mean = (w1 + w2 + w3) / 3;
        // 새 노드는 평균 − 큰 Tablet 하나 이상, 추가 전 최대(= 전체의 절반) 이하
        assertTrue(w3 >= mean - unit, "w3=" + w3 + " mean=" + mean);
        assertTrue(w3 <= Math.max(weightOf(t, new NodeId(1)), weightOf(t, new NodeId(2))));
        // 개수로 셌다면 노드1(11개)에서만 가져갔을 것. 길이로 세므로 둘 다 donor가 된다
        assertTrue(plan.moves().stream().anyMatch(c -> c.from().equals(new NodeId(1))));
        assertTrue(plan.moves().stream().anyMatch(c -> c.from().equals(new NodeId(2))));
        // 기존 노드끼리는 안 움직인다
        for (Change c : plan.moves()) {
            assertEquals(new NodeId(3), c.to());
        }
    }

    @Test
    void exactTieDoesNotMoveATabletThatOnlySwapsPlaces() {
        // 길이가 정확히 2^60인 Tablet 16개(uniform은 마지막 구간이 1 길어서 정확한 동률이 안 나온다). 노드1: 0~7, 노드2: 8~15
        List<Tablet> tablets = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            long start = (long) i << 60;
            long end = i == 15 ? 0 : (long) (i + 1) << 60;
            tablets.add(new Tablet(new TabletId(i), new TabletRange(start, end), new NodeId(i / 8 + 1), OwnershipEpoch.ZERO.next()));
        }
        RoutingTable t = RoutingTable.of(RoutingVersion.ZERO.next(), tablets, 16);

        Plan plan = new HashRangeScheme(8).onNodeAdded(t, new NodeId(3));

        // 5개 받으면 (6, 5, 5). 6번째를 받으면 (5, 5, 6)으로 자리만 바뀌므로 옮기지 않는다
        assertEquals(5, plan.moves().size());
        RoutingTable after = plan.applyAll();
        long w3 = weightOf(after, new NodeId(3));
        assertTrue(w3 <= Math.max(weightOf(after, new NodeId(1)), weightOf(after, new NodeId(2))));
    }

    @Test
    void donorGivesItsLastTabletFirst() {
        RoutingTable t = scheme().initial(nodes(2));

        Plan plan = scheme().onNodeAdded(t, new NodeId(3));

        // 균등 분할의 마지막 구간이 나머지만큼 길어서 노드2가 아주 조금 더 길다 → 노드2의 마지막(id 15)부터, 그다음 노드1의 마지막(id 7)
        assertEquals(new TabletId(15), plan.moves().get(0).unit());
        assertEquals(new NodeId(2), plan.moves().get(0).from());
        assertEquals(new TabletId(7), plan.moves().get(1).unit());
        assertEquals(new NodeId(1), plan.moves().get(1).from());
    }

    @Test
    void tiesGoToSmallerNodeId() {
        // 노드 3대의 길이를 정확히 같게 만든다: 마지막 구간(나머지만큼 긴 것)을 노드3에서 떼어 노드4를 만든 뒤 노드4를 뺀다
        RoutingTable t = scheme().initial(nodes(3));
        TabletId last = t.tablets().get(t.size() - 1).id();
        t = t.withOwner(last, new NodeId(4));
        long w1 = weightOf(t, new NodeId(1));
        long w2 = weightOf(t, new NodeId(2));
        assertEquals(w1, w2);
        // 노드1·2가 동률(노드3은 하나 적음) → 추가 시 donor는 노드1, 그다음 노드2
        Plan add = scheme().onNodeAdded(t, new NodeId(5));
        assertEquals(new NodeId(1), add.moves().get(0).from());
        assertEquals(new NodeId(2), add.moves().get(1).from());
        // 제거 시 receiver는 가장 짧은 노드3, 그다음 동률인 노드1·2 중 노드1
        Plan remove = scheme().onNodeRemoved(t, new NodeId(4));
        assertEquals(1, remove.moves().size());
        assertEquals(new NodeId(3), remove.moves().get(0).to());
        RoutingTable even = remove.applyAll(); // 이제 노드1·2·3 중 노드3이 가장 길다(나머지만큼)
        Plan add2 = scheme().onNodeAdded(even, new NodeId(5));
        assertEquals(new NodeId(3), add2.moves().get(0).from());
        assertEquals(new NodeId(1), add2.moves().get(1).from());
        assertEquals(new NodeId(2), add2.moves().get(2).from());
    }

    @Test
    void removedNodeGivesLongestTabletsFirstToShortestNode() {
        RoutingTable t = scheme().initial(nodes(3));
        // 노드2의 마지막 Tablet(id 15)을 쪼갠다 → 노드2는 긴 것 7개 + 반쪽 2개
        t = split(t, 15);
        NodeId leaving = new NodeId(2);

        Plan plan = scheme().onNodeRemoved(t, leaving);

        assertEquals(9, plan.moves().size());
        // 긴 것 7개가 먼저, 반쪽 둘은 마지막(반쪽끼리 순서는 길이 1 차이로 바뀔 수 있다)
        List<Change> moves = plan.moves();
        assertEquals(Set.of(new TabletId(15), new TabletId(24)), Set.of(moves.get(7).unit(), moves.get(8).unit()));
        for (int i = 0; i < 7; i++) {
            assertEquals(new TabletId(8 + i), moves.get(i).unit());
        }
        RoutingTable after = plan.applyAll();
        assertFalse(after.nodes().contains(leaving));
        long w1 = weightOf(after, new NodeId(1));
        long w3 = weightOf(after, new NodeId(3));
        long half = HashRangeScheme.weight(t.get(new TabletId(24)).range());
        assertTrue(Math.abs(w1 - w3) <= half + 1, "w1=" + w1 + " w3=" + w3);
    }

    @Test
    void mergedTabletsStillBalanceByLength() {
        RoutingTable t = scheme().initial(nodes(2));
        // 노드2의 id 8~15를 전부 하나로 합친다 → 노드2는 Tablet 1개(길이 절반)
        for (int i = 9; i < 2 * T; i++) {
            t = t.withMerge(new TabletId(8), new TabletId(i));
        }
        assertEquals(1, t.ownedBy(new NodeId(2)).size());

        Plan plan = scheme().onNodeAdded(t, new NodeId(3));

        // 노드2는 Tablet이 하나라 donor가 못 된다(주면 표에서 사라짐). 노드1에서만 가져간다
        for (Change c : plan.moves()) {
            assertEquals(new NodeId(1), c.from());
        }
        RoutingTable after = plan.applyAll();
        assertEquals(3, after.nodes().size());
        // 노드1 절반 중 새 노드가 넘지 않는 만큼: 8개 중 4개
        assertEquals(4, after.ownedBy(new NodeId(3)).size());
    }

    @Test
    void rejectsAddingNodeWhenEveryNodeHasOneTablet() {
        HashRangeScheme one = new HashRangeScheme(1);
        RoutingTable t = one.initial(nodes(2));
        // 노드2의 Tablet을 노드1로 옮기고 합쳐 Tablet 하나짜리 표를 만든다
        RoutingTable single = t.withOwner(new TabletId(1), new NodeId(1)).withMerge(new TabletId(0), new TabletId(1));
        assertEquals(1, single.size());

        assertThrows(IllegalArgumentException.class, () -> one.onNodeAdded(single, new NodeId(2)));
        // Tablet 2개, 노드 2개 → 세 번째 노드는 못 들어온다
        assertThrows(IllegalArgumentException.class, () -> one.onNodeAdded(t, new NodeId(3)));
    }

    @Test
    void metadataBytesGrowsPerTablet() {
        HashRangeScheme s = new HashRangeScheme(T);
        RoutingTable t = s.initial(nodes(2));

        assertEquals(16 + 2 * T * 32, s.metadataBytes(t));
        assertEquals(16 + (2 * T + 1) * 32, s.metadataBytes(split(t, 0)));
    }

    @Test
    void weightHandlesFullSpaceAndWrapAround() {
        assertEquals(1L << 62, HashRangeScheme.weight(TabletRange.FULL));
        // [2^63, 2^64) 길이 2^63 → 2^61
        assertEquals(1L << 61, HashRangeScheme.weight(new TabletRange(Long.MIN_VALUE, 0)));
        // [2^64 − 8, 2^64) 길이 8 → 2
        assertEquals(2, HashRangeScheme.weight(new TabletRange(-8L, 0)));
        // 균등 분할은 2^64 − 1 을 나누므로 마지막 구간이 나머지만큼 길다: 반반이면 2^63 − 1 과 2^63 + 1
        List<TabletRange> halves = TabletRange.uniform(2);
        assertEquals(HashRangeScheme.weight(halves.get(0)) + 1, HashRangeScheme.weight(halves.get(1)));
    }

    @Test
    void rejectsBadArguments() {
        assertThrows(IllegalArgumentException.class, () -> new HashRangeScheme(0));
        assertThrows(IllegalArgumentException.class, () -> scheme().initial(List.of(new NodeId(1), new NodeId(1))));
        assertEquals(64, new HashRangeScheme().tabletsPerNode());
        assertEquals("hash-range-tablet", new HashRangeScheme().name());
    }

    /** Tablet {@code id}를 한가운데서 쪼갠다. */
    private static RoutingTable split(RoutingTable t, int id) {
        TabletRange r = t.get(new TabletId(id)).range();
        long mid = r.start() + ((r.end() - r.start()) >>> 1);
        return t.withSplit(new TabletId(id), mid);
    }

    private static long weightOf(RoutingTable t, NodeId node) {
        long w = 0;
        for (Tablet tablet : t.ownedBy(node)) {
            w += HashRangeScheme.weight(tablet.range());
        }
        return w;
    }
}
