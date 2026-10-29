package io.openkache.core.tablet.resize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import io.openkache.core.tablet.resize.ResizableSlotScheme.Resize;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ResizableSlotSchemeTest extends DistributionSchemeContract {

    private static final int T = 4;

    @Override
    protected DistributionScheme scheme() {
        return new ResizableSlotScheme(T);
    }

    private static ResizableSlotScheme resizable() {
        return new ResizableSlotScheme(T);
    }

    @Test
    void initialIsTheSmallestPowerOfTwoAboveNodesTimesTargetAndMatchesFixedSlot() {
        ResizableSlotScheme s = resizable();
        assertEquals(4, s.initialSlots(1));
        assertEquals(8, s.initialSlots(2));
        assertEquals(16, s.initialSlots(3));
        assertEquals(16, s.initialSlots(4));
        assertEquals(32, s.initialSlots(5));

        RoutingTable a = s.initial(nodes(3));
        RoutingTable b = new FixedSlotScheme(16).initial(nodes(3));
        assertEquals(b.tablets(), a.tablets());
        assertEquals(16, a.nextTabletId());
    }

    @Test
    void decideFollowsHysteresisBounds() {
        ResizableSlotScheme s = new ResizableSlotScheme(8, 4, 16);
        assertEquals(Resize.NONE, s.decide(64, 8)); // 8 per node
        assertEquals(Resize.NONE, s.decide(64, 16)); // 4 = min
        assertEquals(Resize.DOUBLE, s.decide(64, 17)); // 3 < min
        assertEquals(Resize.NONE, s.decide(64, 4)); // 16 = max
        assertEquals(Resize.HALVE, s.decide(64, 3)); // 21 > max, 32 >= 3
        assertEquals(Resize.DOUBLE, s.decide(2, 1)); // 2 < min
        assertEquals(Resize.HALVE, new ResizableSlotScheme(1, 1, 2).decide(8, 1));
        assertEquals(Resize.NONE, new ResizableSlotScheme(1, 1, 2).decide(4, 3)); // 1 per node: within [1, 2]
    }

    @Test
    void doubledKeepsIdsOwnersEpochsOnTheLeftAndNumbersTheRightFromNextId() {
        ResizableSlotScheme s = resizable();
        RoutingTable t = s.initial(nodes(3)); // 16 slots
        t = t.withOwner(new TabletId(5), new NodeId(3)); // epoch 2 on one slot

        RoutingTable d = s.doubled(t);

        assertEquals(32, d.size());
        assertEquals(t.version().value() + 1, d.version().value());
        assertEquals(32, d.nextTabletId());
        assertEquals(TabletRange.uniform(32), d.tablets().stream().map(Tablet::range).toList());
        for (int i = 0; i < 16; i++) {
            Tablet parent = t.tablets().get(i);
            Tablet left = d.tablets().get(2 * i);
            Tablet right = d.tablets().get(2 * i + 1);
            assertEquals(parent.id(), left.id());
            assertEquals(new TabletId(16 + i), right.id());
            assertEquals(parent.owner(), left.owner());
            assertEquals(parent.owner(), right.owner());
            assertEquals(parent.epoch(), left.epoch());
            assertEquals(parent.epoch(), right.epoch());
        }
        // 어느 해시든 주인이 그대로
        Random rnd = new Random(7);
        for (int i = 0; i < 10_000; i++) {
            long h = rnd.nextLong();
            assertEquals(t.lookup(h).owner(), d.lookup(h).owner());
        }
    }

    @Test
    void mergedUndoesDoubledWithoutAnyMove() {
        ResizableSlotScheme s = resizable();
        RoutingTable t = s.initial(nodes(3)).withOwner(new TabletId(5), new NodeId(3));

        RoutingTable back = s.merged(s.doubled(t));

        assertEquals(t.tablets(), back.tablets());
        assertEquals(t.version().value() + 2, back.version().value());
        assertEquals(32, back.nextTabletId(), "ids handed out by doubling are not reused");
    }

    @Test
    void mergedTakesTheNewerEpochAndRejectsMixedSiblings() {
        ResizableSlotScheme s = resizable();
        RoutingTable t = s.initial(nodes(2)); // 8 slots, n1: 0~3, n2: 4~7
        RoutingTable bumped = t.withOwner(new TabletId(1), new NodeId(2)).withOwner(new TabletId(1), new NodeId(1)); // epoch 3

        RoutingTable m = s.merged(bumped);
        assertEquals(4, m.size());
        assertEquals(3, m.tablets().get(0).epoch().value());
        assertEquals(new TabletId(0), m.tablets().get(0).id());

        RoutingTable mixed = t.withOwner(new TabletId(1), new NodeId(2));
        assertThrows(IllegalStateException.class, () -> s.merged(mixed));
    }

    @Test
    void halvingColocatesEverySiblingPairAndLeavesPairsBalanced() {
        ResizableSlotScheme s = resizable();
        RoutingTable t = s.initial(nodes(4)); // 16 slots, 4 each
        // 짝을 섞는다: 홀수 슬롯 몇 개를 다른 노드로
        t = t.withOwner(new TabletId(1), new NodeId(3))
                .withOwner(new TabletId(5), new NodeId(1))
                .withOwner(new TabletId(9), new NodeId(4))
                .withOwner(new TabletId(13), new NodeId(2))
                .withOwner(new TabletId(2), new NodeId(4));
        int mixedPairs = countMixedPairs(t);
        assertTrue(mixedPairs >= 4);

        Plan plan = s.halving(t);

        assertTrue(plan.prepared() == t);
        Set<TabletId> seen = new HashSet<>();
        for (Change c : plan.moves()) {
            assertNotEquals(c.from(), c.to());
            assertEquals(t.get(c.unit()).owner(), c.from());
            assertTrue(seen.add(c.unit()), "a slot is moved at most once");
        }
        RoutingTable after = plan.applyAll();
        assertEquals(0, countMixedPairs(after));
        RoutingTable m = s.merged(after);
        assertEquals(8, m.size());
        assertBalancedWithin(m, nodes(4), 1);
        assertTrue(plan.moves().size() >= mixedPairs, "each mixed pair needs at least one move");
        assertEquals(plan.moves(), s.halving(t).moves(), "halving plan must be deterministic");
    }

    @Test
    void halvingTopsUpBalanceWhenPairsAreUneven() {
        ResizableSlotScheme s = resizable();
        // 8 슬롯, 노드1이 6개(짝 3개), 노드2가 2개(짝 1개) → 합치면 3 vs 1. 짝 하나를 옮겨 2 vs 2
        List<Tablet> tablets = new ArrayList<>();
        List<TabletRange> ranges = TabletRange.uniform(8);
        for (int i = 0; i < 8; i++) {
            tablets.add(new Tablet(new TabletId(i), ranges.get(i), new NodeId(i < 6 ? 1 : 2), OwnershipEpoch.ZERO.next()));
        }
        RoutingTable t = RoutingTable.of(RoutingVersion.ZERO.next(), tablets, 8);

        Plan plan = s.halving(t);

        assertEquals(2, plan.moves().size());
        for (Change c : plan.moves()) {
            assertEquals(new NodeId(1), c.from());
            assertEquals(new NodeId(2), c.to());
        }
        RoutingTable m = s.merged(plan.applyAll());
        assertEquals(2, m.ownedBy(new NodeId(1)).size());
        assertEquals(2, m.ownedBy(new NodeId(2)).size());
    }

    @Test
    void halvingMovesExactlyOneSlotPerMixedPairWhenThatAlreadyBalances() {
        ResizableSlotScheme s = resizable();
        // 8 슬롯. 노드1: 0,1,2,3,4,6  노드2: 5,7 → 짝 (4,5)·(6,7)이 섞임. 둘 다 노드2(짝 0개)로 모으면 노드1 2짝, 노드2 2짝으로 끝
        RoutingTable t = tableOf(new int[] {1, 1, 1, 1, 1, 2, 1, 2});

        Plan plan = s.halving(t);

        RoutingTable after = plan.applyAll();
        assertEquals(0, countMixedPairs(after));
        RoutingTable m = s.merged(after);
        assertBalancedWithin(m, nodes(2), 1);
        assertEquals(2, plan.moves().size(), "two mixed pairs, one move each: " + plan.moves());
        for (Change c : plan.moves()) {
            assertEquals(new NodeId(1), c.from());
            assertEquals(new NodeId(2), c.to());
        }
    }

    @Test
    void halvingTopUpRedirectsAPendingMoveInsteadOfMovingTwice() {
        ResizableSlotScheme s = resizable();
        // 16 슬롯, 노드 3대. 짝0 = (노드2, 노드1), 짝1~3 = (노드1, 노드3), 짝4~7 = 노드3끼리.
        // 2단계에서 짝0~3이 전부 노드1로 모이면(4짝) 노드2는 0짝이라 3단계가 노드1의 마지막 짝(짝3)을 노드2로 보낸다.
        // 짝3의 노드3 슬롯은 "노드3 → 노드1"로 옮기기로 했던 것이므로 목적지만 노드2로 고쳐야 하고, 두 번 옮기면 안 된다
        int[] owner = {2, 1, 1, 3, 1, 3, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3};
        RoutingTable t = tableOf(owner);

        Plan plan = s.halving(t);

        Set<TabletId> seen = new HashSet<>();
        for (Change c : plan.moves()) {
            assertTrue(seen.add(c.unit()), "moved twice: " + c.unit());
            assertEquals(t.get(c.unit()).owner(), c.from());
        }
        RoutingTable after = plan.applyAll();
        assertEquals(0, countMixedPairs(after));
        RoutingTable m = s.merged(after);
        assertBalancedWithin(m, nodes(3), 1);
        assertTrue(plan.moves().size() <= 4 + 1 + 2, "4 mixed pairs + top-up of one mixed pair (1 new move) + one same pair (2): " + plan.moves());
    }

    private static RoutingTable tableOf(int[] owner) {
        List<Tablet> tablets = new ArrayList<>();
        List<TabletRange> ranges = TabletRange.uniform(owner.length);
        for (int i = 0; i < owner.length; i++) {
            tablets.add(new Tablet(new TabletId(i), ranges.get(i), new NodeId(owner[i]), OwnershipEpoch.ZERO.next()));
        }
        return RoutingTable.of(RoutingVersion.ZERO.next(), tablets, owner.length);
    }

    @Test
    void addingNodesDoublesWhenSlotsPerNodeDropBelowHalfTarget() {
        ResizableSlotScheme s = resizable(); // T=4, min 2, max 8
        RoutingTable t = s.initial(nodes(1)); // 4 slots
        assertEquals(4, t.size());

        Plan p2 = s.onNodeAdded(t, new NodeId(2)); // 4/2 = 2 = min → 그대로
        assertTrue(p2.prepared() == t);
        t = p2.applyAll();
        assertEquals(2, t.ownedBy(new NodeId(2)).size());

        Plan p3 = s.onNodeAdded(t, new NodeId(3)); // 4/3 = 1 < 2 → 8개로
        assertEquals(8, p3.prepared().size());
        assertEquals(t.version().value() + 1, p3.prepared().version().value());
        for (Change c : p3.moves()) {
            assertEquals(new NodeId(3), c.to());
            assertEquals(p3.prepared().get(c.unit()).owner(), c.from());
        }
        t = p3.applyAll();
        assertEquals(8, t.size());
        assertBalancedWithin(t, nodes(3), 1);
        assertEquals(2, t.ownedBy(new NodeId(3)).size()); // 8/3 → (3, 3, 2)
    }

    @Test
    void addedNodeAfterDoublingRestoresImbalanceOfTwoToOne() {
        ResizableSlotScheme s = new ResizableSlotScheme(6, 3, 12); // 2배 직전에 (4,3,3,3,3)처럼 차이 1인 표를 만들기 위한 값
        RoutingTable t = s.initial(nodes(2)); // 16 슬롯
        for (int n = 3; n <= 5; n++) {
            t = s.onNodeAdded(t, new NodeId(n)).applyAll();
        }
        assertEquals(16, t.size());
        assertEquals(1, imbalance(t, nodes(5)));

        Plan p = s.onNodeAdded(t, new NodeId(6)); // 16/6 = 2 < 3 → 32개, 차이 2인 표 위에서 새 노드가 가져간다
        assertEquals(32, p.prepared().size());
        assertEquals(2, imbalance(p.prepared(), nodes(5)));
        RoutingTable after = p.applyAll();
        assertEquals(1, imbalance(after, nodes(6)));
        assertEquals(5, after.ownedBy(new NodeId(6)).size()); // 32/6 = 5.33 → 5
    }

    @Test
    void removedNodePrefersTheSiblingOwnerAmongLeastLoaded() {
        ResizableSlotScheme s = resizable();
        // 8 슬롯. 노드1: 0,2,3  노드3: 1,5,7  노드2: 4,6. 노드2 제거: 슬롯 4의 짝 5는 노드3, 노드1·3 동률(3) → 노드3(짝 우선, 작은 id는 노드1)
        List<Tablet> tablets = new ArrayList<>();
        List<TabletRange> ranges = TabletRange.uniform(8);
        int[] owner = {1, 3, 1, 1, 2, 3, 2, 3};
        for (int i = 0; i < 8; i++) {
            tablets.add(new Tablet(new TabletId(i), ranges.get(i), new NodeId(owner[i]), OwnershipEpoch.ZERO.next()));
        }
        RoutingTable t = RoutingTable.of(RoutingVersion.ZERO.next(), tablets, 8);

        Plan plan = s.onNodeRemoved(t, new NodeId(2));

        assertEquals(2, plan.moves().size());
        assertEquals(new TabletId(4), plan.moves().get(0).unit());
        assertEquals(new NodeId(3), plan.moves().get(0).to());
        // 슬롯 6의 짝 7도 노드3이지만 이제 노드3이 4개로 더 많다 → 가장 적은 노드1
        assertEquals(new TabletId(6), plan.moves().get(1).unit());
        assertEquals(new NodeId(1), plan.moves().get(1).to());
        assertBalancedWithin(plan.applyAll(), List.of(new NodeId(1), new NodeId(3)), 1);
    }

    @Test
    void shrinkThenHalveKeepsEveryHashOwnedAndBalanced() {
        ResizableSlotScheme s = resizable(); // T=4, max 8
        RoutingTable t = s.initial(nodes(4)); // 16
        t = s.onNodeRemoved(t, new NodeId(4)).applyAll(); // 16/3 = 5
        t = s.onNodeRemoved(t, new NodeId(3)).applyAll(); // 16/2 = 8 = max → NONE
        assertEquals(Resize.NONE, s.decide(t));
        t = s.onNodeRemoved(t, new NodeId(2)).applyAll(); // 16/1 = 16 > 8 → HALVE
        assertEquals(Resize.HALVE, s.decide(t));

        Plan h = s.halving(t);
        assertTrue(h.moves().isEmpty(), "single owner: nothing to co-locate");
        RoutingTable m = s.merged(h.applyAll());
        assertEquals(8, m.size());
        assertEquals(Resize.NONE, s.decide(m)); // 8/1 = 8 = max
        assertEquals(new NodeId(1), m.lookup(0).owner());
        assertEquals(new NodeId(1), m.lookup(-1L).owner());
    }

    @Test
    void metadataBytesCompressesRunsLikeFixedSlotPlusResizeHeader() {
        ResizableSlotScheme s = resizable();
        RoutingTable two = s.initial(nodes(2));
        assertEquals(24 + 2 * 20, s.metadataBytes(two));
        assertEquals(24 + 2 * 20, s.metadataBytes(s.doubled(two)), "doubling keeps the runs");
    }

    @Test
    void resizeOperationsRejectNonUniformTables() {
        ResizableSlotScheme s = resizable();
        RoutingTable t = s.initial(nodes(2)); // 8 slots
        TabletRange r = t.get(new TabletId(0)).range();
        RoutingTable split = t.withSplit(new TabletId(0), r.start() + 1);

        assertThrows(IllegalStateException.class, () -> s.doubled(split));
        assertThrows(IllegalStateException.class, () -> s.halving(split));
        assertThrows(IllegalStateException.class, () -> s.merged(split));
        RoutingTable three = new FixedSlotScheme(12).initial(nodes(2));
        assertThrows(IllegalStateException.class, () -> s.doubled(three), "12 is not a power of two");
    }

    @Test
    void rejectsBadArguments() {
        assertThrows(IllegalArgumentException.class, () -> new ResizableSlotScheme(0));
        assertThrows(IllegalArgumentException.class, () -> new ResizableSlotScheme(4, 5, 8), "min above target");
        assertThrows(IllegalArgumentException.class, () -> new ResizableSlotScheme(4, 2, 3), "max below target");
        assertThrows(IllegalArgumentException.class, () -> new ResizableSlotScheme(4, 3, 5), "max < 2 * min oscillates");
        assertThrows(IllegalArgumentException.class, () -> resizable().initial(List.of(new NodeId(1), new NodeId(1))));
        assertEquals(512, new ResizableSlotScheme().slotsPerNode());
        assertEquals(256, new ResizableSlotScheme().minPerNode());
        assertEquals(1024, new ResizableSlotScheme().maxPerNode());
        assertEquals("resizable-slot", new ResizableSlotScheme().name());
        assertFalse(resizable().supportsSplit());
        assertTrue(resizable().supportsMove());
    }

    static int countMixedPairs(RoutingTable t) {
        int c = 0;
        for (int k = 0; k < t.size() / 2; k++) {
            if (!t.tablets().get(2 * k).owner().equals(t.tablets().get(2 * k + 1).owner())) {
                c++;
            }
        }
        return c;
    }

    static int imbalance(RoutingTable t, List<NodeId> nodes) {
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (NodeId n : nodes) {
            int count = t.ownedBy(n).size();
            min = Math.min(min, count);
            max = Math.max(max, count);
        }
        return max - min;
    }

    static void assertBalancedWithin(RoutingTable t, List<NodeId> nodes, int allowed) {
        assertTrue(imbalance(t, nodes) <= allowed, "imbalance " + imbalance(t, nodes) + " exceeds " + allowed);
    }
}
