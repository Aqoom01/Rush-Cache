package io.openkache.core.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RoutingTableTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);
    private static final OwnershipEpoch E1 = OwnershipEpoch.ZERO.next();

    /** 공간을 {@code n}등분해 노드에 라운드로빈으로 배치한 표. 버전 1, epoch 1. */
    private static RoutingTable uniform(int n, NodeId... nodes) {
        List<TabletRange> ranges = TabletRange.uniform(n);
        List<Tablet> tablets = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tablets.add(new Tablet(new TabletId(i), ranges.get(i), nodes[i % nodes.length], E1));
        }
        return RoutingTable.of(RoutingVersion.ZERO.next(), tablets, n);
    }

    @Test
    void lookupFindsTabletAtEveryBoundary() {
        RoutingTable t = uniform(4, N1, N2);
        List<Tablet> tablets = t.tablets();

        assertSame(tablets.get(0), t.lookup(0));
        assertSame(tablets.get(0), t.lookup(tablets.get(1).range().start() - 1));
        assertSame(tablets.get(1), t.lookup(tablets.get(1).range().start()));
        assertSame(tablets.get(2), t.lookup(Long.MIN_VALUE)); // 2^63
        assertSame(tablets.get(3), t.lookup(-1L)); // 2^64 - 1
    }

    @Test
    void lookupAgreesWithContainsForManyHashes() {
        RoutingTable t = uniform(37, N1, N2, N3);
        long h = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < 100_000; i++) {
            h = h * 6364136223846793005L + 1442695040888963407L;
            assertTrue(t.lookup(h).range().contains(h));
        }
    }

    @Test
    void rejectsGapOverlapAndMissingEnds() {
        TabletRange a = new TabletRange(0, 100);
        TabletRange gap = new TabletRange(101, 0);
        TabletRange overlap = new TabletRange(99, 0);
        TabletRange b = new TabletRange(100, 0);

        assertThrows(IllegalArgumentException.class, () -> table(a, gap));
        assertThrows(IllegalArgumentException.class, () -> table(a, overlap));
        // 가운데 Tablet이 2^64까지 가고 다음이 0부터 다시 시작하면 "end == start" 검사만으로는 통과하므로 따로 막는다
        assertThrows(IllegalArgumentException.class, () -> table(a, b, new TabletRange(0, 50), new TabletRange(50, 0)));
        assertThrows(IllegalArgumentException.class, () -> table(a)); // 끝이 2^64가 아님
        assertThrows(IllegalArgumentException.class, () -> table(b)); // 시작이 0이 아님
        assertThrows(IllegalArgumentException.class, () -> RoutingTable.of(RoutingVersion.ZERO, List.of(), 0));
    }

    @Test
    void rejectsDuplicateIdsAndLowNextId() {
        List<TabletRange> r = TabletRange.uniform(2);
        Tablet x = new Tablet(new TabletId(7), r.get(0), N1, E1);
        Tablet y = new Tablet(new TabletId(7), r.get(1), N1, E1);

        assertThrows(IllegalArgumentException.class, () -> RoutingTable.of(RoutingVersion.ZERO, List.of(x, y), 8));
        assertThrows(IllegalArgumentException.class, () -> RoutingTable.of(RoutingVersion.ZERO, List.of(x), 7));
    }

    @Test
    void withOwnerBumpsOnlyThatEpochAndTheVersion() {
        RoutingTable t0 = uniform(4, N1, N2);
        TabletId id = new TabletId(1);

        RoutingTable t1 = t0.withOwner(id, N3);

        assertEquals(2, t1.version().value());
        assertEquals(N3, t1.get(id).owner());
        assertEquals(2, t1.get(id).epoch().value());
        for (TabletId other : List.of(new TabletId(0), new TabletId(2), new TabletId(3))) {
            assertEquals(t0.get(other), t1.get(other));
        }
        // 입력 표는 그대로
        assertEquals(1, t0.version().value());
        assertEquals(N2, t0.get(id).owner());
        assertThrows(IllegalArgumentException.class, () -> t1.withOwner(id, N3));
    }

    @Test
    void applyChecksCurrentOwner() {
        RoutingTable t0 = uniform(2, N1, N2);
        TabletId id = new TabletId(0);

        RoutingTable t1 = t0.apply(new Change(id, N1, N3));
        assertEquals(N3, t1.get(id).owner());

        assertThrows(IllegalStateException.class, () -> t1.apply(new Change(id, N1, N2))); // 낡은 계획
        assertThrows(IllegalArgumentException.class, () -> t1.apply(new Change(new TabletId(99), N1, N2)));
    }

    @Test
    void splitKeepsOwnerAndEpochAndIssuesNextId() {
        RoutingTable t0 = uniform(2, N1, N2);
        Tablet parent = t0.get(new TabletId(0));
        long at = parent.range().end() / 2;

        RoutingTable t1 = t0.withSplit(parent.id(), at);

        assertEquals(3, t1.size());
        assertEquals(2, t1.version().value());
        assertEquals(3, t1.nextTabletId());
        Tablet left = t1.get(new TabletId(0));
        Tablet right = t1.get(new TabletId(2));
        assertEquals(new TabletRange(0, at), left.range());
        assertEquals(new TabletRange(at, parent.range().end()), right.range());
        assertEquals(parent.owner(), right.owner());
        assertEquals(parent.epoch(), right.epoch());
        assertSame(right, t1.lookup(at));
        assertEquals(2, t0.size()); // 입력 표는 그대로
    }

    @Test
    void mergeRequiresAdjacentTabletsWithSameOwner() {
        RoutingTable t0 = uniform(4, N1, N2);
        TabletId a = new TabletId(0);
        TabletId b = new TabletId(1);

        assertThrows(IllegalArgumentException.class, () -> t0.withMerge(a, b)); // 주인이 다름
        assertThrows(IllegalArgumentException.class, () -> t0.withMerge(a, new TabletId(2))); // 인접하지 않음

        RoutingTable t1 = t0.withOwner(b, N1); // b의 epoch가 2가 됨
        RoutingTable t2 = t1.withMerge(a, b);

        assertEquals(3, t2.size());
        assertEquals(3, t2.version().value());
        Tablet merged = t2.get(a);
        assertEquals(t0.get(a).range().start(), merged.range().start());
        assertEquals(t0.get(b).range().end(), merged.range().end());
        assertEquals(2, merged.epoch().value()); // 둘 중 큰 epoch
        assertThrows(IllegalArgumentException.class, () -> t2.get(b));
    }

    @Test
    void ownedByAndNodes() {
        RoutingTable t = uniform(5, N1, N2);

        assertEquals(3, t.ownedBy(N1).size());
        assertEquals(2, t.ownedBy(N2).size());
        assertEquals(0, t.ownedBy(N3).size());
        assertEquals(Set.of(N1, N2), t.nodes());
    }

    private static RoutingTable table(TabletRange... ranges) {
        List<Tablet> tablets = new ArrayList<>();
        for (int i = 0; i < ranges.length; i++) {
            tablets.add(new Tablet(new TabletId(i), ranges[i], N1, E1));
        }
        return RoutingTable.of(RoutingVersion.ZERO, tablets, ranges.length);
    }
}
