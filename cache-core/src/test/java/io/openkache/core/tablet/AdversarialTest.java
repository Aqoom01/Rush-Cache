package io.openkache.core.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** 극단값·비정상 입력·동시성으로 깨뜨리려는 테스트. 각 케이스가 무엇을 노리는지 주석에 적는다. */
class AdversarialTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final OwnershipEpoch E1 = OwnershipEpoch.ZERO.next();

    // ---- KeyHash: 부호 있는 byte 를 잘못 다루면 0x80 이상 바이트에서 틀린다 ----

    @Test
    void hashHandlesHighBytesLikeTheReference() {
        byte[] ff17 = new byte[17];
        java.util.Arrays.fill(ff17, (byte) 0xff);
        assertEquals(0xc73247cd7aeaa5b0L, KeyHash.of(Bytes.copyOf(ff17)));

        byte[] hi16 = {(byte) 0x80, (byte) 0x81, (byte) 0xfe, (byte) 0xff, 0x7f, 0x00, (byte) 0xc3, (byte) 0xa9,
            (byte) 0x80, (byte) 0x81, (byte) 0xfe, (byte) 0xff, 0x7f, 0x00, (byte) 0xc3, (byte) 0xa9};
        assertEquals(0x878680b80abe9d4eL, KeyHash.of(Bytes.copyOf(hi16)));

        byte[] hi23 = new byte[23];
        System.arraycopy(hi16, 0, hi23, 0, 16);
        byte[] tail = {(byte) 0xff, (byte) 0xfe, (byte) 0xfd, (byte) 0xfc, (byte) 0xfb, (byte) 0xfa, (byte) 0xf9};
        System.arraycopy(tail, 0, hi23, 16, 7);
        assertEquals(0x9ba0f9ec04498d14L, KeyHash.of(Bytes.copyOf(hi23)));

        assertEquals(0x7ab9f915c07ac754L, KeyHash.of(Bytes.copyOf(new byte[33])));
    }

    @Test
    void hashOfOneMegabyteKeyMatchesReference() {
        byte[] big = new byte[1 << 20];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 31);
        }
        assertEquals(0x355074b0596b1f19L, KeyHash.of(Bytes.copyOf(big)));
    }

    // ---- TabletRange: 공간 양끝의 길이 1 구간, 2^63 과 2^64-1 에서의 split ----

    @Test
    void lengthOneRangesAtBothEndsOfTheSpace() {
        TabletRange first = new TabletRange(0, 1);
        TabletRange last = new TabletRange(-1L, 0);

        assertTrue(first.contains(0));
        assertTrue(last.contains(-1L));
        assertThrows(IllegalArgumentException.class, () -> first.splitAt(0));
        assertThrows(IllegalArgumentException.class, () -> last.splitAt(-1L));
        assertThrows(IllegalArgumentException.class, () -> last.splitAt(0)); // 0 은 2^64 이 아니라 공간의 처음
    }

    @Test
    void splitFullRangeAtTheSignBoundaryAndAtTheLastPoint() {
        TabletRange.Split atHalf = TabletRange.FULL.splitAt(Long.MIN_VALUE);
        assertEquals(new TabletRange(0, Long.MIN_VALUE), atHalf.left());
        assertEquals(new TabletRange(Long.MIN_VALUE, 0), atHalf.right());

        TabletRange.Split atLast = TabletRange.FULL.splitAt(-1L);
        assertEquals(new TabletRange(-1L, 0), atLast.right());
        assertTrue(atLast.right().contains(-1L));
        assertTrue(atLast.left().contains(-2L));
    }

    // ---- RoutingTable: 극단 크기, 극단 모양 ----

    @Test
    void sixtyFourThousandTabletsLookupEveryBoundary() {
        int n = 1 << 16;
        List<TabletRange> ranges = TabletRange.uniform(n);
        List<Tablet> tablets = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tablets.add(new Tablet(new TabletId(i), ranges.get(i), i % 2 == 0 ? N1 : N2, E1));
        }
        RoutingTable t = RoutingTable.of(RoutingVersion.ZERO.next(), tablets, n);

        for (int i = 0; i < n; i++) {
            long s = ranges.get(i).start();
            assertSame(tablets.get(i), t.lookup(s));
            if (i > 0) {
                assertSame(tablets.get(i - 1), t.lookup(s - 1));
            }
        }
        assertSame(tablets.get(n - 1), t.lookup(-1L));
    }

    @Test
    void splitDownToSingletonsAndMergeBack() {
        // [0, 4) 를 1짜리 네 조각으로 쪼갰다가 다시 하나로. 길이 1 조각은 더 못 쪼갠다.
        Tablet whole = new Tablet(new TabletId(0), new TabletRange(0, 4), N1, E1);
        Tablet rest = new Tablet(new TabletId(1), new TabletRange(4, 0), N2, E1);
        RoutingTable t = RoutingTable.of(RoutingVersion.ZERO.next(), List.of(whole, rest), 2);

        t = t.withSplit(new TabletId(0), 2); // [0,2) id0, [2,4) id2
        t = t.withSplit(new TabletId(0), 1); // [0,1) id0, [1,2) id3
        t = t.withSplit(new TabletId(2), 3); // [2,3) id2, [3,4) id4
        assertEquals(5, t.size());
        RoutingTable tiny = t;
        assertThrows(IllegalArgumentException.class, () -> tiny.withSplit(new TabletId(0), 1));
        for (long h = 0; h < 4; h++) {
            assertEquals(1L, lengthOf(t.lookup(h).range()));
        }

        t = t.withMerge(new TabletId(0), new TabletId(3)); // [0,2)
        t = t.withMerge(new TabletId(2), new TabletId(4)); // [2,4)
        t = t.withMerge(new TabletId(0), new TabletId(2)); // [0,4)
        assertEquals(2, t.size());
        assertEquals(new TabletRange(0, 4), t.get(new TabletId(0)).range());
        assertEquals(5, t.nextTabletId(), "retired ids must not be handed out again");
        RoutingTable merged = t;
        assertThrows(IllegalArgumentException.class, () -> merged.get(new TabletId(3)));
        assertThrows(IllegalArgumentException.class, () -> merged.apply(new Change(new TabletId(4), N1, N2)));
    }

    @Test
    void mergeInWrongOrderIsRejectedNotSilentlyAccepted() {
        RoutingTable t = twoHalves(N1, N1);

        assertThrows(IllegalArgumentException.class, () -> t.withMerge(new TabletId(1), new TabletId(0)));
        assertEquals(1, t.withMerge(new TabletId(0), new TabletId(1)).size());
    }

    @Test
    void nextTabletIdOverflowIsLoudNotSilent() {
        Tablet a = new Tablet(new TabletId(0), new TabletRange(0, Long.MIN_VALUE), N1, E1);
        Tablet b = new Tablet(new TabletId(1), new TabletRange(Long.MIN_VALUE, 0), N1, E1);
        RoutingTable t = RoutingTable.of(RoutingVersion.ZERO.next(), List.of(a, b), Long.MAX_VALUE);

        // Long.MAX_VALUE 를 발급하면 다음 식별자를 표현할 수 없으므로, 조용히 음수로 넘어가지 않고 그 split 자체가 실패해야 한다
        assertThrows(ArithmeticException.class, () -> t.withSplit(new TabletId(0), 1));
        RoutingTable room = RoutingTable.of(RoutingVersion.ZERO.next(), List.of(a, b), Long.MAX_VALUE - 1);
        assertEquals(Long.MAX_VALUE, room.withSplit(new TabletId(0), 1).nextTabletId());
    }

    // ---- FixedSlotScheme: 슬롯 1~2개, 극단 NodeId, 역순 노드 ----

    @Test
    void oneSlotOneNodeIsTheDegenerateButValidCluster() {
        FixedSlotScheme one = new FixedSlotScheme(1);
        RoutingTable t = one.initial(List.of(N1));

        assertEquals(1, t.size());
        assertEquals(TabletRange.FULL, t.lookup(12345).range());
        assertThrows(IllegalArgumentException.class, () -> one.onNodeAdded(t, N2)); // 줄 슬롯이 없다
        assertThrows(IllegalStateException.class, () -> one.onNodeRemoved(t, N1));
    }

    @Test
    void twoSlotsTwoNodesRemoveOneGivesEverythingToTheOther() {
        FixedSlotScheme two = new FixedSlotScheme(2);
        RoutingTable t = two.onNodeRemoved(two.initial(List.of(N1, N2)), N1).applyAll();

        assertEquals(2, t.ownedBy(N2).size());
        assertEquals(0, t.ownedBy(N1).size());
    }

    @Test
    void extremeNodeIdsAndReversedOrderStillBalance() {
        NodeId zero = new NodeId(0);
        NodeId max = new NodeId(Long.MAX_VALUE);
        FixedSlotScheme s = new FixedSlotScheme(7); // 7 은 2 로 나누어떨어지지 않는다

        RoutingTable t = s.initial(List.of(max, zero));
        assertEquals(4, t.ownedBy(max).size()); // 라운드로빈은 목록 순서. 첫 노드가 하나 더
        assertEquals(3, t.ownedBy(zero).size());

        RoutingTable t3 = s.onNodeAdded(t, new NodeId(5)).applyAll();
        int a = t3.ownedBy(max).size();
        int b = t3.ownedBy(zero).size();
        int c = t3.ownedBy(new NodeId(5)).size();
        assertEquals(7, a + b + c);
        assertTrue(Math.max(a, Math.max(b, c)) - Math.min(a, Math.min(b, c)) <= 1);
        assertEquals(2, c); // 7 / 3
    }

    @Test
    void donorRuleIsMostLoadedThenSmallestNodeId() {
        // 노드 1, 2, 3 이 슬롯 64 개를 22/21/21 로 가진다. 새 노드는 16 개를 받는다.
        // 처음 한 개는 가장 많이 가진 1 에서, 그다음 동률(21/21/21)에서는 NodeId 가 작은 1 에서, 그다음 2, 3 순서여야 한다.
        FixedSlotScheme s = new FixedSlotScheme(64);
        RoutingTable t = s.initial(List.of(N1, N2, new NodeId(3)));

        List<Change> moves = s.onNodeAdded(t, new NodeId(4)).moves();

        assertEquals(N1, moves.get(0).from());
        assertEquals(N1, moves.get(1).from());
        assertEquals(N2, moves.get(2).from());
        assertEquals(new NodeId(3), moves.get(3).from());
        assertEquals(N1, moves.get(4).from());
    }

    // ---- 동시성: 불변 객체는 여러 thread 에서 동시에 읽고 파생해도 결과가 같아야 한다 ----

    @Test
    void concurrentLookupsAndDerivationsAreConsistent() throws Exception {
        FixedSlotScheme s = new FixedSlotScheme(4096);
        RoutingTable base = s.initial(List.of(N1, N2, new NodeId(3)));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Long>> results = new ArrayList<>();
            for (int w = 0; w < 8; w++) {
                final int worker = w;
                results.add(pool.submit(() -> {
                    long checksum = 0;
                    RoutingTable mine = base;
                    for (int i = 0; i < 20_000; i++) {
                        long h = (worker + 1) * 0x9E3779B97F4A7C15L * (i + 1);
                        Tablet found = base.lookup(h);
                        checksum = checksum * 31 + found.id().value();
                        if (i % 1000 == 0) {
                            mine = mine.withOwner(found.id(), found.owner().equals(N1) ? N2 : N1);
                        }
                    }
                    return checksum;
                }));
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
            for (Future<Long> f : results) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(1, base.version().value(), "base table must be untouched by concurrent derivations");
    }

    private static RoutingTable twoHalves(NodeId left, NodeId right) {
        Tablet a = new Tablet(new TabletId(0), new TabletRange(0, Long.MIN_VALUE), left, E1);
        Tablet b = new Tablet(new TabletId(1), new TabletRange(Long.MIN_VALUE, 0), right, E1);
        return RoutingTable.of(RoutingVersion.ZERO.next(), List.of(a, b), 2);
    }

    private static long lengthOf(TabletRange r) {
        return r.end() - r.start();
    }
}
