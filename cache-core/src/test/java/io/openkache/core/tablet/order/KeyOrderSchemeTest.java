package io.openkache.core.tablet.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.kv.KvEntry;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.DistributionSchemeContract;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class KeyOrderSchemeTest extends DistributionSchemeContract {

    private static final int T = 8;

    @Override
    protected DistributionScheme scheme() {
        return new KeyOrderScheme(T);
    }

    @Override
    protected boolean usesCommonKeyHash() {
        return false; // 순서 보존 변환이라 KeyHash와 다르다
    }

    @Test
    void pointIsTheFirstEightBytesBigEndianZeroPadded() {
        assertEquals(0L, KeyOrderScheme.pointOf(Bytes.empty()));
        assertEquals(0x41L << 56, KeyOrderScheme.pointOf(Bytes.utf8("A")));
        assertEquals(0x4142L << 48, KeyOrderScheme.pointOf(Bytes.utf8("AB")));
        assertEquals(0x4142434445464748L, KeyOrderScheme.pointOf(Bytes.utf8("ABCDEFGH")));
        assertEquals(0x4142434445464748L, KeyOrderScheme.pointOf(Bytes.utf8("ABCDEFGHIJK")), "bytes after the 8th are ignored");
        assertEquals(-1L, KeyOrderScheme.pointOf(Bytes.copyOf(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, 0})), "0xFF.. is the top of the space");
        assertEquals(KeyOrderScheme.pointOf(Bytes.utf8("x")), scheme().keyHash(Bytes.utf8("x")));
    }

    @Test
    void pointOrderMatchesByteOrderWithinTheFirstEightBytes() {
        Random rnd = new Random(3);
        for (int i = 0; i < 20_000; i++) {
            byte[] a = new byte[rnd.nextInt(9)];
            byte[] b = new byte[rnd.nextInt(9)];
            rnd.nextBytes(a);
            rnd.nextBytes(b);
            Bytes ka = Bytes.copyOf(a);
            Bytes kb = Bytes.copyOf(b);
            int byKey = Integer.signum(KvEntry.KEY_ORDER.compare(ka, kb));
            int byPoint = Long.compareUnsigned(KeyOrderScheme.pointOf(ka), KeyOrderScheme.pointOf(kb));
            // 한쪽이 다른 쪽의 앞부분이고 나머지가 전부 0이면 점은 같지만 key 순서는 짧은 쪽이 앞선다(0 채움의 한계)
            if (byPoint == 0) {
                continue;
            }
            assertEquals(byKey, Integer.signum(byPoint), ka + " vs " + kb);
        }
    }

    @Test
    void sequentialKeysLandInOneTabletOfTheUniformTableAndSpreadAfterPresplit() {
        KeyOrderScheme s = new KeyOrderScheme(T);
        List<Bytes> keys = sequential(10_000, 1L << 40);
        RoutingTable uniform = s.initial(nodes(4));
        Map<Tablet, Integer> hits = countByTablet(uniform, s, keys);
        assertEquals(1, hits.size(), "all sequential keys fall into one uniform tablet: " + hits.keySet());

        RoutingTable presplit = s.initial(nodes(4), keys);
        assertEquals(4 * T, presplit.size());
        Map<Tablet, Integer> spread = countByTablet(presplit, s, keys);
        assertEquals(4 * T, spread.size(), "every presplit tablet holds some sample keys");
        for (int c : spread.values()) {
            assertTrue(Math.abs(c - 10_000 / (4 * T)) <= 1, "count " + c);
        }
        for (NodeId n : nodes(4)) {
            assertEquals(T, presplit.ownedBy(n).size());
        }
        // 표본 밖(더 큰 key)은 전부 마지막 Tablet으로 간다: 순차 삽입의 hot spot
        Tablet last = presplit.tablets().get(presplit.size() - 1);
        for (Bytes k : sequential(100, (1L << 40) + 10_000)) {
            assertEquals(last, presplit.lookup(s.keyHash(k)));
        }
    }

    @Test
    void presplitCoversWholeSpaceDropsDuplicateBoundariesAndRejectsTooFew() {
        KeyOrderScheme s = new KeyOrderScheme(2);
        List<Bytes> samples = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            samples.add(Bytes.utf8("same-prefix-" + (i % 3))); // 앞 8바이트가 전부 같다 → 점 하나
        }
        // 서로 다른 점이 하나(p ≠ 0)면 경계도 하나 → 구간 [0, p)·[p, 2^64) 둘. 노드 2대까지는 되고 3대는 안 된다
        assertEquals(2, s.initial(nodes(2), samples).size());
        assertThrows(IllegalArgumentException.class, () -> s.initial(nodes(3), samples), "two ranges cannot serve 3 nodes");
        assertThrows(IllegalArgumentException.class, () -> s.initial(nodes(1), List.of()));

        List<Bytes> few = List.of(Bytes.utf8("a"), Bytes.utf8("b"), Bytes.utf8("c"), Bytes.utf8("d"));
        RoutingTable t = s.initial(nodes(2), few); // 4개를 원하지만 서로 다른 경계는 3개까지
        assertEquals(4, t.size());
        assertEquals(0, t.tablets().get(0).range().start());
        assertEquals(0, t.tablets().get(t.size() - 1).range().end());
        assertEquals(1, t.version().value());
        assertEquals(t.size(), t.nextTabletId());
    }

    @Test
    void tableAndPlansAreExactlyHashRangeTablet() {
        DistributionScheme s = scheme();
        HashRangeScheme range = new HashRangeScheme(T);
        RoutingTable a = s.initial(nodes(5));
        RoutingTable b = range.initial(nodes(5));
        assertEquals(b.tablets(), a.tablets());

        Plan addA = s.onNodeAdded(a, new NodeId(6));
        Plan addB = range.onNodeAdded(b, new NodeId(6));
        assertEquals(addB.moves(), addA.moves());
        Plan removeA = s.onNodeRemoved(a, new NodeId(2));
        Plan removeB = range.onNodeRemoved(b, new NodeId(2));
        assertEquals(removeB.moves(), removeA.moves());
        assertEquals(range.metadataBytes(a), s.metadataBytes(a));
        assertTrue(s.supportsSplit());
        assertTrue(s.supportsMove());
        assertNotEquals(range.keyHash(Bytes.utf8("abc")), s.keyHash(Bytes.utf8("abc")));
    }

    @Test
    void nameAndTabletsPerNode() {
        assertEquals("key-order-tablet", scheme().name());
        assertEquals(T, new KeyOrderScheme(T).tabletsPerNode());
        assertEquals(HashRangeScheme.DEFAULT_TABLETS_PER_NODE, new KeyOrderScheme().tabletsPerNode());
    }

    /** 8바이트 big-endian 번호 + 8바이트 꼬리. */
    private static List<Bytes> sequential(int count, long from) {
        List<Bytes> l = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] k = new byte[16];
            long v = from + i;
            for (int b = 7; b >= 0; b--) {
                k[b] = (byte) v;
                v >>>= 8;
            }
            l.add(Bytes.copyOf(k));
        }
        return l;
    }

    private static Map<Tablet, Integer> countByTablet(RoutingTable t, DistributionScheme s, List<Bytes> keys) {
        Map<Tablet, Integer> m = new HashMap<>();
        for (Bytes k : keys) {
            m.merge(t.lookup(s.keyHash(k)), 1, Integer::sum);
        }
        return m;
    }
}
