package io.openkache.core.tablet.experiment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 실험 틀 자체의 검증. 손으로 계산할 수 있는 작은 입력으로 지표 함수가 맞는 값을 내는지 본다. */
class MetricsTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final List<NodeId> TWO = List.of(N1, N2);

    /** [0, 2^63) → 노드1, [2^63, 2^64) → 노드2. */
    private static RoutingTable halves(NodeId left, NodeId right) {
        OwnershipEpoch e = OwnershipEpoch.ZERO.next();
        Tablet a = new Tablet(new TabletId(0), new TabletRange(0, Long.MIN_VALUE), left, e);
        Tablet b = new Tablet(new TabletId(1), new TabletRange(Long.MIN_VALUE, 0), right, e);
        return RoutingTable.of(RoutingVersion.ZERO.next(), List.of(a, b), 2);
    }

    @Test
    void unitAndKeyBalanceOnHandMadeTable() {
        RoutingTable t = halves(N1, N2);
        long[] hashes = {1, 2, 3, Long.MIN_VALUE}; // 3개는 노드1, 1개는 노드2

        assertEquals(1.0, Metrics.unitMaxMean(t, TWO), 1e-12);
        assertEquals(3 / 2.0, Metrics.keyMaxMean(t, hashes, TWO), 1e-12);
    }

    @Test
    void movedRatioCountsOnlyKeysWhoseOwnerChanged() {
        RoutingTable before = halves(N1, N2);
        RoutingTable after = halves(N1, N1);
        long[] hashes = {1, 2, Long.MIN_VALUE, -1L}; // 뒤 두 개가 노드2 → 노드1

        assertEquals(0.5, Metrics.movedRatio(before, after, hashes), 1e-12);
        assertEquals(0.0, Metrics.movedRatio(before, before, hashes), 1e-12);
    }

    @Test
    void requestBalanceWeightsByRequestCount() {
        RoutingTable t = halves(N1, N2);
        long[] hashes = {1, Long.MIN_VALUE};
        int[] requests = {90, 10};

        assertEquals(90 / 50.0, Metrics.requestMaxMean(t, hashes, requests, TWO), 1e-12);
        assertArrayEquals(new long[] {90, 10}, Metrics.unitLoads(t, hashes, requests));
        assertEquals(0.9, Metrics.hottestUnitShare(new long[] {90, 10}), 1e-12);
        assertEquals(0.9, Metrics.hottestKeyShare(requests), 1e-12);
    }

    @Test
    void groupMetricsOnHandMadeTable() {
        RoutingTable t = halves(N1, N2);
        long[] hashes = {1, 2, Long.MIN_VALUE, Long.MIN_VALUE + 1, 3}; // 노드1, 노드1, 노드2, 노드2, 노드1

        // 묶음 0 = {key0, key1} 전부 노드1, 묶음 1 = {key2, key3} 전부 노드2, 묶음 2 = {key4} 혼자
        int[] together = {0, 0, 1, 1, 2};
        assertEquals(0.0, Metrics.groupsSplitRatio(t, hashes, together, 3), 1e-12);
        assertEquals(1.0, Metrics.sameNodePairRatio(t, hashes, together, 3), 1e-12);

        // 묶음 0 = {key0, key2} 노드1+노드2, 묶음 1 = {key1, key3} 노드1+노드2, 묶음 2 = {key4}
        int[] apart = {0, 1, 0, 1, 2};
        assertEquals(1.0, Metrics.groupsSplitRatio(t, hashes, apart, 3), 1e-12);
        assertEquals(0.0, Metrics.sameNodePairRatio(t, hashes, apart, 3), 1e-12);

        // 묶음 0 = {key0, key1, key2}: 노드1 둘, 노드2 하나 → 순서쌍 6개 중 같은 노드 2개. 묶음 1 = {key3, key4}: 노드2+노드1 → 0개
        int[] mixed = {0, 0, 0, 1, 1};
        assertEquals(1.0, Metrics.groupsSplitRatio(t, hashes, mixed, 2), 1e-12);
        assertEquals(2 / 8.0, Metrics.sameNodePairRatio(t, hashes, mixed, 2), 1e-12);

        assertEquals(3 / 5.0, Metrics.largestGroupShare(mixed, 2), 1e-12);
    }

    @Test
    void prefixedKeysAreGroupedAndReproducible() {
        Workload.Prefixed a = Workload.prefixedKeys(10, 1000, 0.0, 5);
        Workload.Prefixed b = Workload.prefixedKeys(10, 1000, 0.0, 5);

        assertEquals(1000, a.keys().length);
        assertArrayEquals(a.groupOf(), b.groupOf());
        assertEquals(a.keys()[0], b.keys()[0]);
        for (int i = 0; i < a.keys().length; i++) {
            String key = new String(a.keys()[i].toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(key.startsWith("{g" + a.groupOf()[i] + "}:"), key);
        }
        assertEquals(0.1, Metrics.largestGroupShare(a.groupOf(), 10), 0.05);
        assertTrue(Metrics.largestGroupShare(Workload.prefixedKeys(10, 1000, 0.99, 5).groupOf(), 10) > 0.25);
    }

    @Test
    void greedyBoundIsLptOnUnitLoads() {
        // 단위 부하 7, 6, 5, 4 를 노드 2대에: 7+4=11, 6+5=11 → 완벽 균형 1.0
        assertEquals(1.0, Metrics.greedyBound(new long[] {7, 6, 5, 4}, 2), 1e-12);
        // 단위 하나가 90이면 어떻게 해도 그 노드가 90 이상: 90 / (100/2) = 1.8
        assertEquals(1.8, Metrics.greedyBound(new long[] {90, 5, 5}, 2), 1e-12);
        // 빈 부하
        assertEquals(1.0, Metrics.greedyBound(new long[] {0, 0}, 2), 1e-12);
    }

    @Test
    void zipfIsReproducibleAndSkewed() {
        int[] a = Workload.zipfRequests(1000, 100_000, 0.99, 7);
        int[] b = Workload.zipfRequests(1000, 100_000, 0.99, 7);
        int[] c = Workload.zipfRequests(1000, 100_000, 0.99, 8);

        assertArrayEquals(a, b);
        assertTrue(!java.util.Arrays.equals(a, c));
        long total = 0;
        int max = 0;
        for (int r : a) {
            total += r;
            max = Math.max(max, r);
        }
        assertEquals(100_000, total);
        assertTrue(max > 100_000 / 1000 * 10, "top key must be far above the 100-per-key average: " + max);
        int[] uniform = Workload.zipfRequests(1000, 100_000, 0.0, 7);
        int maxU = 0;
        for (int r : uniform) {
            maxU = Math.max(maxU, r);
        }
        assertTrue(maxU < 200, "theta=0 should be nearly uniform: " + maxU);
    }

    @Test
    void keyHashesAreReproducible() {
        assertArrayEquals(Workload.keyHashes(1000, 3), Workload.keyHashes(1000, 3));
        assertTrue(!java.util.Arrays.equals(Workload.keyHashes(1000, 3), Workload.keyHashes(1000, 4)));
    }
}
