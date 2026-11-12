package io.openkache.runner.benchmark.measurement;

import static org.junit.jupiter.api.Assertions.*;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class LatencyHistogramTest {
    @Test void emptyPopulationHasNullStatisticsRatherThanFakeZero() {
        var h = new LatencyHistogram(1000).snapshot();
        assertEquals(0, h.count()); assertNull(h.sumNanos()); assertNull(h.meanNanos());
        assertNull(h.minimumNanos()); assertNull(h.maximumNanos());
        assertNull(h.quantile(99, 100).upperBoundNanos()); assertEquals("no_samples", h.quantile(99, 100).reason());
    }
    @Test void exactObservedStatisticsAndNearestRankUpperBounds() {
        var h = new LatencyHistogram(1000);
        for (long value : new long[]{0, 1, 99, 100, 101, 102, 103, 104, 105, 106}) { h.record(value); }
        var s = h.snapshot();
        assertEquals(10, s.count()); assertEquals(BigInteger.valueOf(821), s.sumNanos());
        assertEquals(0L, s.minimumNanos()); assertEquals(106L, s.maximumNanos());
        assertEquals(82.1, s.meanNanos()); assertEquals(101L, s.quantile(50, 100).upperBoundNanos());
        assertEquals(107L, s.quantile(95, 100).upperBoundNanos());
        assertEquals(2L, s.buckets().get(103L)); assertEquals(10, s.buckets().values().stream().mapToLong(Long::longValue).sum());
    }
    @Test void rangeOverflowIsCountedAndTargetPercentileIsNull() {
        var h = new LatencyHistogram(100); h.record(5); h.record(101);
        assertEquals(5L, h.snapshot().quantile(50, 100).upperBoundNanos());
        assertEquals("out_of_range", h.snapshot().quantile(99, 100).reason());
        assertEquals(1, h.snapshot().overflowCount()); assertEquals(101L, h.snapshot().maximumNanos());
    }
    @Test void p999RetainsRawEstimateButRequiresHundredThousandSamplesForComparison() {
        var h = new LatencyHistogram(100);
        for (int i = 0; i < 99_999; i++) { h.record(7); }
        assertEquals(7L, h.snapshot().quantile(999, 1000).upperBoundNanos());
        assertFalse(h.snapshot().quantile(999, 1000).comparisonEligible());
        assertEquals("insufficient_samples", h.snapshot().quantile(999, 1000).reason());
        h.record(7); assertTrue(h.snapshot().quantile(999, 1000).comparisonEligible());
    }
    @Test void exactSumDoesNotOverflowLongAndSnapshotDoesNotChange() {
        var h = new LatencyHistogram(Long.MAX_VALUE); h.record(Long.MAX_VALUE);
        var first = h.snapshot(); h.record(Long.MAX_VALUE);
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TWO), h.snapshot().sumNanos());
        assertEquals(1, first.count()); assertEquals(2, h.snapshot().count());
        assertThrows(UnsupportedOperationException.class, () -> first.buckets().put(1L, 2L));
    }
    @Test void everyBinFollowsTheVersionedIntegerRecurrence() {
        var h = new LatencyHistogram(10_000);
        for (long i = 0; i <= 10_000; i++) { h.record(i); }
        var s = h.snapshot(); long previous = 0;
        for (long upper : s.buckets().keySet()) {
            if (upper > 0) { assertEquals(previous + Math.max(1, (previous + 99) / 100), upper); }
            previous = upper;
        }
        assertEquals(10_001, s.overflowCount() + s.buckets().values().stream().mapToLong(Long::longValue).sum());
        assertThrows(IllegalArgumentException.class, () -> h.record(-1));
        assertThrows(IllegalArgumentException.class, () -> new LatencyHistogram(0));
        assertThrows(IllegalArgumentException.class, () -> s.quantile(0, 100));
    }
}
