package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LatencyReservoirTest {

    @Test
    void capacityBoundsSampleCountEvenWithManyMoreOffers() {
        LatencyReservoir reservoir = new LatencyReservoir(100, 1);

        for (long i = 1; i <= 1_000_000; i++) {
            reservoir.offer(i);
        }

        assertEquals(100, reservoir.sampleCount());
        assertEquals(1_000_000, reservoir.totalObserved());
    }

    @Test
    void percentilesAreApproximatelyCorrectForUniformLatencies() {
        LatencyReservoir reservoir = new LatencyReservoir(4096, 7);

        for (long i = 1; i <= 100_000; i++) {
            reservoir.offer(i); // 1..100_000 나노초, 균등
        }

        long p50 = reservoir.percentileNanos(0.50);
        long p99 = reservoir.percentileNanos(0.99);

        assertTrue(p50 > 40_000 && p50 < 60_000, "p50=" + p50);
        assertTrue(p99 > 95_000 && p99 <= 100_000, "p99=" + p99);
    }

    @Test
    void emptyReservoirPercentileIsZero() {
        LatencyReservoir reservoir = new LatencyReservoir(10, 1);

        assertEquals(0, reservoir.percentileNanos(0.50));
        assertEquals(0, reservoir.sampleCount());
    }

    @Test
    void offersBelowCapacityAreAllKept() {
        LatencyReservoir reservoir = new LatencyReservoir(10, 1);

        for (long i = 1; i <= 5; i++) {
            reservoir.offer(i);
        }

        assertEquals(5, reservoir.sampleCount());
        assertEquals(5, reservoir.totalObserved());
        assertEquals(5, reservoir.percentileNanos(1.0));
    }
}
