package io.openkache.runner.benchmark.execution;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class FixedRateScheduleTest {
    @Test void fractionalNanosecondsDoNotAccumulateDrift() {
        var s = new FixedRateSchedule(7, 3);
        assertEquals(0, s.offsetNanos(0));
        assertEquals(333_333_333L, s.offsetNanos(1));
        assertEquals(666_666_666L, s.offsetNanos(2));
        assertEquals(1_000_000_000L, s.offsetNanos(3));
        assertEquals(2_000_000_000L, s.offsetNanos(6));
        assertEquals(1, s.dueCountAt(333_333_332L));
        assertEquals(2, s.dueCountAt(333_333_333L));
    }
    @Test void dueCountIsTheExactInverseAtAndBeforeEachBoundary() {
        for (long rate : new long[]{1, 3, 7, 1000, 1_000_000_000L}) {
            var s = new FixedRateSchedule(1007, rate);
            for (int i = 0; i < 1007; i++) {
                assertEquals(i + 1, s.dueCountAt(s.offsetNanos(i)));
                assertEquals(i, s.dueCountAt(s.offsetNanos(i) - 1));
            }
            assertEquals(1007, s.dueCountAt(Long.MAX_VALUE));
        }
    }
    @Test void emptyAndSingleArrivalHaveExplicitBoundaries() {
        assertEquals(0, new FixedRateSchedule(0, 7).dueCountAt(0));
        assertEquals(0, new FixedRateSchedule(0, 7).spanNanos());
        var s = new FixedRateSchedule(1, 7);
        assertEquals(0, s.dueCountAt(-1)); assertEquals(1, s.dueCountAt(0));
        assertThrows(IllegalArgumentException.class, () -> s.offsetNanos(1));
    }
    @Test void invalidRateOverflowAndExcessiveHorizonAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new FixedRateSchedule(1, 0));
        assertThrows(IllegalArgumentException.class, () -> new FixedRateSchedule(1, 1_000_000_001L));
        assertThrows(IllegalArgumentException.class, () -> new FixedRateSchedule(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> new FixedRateSchedule(Long.MAX_VALUE, 1));
        assertThrows(IllegalArgumentException.class, () -> new FixedRateSchedule(TimeUnit.DAYS.toSeconds(1) + 2, 1));
    }
}

