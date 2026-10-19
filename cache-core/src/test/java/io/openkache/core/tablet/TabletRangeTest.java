package io.openkache.core.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class TabletRangeTest {

    private static final long HALF = Long.MIN_VALUE; // 2^63 (부호 없는 해석)
    private static final long LAST = -1L; // 2^64 - 1

    @Test
    void fullRangeContainsEveryPoint() {
        assertTrue(TabletRange.FULL.contains(0));
        assertTrue(TabletRange.FULL.contains(HALF));
        assertTrue(TabletRange.FULL.contains(LAST));
    }

    @Test
    void endIsExclusiveAndStartIsInclusive() {
        TabletRange r = new TabletRange(10, 20);

        assertTrue(r.contains(10));
        assertTrue(r.contains(19));
        assertFalse(r.contains(20));
        assertFalse(r.contains(9));
    }

    @Test
    void comparesUnsigned() {
        TabletRange upper = new TabletRange(HALF, 0);

        assertTrue(upper.contains(LAST));
        assertFalse(upper.contains(HALF - 1)); // 2^63 - 1
    }

    @Test
    void rejectsEmptyOrReversedRange() {
        assertThrows(IllegalArgumentException.class, () -> new TabletRange(5, 5));
        assertThrows(IllegalArgumentException.class, () -> new TabletRange(20, 10));
    }

    @Test
    void splitProducesTwoAdjacentPieces() {
        TabletRange.Split s = new TabletRange(10, 20).splitAt(15);

        assertEquals(new TabletRange(10, 15), s.left());
        assertEquals(new TabletRange(15, 20), s.right());
        assertEquals(new TabletRange(10, 20), s.left().mergeWith(s.right()));
    }

    @Test
    void splitAtStartOrOutsideIsRejected() {
        TabletRange r = new TabletRange(10, 20);

        assertThrows(IllegalArgumentException.class, () -> r.splitAt(10));
        assertThrows(IllegalArgumentException.class, () -> r.splitAt(20));
        assertThrows(IllegalArgumentException.class, () -> r.splitAt(5));
    }

    @Test
    void mergeRequiresAdjacency() {
        TabletRange a = new TabletRange(0, 10);

        assertThrows(IllegalArgumentException.class, () -> a.mergeWith(new TabletRange(11, 20)));
        assertThrows(IllegalArgumentException.class, () -> TabletRange.FULL.mergeWith(a));
    }

    @Test
    void uniformCoversWholeSpaceWithoutGaps() {
        for (int n : new int[] {1, 2, 3, 7, 4096}) {
            List<TabletRange> ranges = TabletRange.uniform(n);

            assertEquals(n, ranges.size());
            assertEquals(0, ranges.get(0).start());
            assertEquals(0, ranges.get(n - 1).end());
            for (int i = 0; i + 1 < n; i++) {
                assertEquals(ranges.get(i).end(), ranges.get(i + 1).start(), "n=" + n + " i=" + i);
            }
        }
    }

    @Test
    void uniformPiecesAreNearlyEqual() {
        List<TabletRange> ranges = TabletRange.uniform(3);
        long step = Long.divideUnsigned(-1L, 3);

        assertEquals(new TabletRange(0, step), ranges.get(0));
        assertEquals(new TabletRange(step, 2 * step), ranges.get(1));
        assertEquals(new TabletRange(2 * step, 0), ranges.get(2));
    }
}
