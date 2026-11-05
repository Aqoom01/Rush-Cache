package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class ScheduleTest {

    private static final List<String> TWO_TABLETS = List.of(
            "# 두 Tablet: [0, 2^63) → 0, [2^63, 2^64) → 1",
            "tablets 2",
            "tablet 0 0",
            "tablet 9223372036854775808 1",
            "window 0",
            "window 1",
            "move 0 1",
            "window 2");

    @Test
    void routesByTabletAndCarriesMovesForward() {
        Schedule s = Schedule.parse(TWO_TABLETS);

        assertEquals(3, s.windows());
        assertEquals(0, s.target(0, 5));
        assertEquals(1, s.target(0, -1L));
        assertEquals(1, s.target(1, 5), "window 1부터 Tablet 0은 서버 1");
        assertEquals(1, s.target(2, 5), "다음 window에도 유지");
        assertEquals(0, s.movesAt(0));
        assertEquals(1, s.movesAt(1));
        assertEquals(0, s.movesAt(2));
        assertEquals(1, s.maxTarget());
    }

    @Test
    void tabletLookupUsesUnsignedOrder() {
        Schedule s = Schedule.parse(TWO_TABLETS);
        assertEquals(0, s.tabletOf(0x7fffffffffffffffL));
        assertEquals(1, s.tabletOf(0x8000000000000000L));
    }

    @Test
    void rejectsMalformedSchedules() {
        assertThrows(ConfigException.class, () -> Schedule.parse(List.of("tablets 1", "tablet 5 0", "window 0")), "첫 시작점은 0");
        assertThrows(ConfigException.class, () -> Schedule.parse(List.of("tablets 1", "tablet 0 0", "window 1")), "window 번호는 0부터");
        assertThrows(ConfigException.class, () -> Schedule.parse(List.of("tablets 1", "tablet 0 0")), "window가 하나는 있어야 한다");
        assertThrows(ConfigException.class, () -> Schedule.parse(List.of("tablets 1", "tablet 0 0", "window 0", "move 3 1")));
        assertThrows(ConfigException.class, () -> Schedule.parse(List.of("bogus")));
    }
}
