package io.openkache.core.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.spi.TimeSource;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class DeadlineTest {

    private static final class ManualTime implements TimeSource {
        long nanos;

        ManualTime(long nanos) {
            this.nanos = nanos;
        }

        @Override
        public long monotonicNanos() {
            return nanos;
        }

        @Override
        public long epochMillis() {
            return 0;
        }
    }

    @Test
    void relativeTimeoutBecomesLocalDeadline() {
        ManualTime time = new ManualTime(1_000);
        Deadline deadline = Deadline.after(time, Duration.ofMillis(2));

        assertEquals(Duration.ofMillis(2), deadline.remaining(time));
        assertFalse(deadline.isExpired(time));

        time.nanos += Duration.ofMillis(2).toNanos();
        assertTrue(deadline.isExpired(time));
    }

    @Test
    void worksAcrossNanoTimeOverflow() {
        ManualTime time = new ManualTime(Long.MAX_VALUE - 10);
        Deadline deadline = Deadline.after(time, Duration.ofNanos(100));

        time.nanos += 50; // overflow 직전
        assertFalse(deadline.isExpired(time));

        time.nanos += 60; // overflow 이후 (음수 값)
        assertTrue(deadline.isExpired(time));
    }

    @Test
    void negativeTimeoutIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Deadline.after(TimeSource.system(), Duration.ofMillis(-1)));
    }
}
