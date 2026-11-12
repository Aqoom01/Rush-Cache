package com.openkache.failuredetector.adaptive;

import com.openkache.failuredetector.core.NodeStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptiveDeadlineFailureDetectorTest {
    @Test
    void rejectsNonMonotonicObservationWithoutMovingLastSeen() {
        AdaptiveDeadlineFailureDetector detector = detector();
        detector.registerTarget(0, "target", 1);
        assertTrue(detector.recordHeartbeat(ms(100), "target", 1));
        assertFalse(detector.recordHeartbeat(ms(100), "target", 1));
        assertFalse(detector.recordHeartbeat(ms(90), "target", 1));
    }

    @Test
    void rejectsNaNAndInfiniteParameters() {
        assertThrows(IllegalArgumentException.class, () -> config(Double.NaN, 2));
        assertThrows(IllegalArgumentException.class, () -> config(0.5, Double.POSITIVE_INFINITY));
    }

    @Test
    void deadlineExpandsWithObservedVariance() {
        AdaptiveDeadlineFailureDetector detector = detector();
        detector.registerTarget(0, "target", 1);
        detector.recordHeartbeat(ms(100), "target", 1);
        detector.recordHeartbeat(ms(200), "target", 1);
        detector.recordHeartbeat(ms(300), "target", 1);
        long stableDeadline = detector.deadlineNanos("target");

        detector.recordHeartbeat(ms(500), "target", 1);
        detector.recordHeartbeat(ms(800), "target", 1);

        assertTrue(detector.deadlineNanos("target") > stableDeadline);
    }

    @Test
    void missingHeartbeatTransitionsThroughSuspectAndDead() {
        AdaptiveDeadlineFailureDetector detector = detector();
        detector.registerTarget(0, "target", 1);
        detector.recordHeartbeat(ms(100), "target", 1);
        detector.recordHeartbeat(ms(200), "target", 1);
        detector.recordHeartbeat(ms(300), "target", 1);

        long suspectAt = ms(300) + detector.deadlineNanos("target");
        detector.tick(suspectAt);
        assertEquals(NodeStatus.SUSPECT, detector.status("target"));

        detector.tick(suspectAt + ms(50));
        assertEquals(NodeStatus.DEAD, detector.status("target"));

        detector.recordHeartbeat(suspectAt + ms(60), "target", 2);
        assertEquals(NodeStatus.ALIVE, detector.status("target"));
    }

    private static AdaptiveDeadlineFailureDetector detector() {
        return new AdaptiveDeadlineFailureDetector("observer", config(0.5, 2));
    }

    private static AdaptiveDeadlineConfig config(double alpha, double sigma) {
        return new AdaptiveDeadlineConfig(
                        Duration.ofMillis(100),
                        2,
                        alpha,
                        Duration.ofMillis(100),
                        Duration.ofMillis(10),
                        sigma,
                        Duration.ZERO,
                        Duration.ofMillis(100),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        Duration.ofMillis(50),
                        Duration.ZERO,
                        64
        );
    }

    private static long ms(long value) {
        return Duration.ofMillis(value).toNanos();
    }
}
