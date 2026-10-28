package com.openkache.failuredetector.deadline;

import com.openkache.failuredetector.core.NodeStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedDeadlineFailureDetectorTest {
    @Test
    void verifySuspectRequiresIndependentAttemptsBeforeDead() {
        FixedDeadlineFailureDetector detector = new FixedDeadlineFailureDetector(
                "observer", FixedDeadlineConfig.ringVerified());
        detector.registerTarget(0, "target", 1);
        detector.tick(FixedDeadlineConfig.ringVerified().deadline().toNanos());
        assertEquals(NodeStatus.SUSPECT, detector.status("target"));

        assertTrue(detector.beginVerification("target"));
        detector.recordVerification(Duration.ofSeconds(4).toNanos(), "target", false);
        assertEquals(NodeStatus.SUSPECT, detector.status("target"));

        assertTrue(detector.beginVerification("target"));
        detector.recordVerification(Duration.ofSeconds(5).toNanos(), "target", false);
        assertEquals(NodeStatus.DEAD, detector.status("target"));
    }

    @Test
    void successfulVerifySuspectRefutesButDoesNotCountAsIndependentDeadEvidence() {
        FixedDeadlineFailureDetector detector = new FixedDeadlineFailureDetector(
                "observer", FixedDeadlineConfig.ringVerified());
        detector.registerTarget(0, "target", 1);
        detector.tick(FixedDeadlineConfig.ringVerified().deadline().toNanos());

        assertTrue(detector.beginVerification("target"));
        detector.recordVerification(Duration.ofSeconds(4).toNanos(), "target", true);

        assertEquals(NodeStatus.ALIVE, detector.status("target"));
    }

    private static final long SECOND = Duration.ofSeconds(1).toNanos();

    @Test
    void normalHeartbeatsDoNotCreateFalseFailure() {
        FixedDeadlineFailureDetector detector = detector();
        detector.registerTarget(0, "target", 1);

        for (int second = 1; second <= 10; second++) {
            detector.recordHeartbeat(second * SECOND, "target", 1);
            detector.tick(second * SECOND);
        }

        assertEquals(NodeStatus.ALIVE, detector.status("target"));
        assertTrue(detector.drainEvents().isEmpty());
    }

    @Test
    void crashTransitionsAfterDeadlineThenConfirmation() {
        FixedDeadlineFailureDetector detector = detector();
        detector.registerTarget(0, "target", 1);
        detector.recordHeartbeat(SECOND, "target", 1);

        detector.tick(4 * SECOND);
        assertEquals(NodeStatus.SUSPECT, detector.status("target"));
        detector.tick(6 * SECOND);
        assertEquals(NodeStatus.DEAD, detector.status("target"));
    }

    @Test
    void delayedHeartbeatRecoversSuspectButDeadRequiresNewIncarnation() {
        FixedDeadlineFailureDetector detector = detector();
        detector.registerTarget(0, "target", 1);
        detector.tick(3 * SECOND);
        assertEquals(NodeStatus.SUSPECT, detector.status("target"));

        assertTrue(detector.recordHeartbeat(4 * SECOND, "target", 1));
        assertEquals(NodeStatus.ALIVE, detector.status("target"));
        detector.tick(7 * SECOND);
        detector.tick(9 * SECOND);
        assertEquals(NodeStatus.DEAD, detector.status("target"));

        assertFalse(detector.recordHeartbeat(10 * SECOND, "target", 1));
        assertTrue(detector.recordHeartbeat(10 * SECOND, "target", 2));
        assertEquals(NodeStatus.ALIVE, detector.status("target"));
    }

    private static FixedDeadlineFailureDetector detector() {
        return new FixedDeadlineFailureDetector(
                "observer",
                new FixedDeadlineConfig(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(3),
                        Duration.ofSeconds(2),
                        Duration.ZERO,
                        FixedDeadlineConfig.PeerSelectionMode.FULL_MESH,
                        64
                )
        );
    }
}
