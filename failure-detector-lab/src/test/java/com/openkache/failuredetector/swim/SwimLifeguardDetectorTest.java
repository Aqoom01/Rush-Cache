package com.openkache.failuredetector.swim;

import com.openkache.failuredetector.core.NodeStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SwimLifeguardDetectorTest {
    @Test
    void successfulProbesKeepNormalPeerAlive() {
        SwimLifeguardDetector detector = new SwimLifeguardDetector(
                "observer", 100, SwimLifeguardConfig.swimBase());

        detector.recordProbe(1_000, "target", true, 1);
        detector.recordProbe(2_000, "target", true, 1);
        detector.tick(Duration.ofMinutes(10).toNanos());

        assertEquals(NodeStatus.ALIVE, detector.status("target"));
        assertEquals(0, detector.suspicionDeadlineNanos("target"));
    }

    @Test
    void localFailuresIncreaseAwarenessAndSuccessRecoversIt() {
        SwimLifeguardDetector detector = new SwimLifeguardDetector("observer", 5);

        detector.recordProbe(1, "target", false, 1);
        assertEquals(1, detector.awarenessScore());

        detector.recordProbe(2, "target", true, 2);
        assertEquals(0, detector.awarenessScore());
        assertEquals(NodeStatus.ALIVE, detector.status("target"));
    }

    @Test
    void independentConfirmationsAccelerateSuspicion() {
        SwimLifeguardConfig config = new SwimLifeguardConfig(
                Duration.ofSeconds(1),
                Duration.ofMillis(500),
                3,
                1,
                6,
                3,
                8
        );
        SwimLifeguardDetector detector = new SwimLifeguardDetector("observer", 5, config);

        detector.recordProbe(0, "target", false, 1);
        long withoutConfirmations = detector.suspicionTimeoutNanos(0);
        long withConfirmations = detector.suspicionTimeoutNanos(3);
        assertTrue(withConfirmations < withoutConfirmations);

        detector.confirmSuspicion(1, "target", "node-2");
        detector.confirmSuspicion(2, "target", "node-3");
        detector.confirmSuspicion(3, "target", "node-4");
        detector.tick(withConfirmations + 3);

        assertEquals(NodeStatus.DEAD, detector.status("target"));
    }

    @Test
    void localSchedulerDelayScalesProbeTimeout() {
        SwimLifeguardDetector detector = new SwimLifeguardDetector("observer", 5);
        long normal = detector.effectiveProbeTimeoutNanos();

        detector.observeLocalDelay(Duration.ofSeconds(4).toNanos());

        assertTrue(detector.effectiveProbeTimeoutNanos() > normal);
    }

    @Test
    void swimBaseUsesFixedMinimumSuspicionTimeoutAtEveryClusterSize() {
        for (int clusterSize : new int[]{5, 25, 100}) {
            SwimLifeguardDetector detector = new SwimLifeguardDetector(
                    "observer", clusterSize, SwimLifeguardConfig.swimBase());
            long expected = (long) (4.0 * Math.log(clusterSize + 1.0)
                    * Duration.ofSeconds(1).toNanos());

            assertEquals(expected, detector.suspicionTimeoutNanos(0), clusterSize);
            assertEquals(expected, detector.suspicionTimeoutNanos(3), clusterSize);
        }
    }

    @Test
    void duplicateSuspicionDoesNotExtendDeadlineAndExpiresDeterministically() {
        SwimLifeguardDetector detector = new SwimLifeguardDetector(
                "observer", 100, SwimLifeguardConfig.swimBase());

        detector.recordProbe(1_000, "target", false, 1);
        long deadline = detector.suspicionDeadlineNanos("target");
        detector.recordProbe(2_000, "target", false, 1);

        assertEquals(deadline, detector.suspicionDeadlineNanos("target"));
        detector.tick(deadline - 1);
        assertEquals(NodeStatus.SUSPECT, detector.status("target"));
        detector.tick(deadline);
        assertEquals(NodeStatus.DEAD, detector.status("target"));
    }

    @Test
    void newerIncarnationRefutesSuspicionAndPreventsOldDeadlineFromKillingPeer() {
        SwimLifeguardDetector detector = new SwimLifeguardDetector(
                "observer", 25, SwimLifeguardConfig.swimBase());

        detector.recordProbe(1_000, "target", false, 1);
        long oldDeadline = detector.suspicionDeadlineNanos("target");
        detector.refute(2_000, "target", 2);
        detector.tick(oldDeadline);

        assertEquals(NodeStatus.ALIVE, detector.status("target"));
        assertEquals(0, detector.suspicionDeadlineNanos("target"));
    }
}
