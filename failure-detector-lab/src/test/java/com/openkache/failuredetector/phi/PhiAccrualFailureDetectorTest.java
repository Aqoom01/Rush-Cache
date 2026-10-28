package com.openkache.failuredetector.phi;

import com.openkache.failuredetector.core.NodeStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PhiAccrualFailureDetectorTest {
    @Test
    void missingHeartbeatsEventuallyConvictPeer() {
        PhiAccrualConfig config = new PhiAccrualConfig(
                Duration.ofSeconds(1),
                3,
                100,
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                Duration.ZERO
        );
        PhiAccrualFailureDetector detector = new PhiAccrualFailureDetector("observer", config);
        for (int version = 1; version <= 10; version++) {
            long at = Duration.ofSeconds(version).toNanos();
            detector.recordHeartbeat(new GossipHeartbeat("target", 1, version, at), true);
            detector.tick(at);
        }

        long failureCheck = Duration.ofSeconds(18).toNanos();
        detector.tick(failureCheck);

        assertEquals(NodeStatus.DEAD, detector.status("target"));
        assertTrue(detector.phi("target", failureCheck) > config.convictThreshold());
    }

    @Test
    void staleGossipVersionIsIgnored() {
        PhiAccrualFailureDetector detector = new PhiAccrualFailureDetector("observer");
        assertTrue(detector.recordHeartbeat(
                new GossipHeartbeat("target", 2, 10, 1_000_000_000L), true));
        assertFalse(detector.recordHeartbeat(
                new GossipHeartbeat("target", 1, 99, 2_000_000_000L), true));
        assertFalse(detector.recordHeartbeat(
                new GossipHeartbeat("target", 2, 9, 2_000_000_000L), true));
    }

    @Test
    void coldStartAndZeroVarianceNeverProduceNanOrInfinity() {
        PhiAccrualFailureDetector detector = new PhiAccrualFailureDetector("observer");

        assertEquals(0.0, detector.phi("target", Duration.ofSeconds(100).toNanos()));
        detector.recordHeartbeat(
                new GossipHeartbeat("target", 1, 1, Duration.ofSeconds(1).toNanos()),
                true
        );
        double phi = detector.phi("target", Duration.ofSeconds(2).toNanos());

        assertTrue(Double.isFinite(phi));
        assertTrue(phi >= 0);
    }
}
