package com.openkache.failuredetector.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UdpExperimentNodeMainTest {
    @Test
    void ringHeartbeatFlowsFromMonitoredSuccessorToObserver() {
        assertEquals(0, UdpExperimentNodeMain.ringHeartbeatRecipient(1, 5));
        assertEquals(4, UdpExperimentNodeMain.ringHeartbeatRecipient(0, 5));
    }

    @Test
    void rejectsInvalidRingNode() {
        assertThrows(IllegalArgumentException.class,
                () -> UdpExperimentNodeMain.ringHeartbeatRecipient(5, 5));
    }

    @Test
    void ringTwoSendsHeartbeatToTwoPredecessors() {
        assertEquals(
                java.util.List.of(0, 4),
                UdpExperimentNodeMain.ringHeartbeatRecipients(1, 5, 2));
        assertEquals(
                java.util.List.of(7, 6),
                UdpExperimentNodeMain.ringHeartbeatRecipients(0, 8, 2));
    }

    @Test
    void verificationHelpersAreDistinctAndExcludeObserverAndTarget() {
        assertEquals(
                java.util.List.of(2, 3, 4),
                UdpExperimentNodeMain.verificationHelpers(
                        0, 1, java.util.List.of(4, 1, 3, 2), 3));
        assertThrows(
                IllegalArgumentException.class,
                () -> UdpExperimentNodeMain.verificationHelpers(
                        0, 1, java.util.List.of(0, 1, 2), 2));
    }
}
