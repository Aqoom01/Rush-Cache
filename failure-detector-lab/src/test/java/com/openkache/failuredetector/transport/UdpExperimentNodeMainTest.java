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
}
