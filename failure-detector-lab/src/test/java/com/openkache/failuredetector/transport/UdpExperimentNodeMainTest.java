package com.openkache.failuredetector.transport;

import com.openkache.failuredetector.experiment.DetectorProfile;
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

    @Test
    void confirmationProbeUsesADistinctDeterministicIndirectHelper() {
        assertEquals(
                2,
                UdpExperimentNodeMain.indirectHelper(
                        0, 1, java.util.List.of(4, 1, 3, 2)));
        assertEquals(
                -1,
                UdpExperimentNodeMain.indirectHelper(
                        0, 1, java.util.List.of(0, 1)));
    }

    @Test
    void aliveRefutationAdvancesOnlyCurrentOrNewerSuspicionIncarnation() {
        assertEquals(2, UdpExperimentNodeMain.refutationIncarnation(1, 1));
        assertEquals(5, UdpExperimentNodeMain.refutationIncarnation(2, 4));
        assertEquals(5, UdpExperimentNodeMain.refutationIncarnation(5, 2));
        assertThrows(IllegalArgumentException.class,
                () -> UdpExperimentNodeMain.refutationIncarnation(0, 1));
    }

    @Test
    void swimTrafficWaitsForPeersWhileRingKeepsItsDetectorWarmupContract() {
        assertEquals(
                3_000,
                UdpExperimentNodeMain.protocolInitialDelayMillis(
                        DetectorProfile.F3_SWIM_LIFEGUARD));
        assertEquals(
                0,
                UdpExperimentNodeMain.protocolInitialDelayMillis(
                        DetectorProfile.F6_FIXED_DEADLINE_RING_VERIFY));
    }

    @Test
    void fiveNodeLifeguardKeepsOneIndirectProbeAndThreeConfirmations() {
        var config = UdpExperimentNodeMain.swimConfig(true, 5);

        assertEquals(1, config.indirectChecks());
        assertEquals(3, config.maxConfirmations());
        assertEquals(1, UdpExperimentNodeMain.boundedConfirmationCount(3, 3));
    }
}
