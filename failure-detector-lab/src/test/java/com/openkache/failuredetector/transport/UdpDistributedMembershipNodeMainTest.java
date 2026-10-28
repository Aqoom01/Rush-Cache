package com.openkache.failuredetector.transport;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UdpDistributedMembershipNodeMainTest {
    private static final List<String> EIGHT_MEMBERS = List.of(
            "node-0", "node-1", "node-2", "node-3",
            "node-4", "node-5", "node-6", "node-7");

    @Test
    void ringTwoHeartbeatGoesToTheTwoObserversOfTheSender() {
        assertEquals(
                List.of(0, 7),
                UdpDistributedMembershipNodeMain.heartbeatRecipients(
                        1, EIGHT_MEMBERS, 2));
        assertEquals(
                List.of(6, 7),
                UdpDistributedMembershipNodeMain.heartbeatRecipients(
                        0, EIGHT_MEMBERS, 2));
    }

    @Test
    void removalReassignsHiddenThirdCrashToASurvivingObserver() {
        List<String> afterFirstTwoRemovals = List.of(
                "node-0", "node-3", "node-4", "node-5", "node-6", "node-7");

        assertEquals(
                List.of(0, 7),
                UdpDistributedMembershipNodeMain.heartbeatRecipients(
                        3, afterFirstTwoRemovals, 2));
    }

    @Test
    void helperSelectionUsesOnlyCurrentViewAndExcludesObserverAndTarget() {
        List<String> reconfigured = List.of(
                "node-0", "node-3", "node-4", "node-5", "node-6", "node-7");

        assertEquals(
                List.of(4, 5, 6, 7),
                UdpDistributedMembershipNodeMain.verificationHelpers(
                        0, 3, reconfigured));
    }
}
