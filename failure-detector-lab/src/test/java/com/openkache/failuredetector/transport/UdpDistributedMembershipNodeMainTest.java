package com.openkache.failuredetector.transport;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void membershipFaultPolicyIsDeterministicAndCanReorderTheFirstRemoval() {
        var policy = new UdpDistributedMembershipNodeMain.MembershipFaultPolicy(
                45, 250, 500, 2, 17);

        assertEquals(
                policy.drop(0, 4, 1, 1, 0),
                policy.drop(0, 4, 1, 1, 0));
        assertEquals(750, policy.delayMillis(1, 0));
        assertEquals(255, policy.delayMillis(2, 1));
    }

    @Test
    void zeroLossNeverDropsAndInvalidPoliciesAreRejected() {
        var policy = new UdpDistributedMembershipNodeMain.MembershipFaultPolicy(
                0, 0, 0, 1, 1);

        assertFalse(policy.drop(0, 4, 1, 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new UdpDistributedMembershipNodeMain.MembershipFaultPolicy(
                        101, 0, 0, 1, 1));
    }
}
