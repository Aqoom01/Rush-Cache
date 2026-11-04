package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingPartitionMembershipSafetyExperimentTest {
    private static final long SECOND = Duration.ofSeconds(1).toNanos();

    @Test
    void partitionFalseDeadSpreadsRemovalToEveryNodeWithoutVerification() {
        RingPartitionMembershipSafetyExperiment.Result result =
                new RingPartitionMembershipSafetyExperiment(false).run(5, 1);

        assertEquals(2, result.falseDeadEvents());
        assertEquals(5, result.removedViewCount());
        assertEquals(5 * SECOND, result.falseRemovalConvergenceLatencyNanos());
        assertTrue(result.membershipControlMessages() > 0);
    }

    @Test
    void verifySuspectPreventsFalseRemovalForEveryTargetRotation() {
        for (int nodeCount : List.of(5, 8)) {
            for (int target = 0; target < nodeCount; target++) {
                RingPartitionMembershipSafetyExperiment.Result result =
                        new RingPartitionMembershipSafetyExperiment(true)
                                .run(nodeCount, target);

                assertEquals(0, result.falseDeadEvents());
                assertEquals(0, result.removedViewCount());
                assertEquals(0, result.membershipControlMessages());
                assertTrue(result.verificationMessages() > 0);
            }
        }
    }

    @Test
    void verifyCannotPreventRemovalWhenHelperIsAlsoPartitioned() {
        RingPartitionMembershipSafetyExperiment.Result result =
                new RingPartitionMembershipSafetyExperiment(true, false, 0.0, 7)
                        .run(5, 1);

        assertTrue(result.falseDeadEvents() > 0);
        assertEquals(5, result.removedViewCount());
        assertEquals(0, result.verificationSuccesses());
        assertTrue(result.verificationFailures() > 0);
    }

    @Test
    void seededHelperLossIsReproducible() {
        RingPartitionMembershipSafetyExperiment first =
                new RingPartitionMembershipSafetyExperiment(true, true, 0.45, 19);
        RingPartitionMembershipSafetyExperiment second =
                new RingPartitionMembershipSafetyExperiment(true, true, 0.45, 19);

        RingPartitionMembershipSafetyExperiment.Result a = first.run(8, 3);
        RingPartitionMembershipSafetyExperiment.Result b = second.run(8, 3);

        assertEquals(a.falseDeadEvents(), b.falseDeadEvents());
        assertEquals(a.removedViewCount(), b.removedViewCount());
        assertEquals(a.verificationSuccesses(), b.verificationSuccesses());
        assertEquals(a.verificationFailures(), b.verificationFailures());
        assertEquals(a.helperProbeSuccesses(), b.helperProbeSuccesses());
        assertEquals(a.helperProbeFailures(), b.helperProbeFailures());
    }

    @Test
    void dualHelperAllFailureQuorumSurvivesOneBlockedHelper() {
        RingPartitionMembershipSafetyExperiment.VerificationPolicy policy =
                new RingPartitionMembershipSafetyExperiment.VerificationPolicy(
                        2, 1, 0.0, 2, 2);

        RingPartitionMembershipSafetyExperiment.Result result =
                new RingPartitionMembershipSafetyExperiment(true, policy, 11)
                        .run(5, 1);

        assertEquals(0, result.falseDeadEvents());
        assertEquals(0, result.removedViewCount());
        assertTrue(result.helperProbeSuccesses() > 0);
        assertTrue(result.helperProbeFailures() > 0);
    }

    @Test
    void dualHelperCannotSurviveWhenEveryHelperIsBlocked() {
        RingPartitionMembershipSafetyExperiment.VerificationPolicy policy =
                new RingPartitionMembershipSafetyExperiment.VerificationPolicy(
                        2, 2, 0.0, 2, 2);

        RingPartitionMembershipSafetyExperiment.Result result =
                new RingPartitionMembershipSafetyExperiment(true, policy, 11)
                        .run(5, 1);

        assertTrue(result.falseDeadEvents() > 0);
        assertEquals(5, result.removedViewCount());
        assertEquals(0, result.helperProbeSuccesses());
    }

    @Test
    void allFailureQuorumUsesSuccessfulHelperAsRefutation() {
        RingPartitionMembershipSafetyExperiment.VerificationPolicy majorityFailure =
                new RingPartitionMembershipSafetyExperiment.VerificationPolicy(
                        3, 2, 0.0, 2, 2);
        RingPartitionMembershipSafetyExperiment.VerificationPolicy allFailure =
                new RingPartitionMembershipSafetyExperiment.VerificationPolicy(
                        3, 2, 0.0, 3, 2);

        RingPartitionMembershipSafetyExperiment.Result majority =
                new RingPartitionMembershipSafetyExperiment(true, majorityFailure, 11)
                        .run(5, 1);
        RingPartitionMembershipSafetyExperiment.Result all =
                new RingPartitionMembershipSafetyExperiment(true, allFailure, 11)
                        .run(5, 1);

        assertEquals(5, majority.removedViewCount());
        assertEquals(0, all.removedViewCount());
    }
}
