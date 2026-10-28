package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingMembershipReconfigurationExperimentTest {
    private static final long SECOND = Duration.ofSeconds(1).toNanos();

    private final RingMembershipReconfigurationExperiment experiment =
            new RingMembershipReconfigurationExperiment();

    @Test
    void sharedViewReconfigurationUsesDeadEvidenceToRevealHiddenThirdCrash() {
        RingMembershipReconfigurationExperiment.Result result =
                experiment.runConsecutiveCrash(5, 2, 0, 3);

        assertTrue(result.allDetectedDead());
        assertEquals(3, result.detectedDeadCount());
        assertEquals(9 * SECOND, result.lastDeadLatencyNanos());
        assertEquals(9 * SECOND, result.membershipCompleteLatencyNanos());
        assertEquals(2, result.membershipEvents().size());

        RingMembershipReconfigurationExperiment.MembershipViewEvent first =
                result.membershipEvents().get(0);
        assertEquals(24 * SECOND, first.atNanos());
        assertEquals(List.of("node-0", "node-1"), first.removedMembers());
        assertFalse(first.removedMembers().contains("node-2"));
        assertEquals("detector-dead-events", first.source());
        assertTrue(first.detectorEvidence().stream()
                .allMatch(evidence -> evidence.endsWith("node-0")
                        || evidence.endsWith("node-1")));

        RingMembershipReconfigurationExperiment.MembershipViewEvent second =
                result.membershipEvents().get(1);
        assertEquals(29 * SECOND, second.atNanos());
        assertEquals(List.of("node-2"), second.removedMembers());
        assertTrue(second.detectorEvidence().stream()
                .allMatch(evidence -> evidence.endsWith("node-2")));

        RingMembershipReconfigurationExperiment.TargetOutcome hidden = result.outcomes().stream()
                .filter(outcome -> outcome.target().equals("node-2"))
                .findFirst()
                .orElseThrow();
        assertEquals(4 * SECOND, hidden.firstLiveObserverLatencyNanos());
        assertEquals(7 * SECOND, hidden.suspectLatencyNanos());
        assertEquals(9 * SECOND, hidden.deadLatencyNanos());
    }

    @Test
    void everyFiveAndEightNodeRotationEventuallyDetectsAllThreeCrashes() {
        for (int nodeCount : List.of(5, 8)) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                RingMembershipReconfigurationExperiment.Result result =
                        experiment.runConsecutiveCrash(nodeCount, 2, firstCrash, 3);

                assertTrue(result.allDetectedDead(),
                        "nodeCount=" + nodeCount + ", firstCrash=" + firstCrash);
                assertEquals(9 * SECOND, result.lastDeadLatencyNanos());
                assertEquals(9 * SECOND, result.membershipCompleteLatencyNanos());
                assertEquals(0, result.membershipControlMessages());
            }
        }
    }

    @Test
    void idealRingTwoReconfigurationUsesFewerHeartbeatMessagesThanStaticRingThree() {
        RingMultiCrashExperiment staticExperiment = new RingMultiCrashExperiment();
        for (int nodeCount : List.of(5, 8)) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                RingMembershipReconfigurationExperiment.Result reconfigured =
                        experiment.runConsecutiveCrash(nodeCount, 2, firstCrash, 3);
                RingMultiCrashExperiment.Result ringThree =
                        staticExperiment.runConsecutiveCrash(nodeCount, 3, firstCrash, 3);

                assertTrue(ringThree.allDetectedDead());
                assertTrue(reconfigured.heartbeatMessages() < ringThree.protocolMessages());
            }
        }
    }
}
