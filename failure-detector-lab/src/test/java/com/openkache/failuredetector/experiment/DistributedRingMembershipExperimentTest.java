package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistributedRingMembershipExperimentTest {
    private static final long SECOND = Duration.ofSeconds(1).toNanos();

    private final DistributedRingMembershipExperiment experiment =
            new DistributedRingMembershipExperiment();

    @Test
    void delayedPerNodeViewsRecoverHiddenCrashAndConverge() {
        DistributedRingMembershipExperiment.Result result =
                experiment.runConsecutiveCrash(5, 2, 0, 3);

        assertTrue(result.allDetectedDead());
        assertEquals(9 * SECOND, result.lastDeadLatencyNanos());
        assertEquals(10 * SECOND, result.viewConvergenceLatencyNanos());
        assertTrue(result.membershipControlMessages() > 0);
        assertTrue(result.staleOrDuplicateDeliveries() > 0);
        assertTrue(result.viewEvents().stream()
                .allMatch(event -> event.source().equals("local-detector-dead")
                        || event.source().equals("remote-dead-event")));

        DistributedRingMembershipExperiment.TargetOutcome hidden = result.outcomes().stream()
                .filter(outcome -> outcome.target().equals("node-2"))
                .findFirst()
                .orElseThrow();
        assertEquals(4 * SECOND, hidden.firstLiveObserverLatencyNanos());
        assertEquals(9 * SECOND, hidden.deadLatencyNanos());
    }

    @Test
    void everyRotationDetectsAllCrashesWithDelayedViews() {
        for (int nodeCount : List.of(5, 8)) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                DistributedRingMembershipExperiment.Result result =
                        experiment.runConsecutiveCrash(nodeCount, 2, firstCrash, 3);

                assertTrue(result.allDetectedDead(),
                        "nodeCount=" + nodeCount + ", firstCrash=" + firstCrash);
                assertEquals(9 * SECOND, result.lastDeadLatencyNanos());
                assertTrue(result.viewConvergenceLatencyNanos()
                        >= result.lastDeadLatencyNanos());
            }
        }
    }
}
