package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingMultiCrashExperimentTest {
    private final RingMultiCrashExperiment experiment = new RingMultiCrashExperiment();

    @Test
    void ringOneMissesSecondNodeWhenAdjacentObserverCrashesWithIt() {
        RingMultiCrashExperiment.Result result = experiment.runAdjacentCrash(5, 1, 1);

        assertEquals(List.of("node-1", "node-2"), result.crashedTargets());
        assertEquals(1, result.detectedDeadCount());
        assertFalse(result.allDetectedDead());

        RingMultiCrashExperiment.TargetOutcome node1 = outcome(result, "node-1");
        assertEquals(List.of("node-0"), node1.assignedObservers());
        assertEquals(List.of("node-0"), node1.liveObservers());
        assertEquals(Duration.ofSeconds(2).toNanos(), node1.suspectLatencyNanos());
        assertEquals(Duration.ofSeconds(4).toNanos(), node1.deadLatencyNanos());

        RingMultiCrashExperiment.TargetOutcome node2 = outcome(result, "node-2");
        assertEquals(List.of("node-1"), node2.assignedObservers());
        assertTrue(node2.liveObservers().isEmpty());
        assertNull(node2.suspectLatencyNanos());
        assertNull(node2.deadLatencyNanos());
    }

    @Test
    void everyAdjacentPairLeavesOneCrashUnobservedAtFiveAndEightNodes() {
        for (int nodeCount : List.of(5, 8)) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                RingMultiCrashExperiment.Result result = experiment.runAdjacentCrash(
                        nodeCount, 1, firstCrash);

                assertEquals(1, result.detectedDeadCount(),
                        "nodeCount=" + nodeCount + ", firstCrash=" + firstCrash);
                assertFalse(result.allDetectedDead(),
                        "nodeCount=" + nodeCount + ", firstCrash=" + firstCrash);
            }
        }
    }

    @Test
    void ringOneDetectsTwoNonAdjacentCrashesWhenTheirObserversStayAlive() {
        RingMultiCrashExperiment.Result result = experiment.run(
                List.of("node-0", "node-1", "node-2", "node-3", "node-4"),
                1,
                List.of("node-1", "node-3")
        );

        assertEquals(2, result.detectedDeadCount());
        assertTrue(result.allDetectedDead());
    }

    @Test
    void ringTwoDetectsEveryAdjacentPairAtDoubleRingOneModelCost() {
        for (int nodeCount : List.of(5, 8)) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                RingMultiCrashExperiment.Result ringOne = experiment.runAdjacentCrash(
                        nodeCount, 1, firstCrash);
                RingMultiCrashExperiment.Result ringTwo = experiment.runAdjacentCrash(
                        nodeCount, 2, firstCrash);

                assertEquals(2, ringTwo.detectedDeadCount());
                assertTrue(ringTwo.allDetectedDead());
                assertTrue(ringTwo.outcomes().stream()
                        .allMatch(outcome -> !outcome.liveObservers().isEmpty()));
                assertEquals(ringOne.protocolMessages() * 2, ringTwo.protocolMessages());
                assertEquals(ringOne.protocolBytes() * 2, ringTwo.protocolBytes());
            }
        }
    }

    @Test
    void ringTwoMissesLastNodeOfEveryConsecutiveThreeNodeCrash() {
        for (int nodeCount : List.of(5, 8)) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                RingMultiCrashExperiment.Result result = experiment.runConsecutiveCrash(
                        nodeCount, 2, firstCrash, 3);

                assertEquals(2, result.detectedDeadCount(),
                        "nodeCount=" + nodeCount + ", firstCrash=" + firstCrash);
                assertFalse(result.allDetectedDead());
                assertEquals(1, result.outcomes().stream()
                        .filter(outcome -> outcome.liveObservers().isEmpty())
                        .count());
            }
        }
    }

    private static RingMultiCrashExperiment.TargetOutcome outcome(
            RingMultiCrashExperiment.Result result,
            String target
    ) {
        return result.outcomes().stream()
                .filter(outcome -> outcome.target().equals(target))
                .findFirst()
                .orElseThrow();
    }
}
