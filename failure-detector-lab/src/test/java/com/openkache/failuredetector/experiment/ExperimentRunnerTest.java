package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExperimentRunnerTest {
    @Test
    void modularComparisonRunsAllDeclaredProfiles() {
        List<ExperimentResult> results = new ExperimentRunner().runAllProfiles(
                ScenarioConfig.defaults(ScenarioType.CRASH),
                42
        );

        assertEquals(DetectorProfile.values().length, results.size());
        assertEquals(
                DetectorProfile.values().length,
                results.stream().map(ExperimentResult::algorithm).distinct().count()
        );
        for (ExperimentResult result : results) {
            assertNotNull(result.metrics().detectionLatencyNanos(), result.algorithm());
        }
    }

    @Test
    void bothAlgorithmsDetectCrashWithSameScenarioAndSeed() {
        List<ExperimentResult> results = new ExperimentRunner().runBoth(
                ScenarioConfig.defaults(ScenarioType.CRASH),
                42
        );

        assertEquals(2, results.size());
        for (ExperimentResult result : results) {
            assertEquals(0, result.metrics().falseNegativeEvents(), result.algorithm());
            assertNotNull(result.metrics().detectionLatencyNanos(), result.algorithm());
        }
    }

    @Test
    void normalScenarioHasNoFalseFailure() {
        List<ExperimentResult> results = new ExperimentRunner().runBoth(
                ScenarioConfig.defaults(ScenarioType.NORMAL),
                42
        );

        for (ExperimentResult result : results) {
            assertEquals(0, result.metrics().falsePositiveEvents(), result.algorithm());
        }
    }

    @Test
    void detailedLatencyProfilesAreDeterministicForTheSameSeed() {
        ScenarioConfig scenario = ScenarioConfig.defaults(ScenarioType.LATENCY_GRADUAL);

        List<ExperimentResult> first = new ExperimentRunner().runBoth(scenario, 73);
        List<ExperimentResult> second = new ExperimentRunner().runBoth(scenario, 73);

        assertEquals(first, second);
    }

    @Test
    void everyDetailedProfileRunsForBothAlgorithms() {
        List<ScenarioType> detailed = List.of(
                ScenarioType.CRASH_UNDER_JITTER,
                ScenarioType.PACKET_LOSS_BURST,
                ScenarioType.LATENCY_GRADUAL,
                ScenarioType.LATENCY_BURSTY,
                ScenarioType.LATENCY_REGIME
        );

        for (ScenarioType type : detailed) {
            List<ExperimentResult> results = new ExperimentRunner().runBoth(
                    ScenarioConfig.defaults(type),
                    19
            );
            assertEquals(2, results.size(), type.id());
        }
    }

    @Test
    void protocolCostUsesTheWholeClusterAndScalesWithNodeCount() {
        ScenarioConfig fiveNodes = ScenarioConfig.defaults(ScenarioType.NORMAL);
        ScenarioConfig twentyFiveNodes = new ScenarioConfig(
                fiveNodes.type(),
                25,
                fiveNodes.duration(),
                fiveNodes.tick(),
                fiveNodes.faultStart(),
                fiveNodes.faultEnd(),
                fiveNodes.packetLoss(),
                fiveNodes.fixedLatency(),
                fiveNodes.jitter(),
                fiveNodes.gossipFanout()
        );

        List<ExperimentResult> small = new ExperimentRunner().runBoth(fiveNodes, 11);
        List<ExperimentResult> large = new ExperimentRunner().runBoth(twentyFiveNodes, 11);

        for (int index = 0; index < small.size(); index++) {
            assertTrue(
                    large.get(index).metrics().protocolMessages()
                            > small.get(index).metrics().protocolMessages(),
                    small.get(index).algorithm()
            );
        }
    }

    @Test
    void crashWindowIsLongEnoughToMeasureDetectionAtOneHundredNodes() {
        ScenarioConfig defaults = ScenarioConfig.defaults(ScenarioType.CRASH);
        ScenarioConfig scenario = new ScenarioConfig(
                defaults.type(),
                100,
                defaults.duration(),
                defaults.tick(),
                defaults.faultStart(),
                defaults.faultEnd(),
                defaults.packetLoss(),
                defaults.fixedLatency(),
                defaults.jitter(),
                defaults.gossipFanout()
        );

        for (ExperimentResult result : new ExperimentRunner().runBoth(scenario, 3)) {
            assertEquals(0, result.metrics().falseNegativeEvents(), result.algorithm());
            assertNotNull(result.metrics().detectionLatencyNanos(), result.algorithm());
        }
    }

    @Test
    void swimTemporarySuspicionIsNotCountedAsFalseDead() {
        ScenarioConfig defaults = ScenarioConfig.defaults(ScenarioType.PARTITION);
        ScenarioConfig scenario = new ScenarioConfig(
                defaults.type(),
                100,
                defaults.duration(),
                defaults.tick(),
                defaults.faultStart(),
                defaults.faultEnd(),
                defaults.packetLoss(),
                defaults.fixedLatency(),
                defaults.jitter(),
                defaults.gossipFanout()
        );

        ExperimentResult swim = new ExperimentRunner().runSwimLifeguard(scenario, 5);

        assertEquals(1, swim.metrics().falsePositiveEvents());
        assertEquals(0, swim.metrics().falseDeadEvents());
        assertTrue(swim.metrics().falseSuspectNanos() > 0);
    }
}
