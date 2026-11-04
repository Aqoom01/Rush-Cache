package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScenarioReproducibilityTest {
    @Test
    void allProfilesReuseTheSameScenarioSeedAndFaultOffset() {
        ExperimentRunner runner = new ExperimentRunner();
        ScenarioConfig scenario = ScenarioConfig.defaults(ScenarioType.CRASH);

        List<ExperimentResult> first = runner.runAllProfiles(scenario, 7);
        List<ExperimentResult> second = runner.runAllProfiles(scenario, 7);

        assertEquals(DetectorProfile.values().length, first.size());
        assertEquals(first, second);
        assertEquals(
                DetectorProfile.values().length,
                first.stream().map(ExperimentResult::algorithm).distinct().count()
        );
        assertEquals(1, first.stream().map(ExperimentResult::faultStartNanos).distinct().count());
        assertTrue(first.stream().allMatch(result -> result.seed() == 7));
        assertTrue(first.stream().allMatch(result -> result.protocolCostSource().equals("model-estimated")));
    }

    @Test
    void fixedFullMeshCostGrowsFasterThanNodeCount() {
        ScenarioConfig five = ScenarioConfig.defaults(ScenarioType.NORMAL);
        ScenarioConfig twentyFive = new ScenarioConfig(
                five.type(), 25, five.duration(), five.tick(), five.faultStart(), five.faultEnd(),
                five.packetLoss(), five.fixedLatency(), five.jitter(), five.gossipFanout());
        ExperimentRunner runner = new ExperimentRunner();

        long small = runner.runFixedDeadline(five, 1).metrics().protocolMessages();
        long large = runner.runFixedDeadline(twentyFive, 1).metrics().protocolMessages();

        assertTrue(large > small * 5);
    }

    @Test
    void profilesSharingObservationModuleReuseProtocolRandomStream() {
        ScenarioConfig scenario = ScenarioConfig.defaults(ScenarioType.PACKET_LOSS);
        List<ExperimentResult> results = new ExperimentRunner().runProfiles(
                scenario,
                17,
                List.of(
                        DetectorProfile.F0_FIXED_DEADLINE,
                        DetectorProfile.F4_ADAPTIVE_DEADLINE
                ),
                ExperimentRunner.REALISTIC_DEFAULT
        );

        List<String> fixedPackets = heartbeatPacketTrace(results.get(0));
        List<String> adaptivePackets = heartbeatPacketTrace(results.get(1));
        assertEquals(fixedPackets, adaptivePackets);
    }

    private static List<String> heartbeatPacketTrace(ExperimentResult result) {
        return result.simulationEvents().stream()
                .filter(event -> event.type() == SimulationEvent.Type.PACKET_DROPPED
                        || event.type() == SimulationEvent.Type.PACKET_DELIVERED)
                .map(event -> event.atNanos() + ":" + event.type())
                .toList();
    }
}
