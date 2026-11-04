package com.openkache.failuredetector.swim;

import com.openkache.failuredetector.experiment.ExperimentResult;
import com.openkache.failuredetector.experiment.ExperimentRunner;
import com.openkache.failuredetector.experiment.ScenarioConfig;
import com.openkache.failuredetector.experiment.ScenarioType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SwimLifeguardAblationTest {
    @Test
    void f7ProfilesToggleOnlyTheirDeclaredCorrection() {
        SwimLifeguardConfig probe = SwimLifeguardConfig.lhaProbeOnly();
        assertTrue(probe.lhaProbeEnabled());
        assertFalse(probe.lhaSuspicionEnabled());
        assertFalse(probe.suspicionConfirmationsEnabled());
        assertFalse(probe.buddySystemEnabled());

        SwimLifeguardConfig suspicion = SwimLifeguardConfig.lhaSuspicionOnly();
        assertFalse(suspicion.lhaProbeEnabled());
        assertTrue(suspicion.lhaSuspicionEnabled());
        assertTrue(suspicion.suspicionConfirmationsEnabled());
        assertFalse(suspicion.buddySystemEnabled());

        SwimLifeguardConfig buddy = SwimLifeguardConfig.buddyOnly();
        assertFalse(buddy.lhaProbeEnabled());
        assertFalse(buddy.lhaSuspicionEnabled());
        assertFalse(buddy.suspicionConfirmationsEnabled());
        assertTrue(buddy.buddySystemEnabled());
    }

    @Test
    void baseProfileDoesNotApplyLocalHealthMultiplier() {
        SwimLifeguardDetector base = new SwimLifeguardDetector(
                "observer", 5, SwimLifeguardConfig.swimBase());

        base.observeLocalDelay(Duration.ofSeconds(10).toNanos());
        base.recordProbe(0, "target", false, 1);

        assertEquals(0, base.awarenessScore());
        assertEquals(1, base.awarenessMultiplier());
    }

    @Test
    void normalTrafficBudgetIsUnchangedByAblationFlags() {
        ExperimentRunner runner = new ExperimentRunner();
        ScenarioConfig scenario = ScenarioConfig.defaults(ScenarioType.NORMAL);

        ExperimentResult base = runner.runSwimBase(scenario, 9);
        ExperimentResult lifeguard = runner.runSwimLifeguard(scenario, 9);

        assertEquals(base.metrics().protocolMessages(), lifeguard.metrics().protocolMessages());
        assertEquals(base.metrics().protocolBytes(), lifeguard.metrics().protocolBytes());
        assertEquals(base.detectorEvents(), lifeguard.detectorEvents());
    }

    @Test
    void everySwimAblationDetectsCrashAtFiveTwentyFiveAndOneHundredNodes() {
        ExperimentRunner runner = new ExperimentRunner();
        List<com.openkache.failuredetector.experiment.DetectorProfile> profiles = List.of(
                com.openkache.failuredetector.experiment.DetectorProfile.F2_SWIM_BASE,
                com.openkache.failuredetector.experiment.DetectorProfile.F3_SWIM_LIFEGUARD,
                com.openkache.failuredetector.experiment.DetectorProfile.F7A_SWIM_LHA_PROBE,
                com.openkache.failuredetector.experiment.DetectorProfile.F7B_SWIM_LHA_SUSPICION,
                com.openkache.failuredetector.experiment.DetectorProfile.F7C_SWIM_BUDDY
        );
        ScenarioConfig defaults = ScenarioConfig.defaults(ScenarioType.CRASH);

        for (int nodeCount : new int[]{5, 25, 100}) {
            ScenarioConfig scenario = new ScenarioConfig(
                    defaults.type(),
                    nodeCount,
                    defaults.duration(),
                    defaults.tick(),
                    defaults.faultStart(),
                    defaults.faultEnd(),
                    defaults.packetLoss(),
                    defaults.fixedLatency(),
                    defaults.jitter(),
                    defaults.gossipFanout()
            );
            for (ExperimentResult result : runner.runProfiles(
                    scenario, 1, profiles, ExperimentRunner.REALISTIC_DEFAULT)) {
                assertEquals(0, result.metrics().falseNegativeEvents(), result.algorithm());
                assertTrue(result.metrics().detectionLatencyNanos() != null, result.algorithm());
            }
        }
    }
}
