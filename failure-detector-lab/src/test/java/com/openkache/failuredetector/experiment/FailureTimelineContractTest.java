package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.NodeStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureTimelineContractTest {
    @Test
    void crashAfterProbeWaitsForDirectAndIndirectTimeoutsBeforeNewSuspicion() {
        ExperimentResult result = new ExperimentRunner().runSwimBase(
                ScenarioConfig.defaults(ScenarioType.CRASH),
                2
        );
        DetectorEvent suspect = result.detectorEvents().stream()
                .filter(event -> event.newStatus() == NodeStatus.SUSPECT)
                .findFirst()
                .orElseThrow();
        SimulationEvent indirectTimeout = result.simulationEvents().stream()
                .filter(event -> event.type() == SimulationEvent.Type.INDIRECT_TIMEOUT)
                .filter(event -> event.atNanos() == suspect.atNanos())
                .findFirst()
                .orElseThrow();
        SimulationEvent directTimeout = result.simulationEvents().stream()
                .filter(event -> event.type() == SimulationEvent.Type.DIRECT_TIMEOUT)
                .filter(event -> event.atNanos() < indirectTimeout.atNanos())
                .reduce((left, right) -> right)
                .orElseThrow();
        SimulationEvent probe = result.simulationEvents().stream()
                .filter(event -> event.type() == SimulationEvent.Type.PROBE_SENT)
                .filter(event -> event.atNanos() < directTimeout.atNanos())
                .reduce((left, right) -> right)
                .orElseThrow();

        assertEquals(Duration.ofSeconds(20).plusMillis(137).toNanos(), result.faultStartNanos());
        assertEquals(Duration.ofMillis(500).toNanos(), directTimeout.atNanos() - probe.atNanos());
        assertEquals(Duration.ofMillis(500).toNanos(), suspect.atNanos() - directTimeout.atNanos());
        assertNotNull(result.metrics().suspicionLatencyNanos());
        assertTrue(result.metrics().suspicionLatencyNanos() > 0);
    }

    @Test
    void partitionIsReportedAsAliveButUnreachable() {
        ExperimentResult result = new ExperimentRunner().runSwimBase(
                ScenarioConfig.defaults(ScenarioType.PARTITION),
                1
        );

        assertTrue(result.groundTruthEvents().stream()
                .anyMatch(event -> event.alive() && !event.reachable()));
        assertTrue(result.metrics().unreachableButAliveNanos() > 0);
    }
}
