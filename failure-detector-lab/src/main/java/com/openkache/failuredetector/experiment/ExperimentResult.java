package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.GroundTruthEvent;

import java.util.List;

public record ExperimentResult(
        String runId,
        String algorithm,
        ScenarioType scenario,
        long seed,
        int nodeCount,
        long durationNanos,
        long faultStartNanos,
        String target,
        List<DetectorEvent> detectorEvents,
        List<GroundTruthEvent> groundTruthEvents,
        List<SimulationEvent> simulationEvents,
        String comparisonMode,
        String latencySource,
        String protocolCostSource,
        boolean executionSuccess,
        RunMetrics metrics
) {
    public ExperimentResult {
        detectorEvents = List.copyOf(detectorEvents);
        groundTruthEvents = List.copyOf(groundTruthEvents);
        simulationEvents = List.copyOf(simulationEvents);
    }
}
