package com.openkache.failuredetector.product;

import java.util.List;
import java.util.Map;

public record ProductRunResult(
        String runId,
        ProductAlgorithm algorithm,
        ProductScenario scenario,
        long seed,
        int nodeCount,
        String mode,
        long durationNanos,
        long faultStartNanos,
        List<String> targets,
        List<BenchmarkEvent> events,
        Long detectionLatencyNanos,
        Long clusterConfirmationLatencyNanos,
        Long membershipConvergenceNanos,
        long falseUnavailableEvents,
        long falseDeadEvents,
        long falseUnavailableNanos,
        int detectedTargets,
        boolean detectionSuccess,
        long txMessages,
        long rxMessages,
        long txBytes,
        long rxBytes,
        Map<String, Object> parameters,
        String implementationType,
        String latencySource,
        String networkCostSource,
        boolean executionSuccess,
        String error
) {
    public ProductRunResult {
        targets = List.copyOf(targets);
        events = List.copyOf(events);
        parameters = Map.copyOf(parameters);
    }
}
