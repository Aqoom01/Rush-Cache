package com.openkache.failuredetector.core;

public record GroundTruthEvent(
        long atNanos,
        String target,
        boolean alive,
        boolean reachable,
        boolean serviceHealthy,
        String faultId,
        String faultType
) {
    public GroundTruthEvent(
            long atNanos,
            String target,
            boolean alive,
            String faultId,
            String faultType
    ) {
        this(atNanos, target, alive, alive, alive, faultId, faultType);
    }
}
