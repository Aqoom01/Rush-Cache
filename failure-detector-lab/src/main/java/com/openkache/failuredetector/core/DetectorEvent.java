package com.openkache.failuredetector.core;

import java.util.Map;

public record DetectorEvent(
        long atNanos,
        String observer,
        String target,
        NodeStatus oldStatus,
        NodeStatus newStatus,
        String reason,
        String algorithm,
        Map<String, Object> details
) {
    public DetectorEvent {
        details = Map.copyOf(details);
    }
}
