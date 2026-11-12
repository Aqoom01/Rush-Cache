package com.openkache.failuredetector.product;

import java.util.Map;

public record BenchmarkEvent(
        long atNanos,
        BenchmarkEventType type,
        String observer,
        String target,
        Map<String, Object> details
) {
    public BenchmarkEvent {
        details = Map.copyOf(details);
    }
}
