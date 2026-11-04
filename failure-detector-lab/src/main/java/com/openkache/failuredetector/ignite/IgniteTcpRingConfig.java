package com.openkache.failuredetector.ignite;

import java.time.Duration;

public record IgniteTcpRingConfig(Duration neighborTimeout) {
    public IgniteTcpRingConfig {
        if (neighborTimeout.isZero() || neighborTimeout.isNegative()) {
            throw new IllegalArgumentException("neighborTimeout must be positive");
        }
    }

    public static IgniteTcpRingConfig defaults() {
        return new IgniteTcpRingConfig(Duration.ofSeconds(5));
    }
}
