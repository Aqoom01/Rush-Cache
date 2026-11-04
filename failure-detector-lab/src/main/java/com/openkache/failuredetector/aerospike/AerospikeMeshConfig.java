package com.openkache.failuredetector.aerospike;

import java.time.Duration;

public record AerospikeMeshConfig(Duration heartbeatTimeout) {
    public AerospikeMeshConfig {
        if (heartbeatTimeout.isZero() || heartbeatTimeout.isNegative()) {
            throw new IllegalArgumentException("heartbeatTimeout must be positive");
        }
    }

    public static AerospikeMeshConfig defaults() {
        return new AerospikeMeshConfig(Duration.ofMillis(1_500));
    }
}
