package com.openkache.failuredetector.phi;

import java.time.Duration;

public record PhiAccrualConfig(
        Duration heartbeatInterval,
        double convictThreshold,
        int sampleSize,
        Duration initialInterval,
        Duration maxSampleInterval,
        Duration maxLocalPause
) {
    public PhiAccrualConfig {
        if (heartbeatInterval == null || heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        if (convictThreshold <= 0 || sampleSize < 2) {
            throw new IllegalArgumentException("invalid threshold or sample size");
        }
        if (initialInterval == null || initialInterval.isZero() || initialInterval.isNegative()) {
            throw new IllegalArgumentException("initialInterval must be positive");
        }
        if (maxSampleInterval == null || maxSampleInterval.isZero() || maxSampleInterval.isNegative()) {
            throw new IllegalArgumentException("maxSampleInterval must be positive");
        }
        if (maxLocalPause == null || maxLocalPause.isNegative()) {
            throw new IllegalArgumentException("maxLocalPause cannot be negative");
        }
    }

    public static PhiAccrualConfig defaults() {
        return new PhiAccrualConfig(
                Duration.ofSeconds(1),
                8.0,
                1_000,
                Duration.ofSeconds(2),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5)
        );
    }
}
