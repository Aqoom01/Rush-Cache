package com.openkache.failuredetector.adaptive;

import java.time.Duration;

/** Parameters for the statistically adaptive heartbeat deadline baseline. */
public record AdaptiveDeadlineConfig(
        Duration heartbeatInterval,
        int warmupSamples,
        double ewmaAlpha,
        Duration initialInterval,
        Duration minimumStandardDeviation,
        double standardDeviationMultiplier,
        Duration grace,
        Duration minimumDeadline,
        Duration maximumDeadline,
        Duration maximumSampleInterval,
        Duration confirm,
        Duration warmup,
        int payloadBytes
) {
    public AdaptiveDeadlineConfig {
        if (heartbeatInterval == null || heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        if (warmupSamples < 1 || !Double.isFinite(ewmaAlpha)
                || ewmaAlpha <= 0 || ewmaAlpha > 1) {
            throw new IllegalArgumentException("warmupSamples and ewmaAlpha must be valid");
        }
        if (initialInterval == null || initialInterval.isZero() || initialInterval.isNegative()
                || minimumStandardDeviation == null || minimumStandardDeviation.isNegative()) {
            throw new IllegalArgumentException("initial interval and deviation must be valid");
        }
        if (!Double.isFinite(standardDeviationMultiplier) || standardDeviationMultiplier <= 0) {
            throw new IllegalArgumentException("standardDeviationMultiplier must be positive");
        }
        if (grace == null || grace.isNegative()) {
            throw new IllegalArgumentException("grace cannot be negative");
        }
        if (minimumDeadline == null || minimumDeadline.isZero() || minimumDeadline.isNegative()
                || maximumDeadline == null || maximumDeadline.compareTo(minimumDeadline) < 0) {
            throw new IllegalArgumentException("deadline bounds must be valid");
        }
        if (maximumSampleInterval == null || maximumSampleInterval.isZero()
                || maximumSampleInterval.isNegative()) {
            throw new IllegalArgumentException("maximumSampleInterval must be positive");
        }
        if (confirm == null || confirm.isNegative() || warmup == null || warmup.isNegative()) {
            throw new IllegalArgumentException("confirm and warmup cannot be negative");
        }
        if (payloadBytes < 1) {
            throw new IllegalArgumentException("payloadBytes must be positive");
        }
    }

    public static AdaptiveDeadlineConfig defaults() {
        return new AdaptiveDeadlineConfig(
                Duration.ofSeconds(1),
                5,
                0.2,
                Duration.ofSeconds(1),
                Duration.ofMillis(500),
                4.0,
                Duration.ZERO,
                Duration.ofSeconds(2),
                Duration.ofSeconds(10),
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                64
        );
    }
}
