package com.openkache.failuredetector.experiment;

import java.time.Duration;

public record ScenarioConfig(
        ScenarioType type,
        int nodeCount,
        Duration duration,
        Duration tick,
        Duration faultStart,
        Duration faultEnd,
        double packetLoss,
        Duration fixedLatency,
        Duration jitter,
        int gossipFanout
) {
    public ScenarioConfig {
        if (nodeCount < 3 || gossipFanout < 1) {
            throw new IllegalArgumentException("invalid nodeCount or gossipFanout");
        }
        if (duration.isNegative() || duration.isZero() || tick.isNegative() || tick.isZero()) {
            throw new IllegalArgumentException("duration and tick must be positive");
        }
        if (packetLoss < 0 || packetLoss > 1) {
            throw new IllegalArgumentException("packetLoss must be between zero and one");
        }
    }

    public static ScenarioConfig defaults(ScenarioType type) {
        Duration end = type == ScenarioType.OBSERVER_PAUSE || type == ScenarioType.TARGET_PAUSE
                ? Duration.ofSeconds(22)
                : Duration.ofSeconds(30);
        double loss = switch (type) {
            case PACKET_LOSS -> 0.15;
            case PACKET_LOSS_BURST -> 0.45;
            default -> 0.0;
        };
        Duration latency = switch (type) {
            case LATENCY -> Duration.ofMillis(1_200);
            case LATENCY_GRADUAL -> Duration.ofMillis(2_500);
            case LATENCY_BURSTY -> Duration.ofMillis(3_000);
            case LATENCY_REGIME -> Duration.ofMillis(1_800);
            case CRASH_UNDER_JITTER -> Duration.ofMillis(2_200);
            default -> Duration.ofMillis(20);
        };
        Duration jitter = switch (type) {
            case LATENCY -> Duration.ofMillis(400);
            case LATENCY_GRADUAL -> Duration.ofMillis(250);
            case LATENCY_BURSTY -> Duration.ofMillis(800);
            case LATENCY_REGIME -> Duration.ofMillis(300);
            case CRASH_UNDER_JITTER -> Duration.ofMillis(700);
            default -> Duration.ofMillis(5);
        };
        boolean crashScenario = type == ScenarioType.CRASH
                || type == ScenarioType.CRASH_UNDER_JITTER;
        Duration duration = crashScenario
                ? Duration.ofSeconds(90)
                : isDetailedLatency(type) ? Duration.ofSeconds(60) : Duration.ofSeconds(45);
        Duration faultStart = type == ScenarioType.CRASH
                ? Duration.ofSeconds(20)
                : Duration.ofSeconds(15);
        Duration faultEnd = type == ScenarioType.CRASH_UNDER_JITTER
                ? Duration.ofSeconds(60)
                : isDetailedLatency(type) ? Duration.ofSeconds(45) : end;
        return new ScenarioConfig(
                type,
                5,
                duration,
                Duration.ofMillis(100),
                faultStart,
                faultEnd,
                loss,
                latency,
                jitter,
                3
        );
    }

    private static boolean isDetailedLatency(ScenarioType type) {
        return type == ScenarioType.LATENCY_GRADUAL
                || type == ScenarioType.LATENCY_BURSTY
                || type == ScenarioType.LATENCY_REGIME
                || type == ScenarioType.CRASH_UNDER_JITTER
                || type == ScenarioType.PACKET_LOSS_BURST;
    }

    public boolean inFaultWindow(long nowNanos) {
        return nowNanos >= faultStart.toNanos() && nowNanos < faultEnd.toNanos();
    }
}
