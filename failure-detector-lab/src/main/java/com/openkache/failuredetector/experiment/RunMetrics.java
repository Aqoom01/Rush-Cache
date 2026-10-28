package com.openkache.failuredetector.experiment;

public record RunMetrics(
        long falsePositiveEvents,
        long falseSuspectNanos,
        long falseDeadEvents,
        long falseDeadNanos,
        long unreachableButAliveEvents,
        long unreachableButAliveNanos,
        long falseNegativeEvents,
        Long suspicionLatencyNanos,
        Long detectionLatencyNanos,
        long flapCount,
        long protocolMessages,
        long protocolBytes,
        boolean falseStateRightCensored
) {
    public double falseSuspectSeconds() {
        return falseSuspectNanos / 1_000_000_000.0;
    }

    public Double detectionLatencyMillis() {
        return detectionLatencyNanos == null ? null : detectionLatencyNanos / 1_000_000.0;
    }

    public double falseDeadSeconds() {
        return falseDeadNanos / 1_000_000_000.0;
    }

    public double unreachableButAliveSeconds() {
        return unreachableButAliveNanos / 1_000_000_000.0;
    }

    public Double suspicionLatencyMillis() {
        return suspicionLatencyNanos == null ? null : suspicionLatencyNanos / 1_000_000.0;
    }

    public double falsePositivePerNodeHour(long durationNanos) {
        double hours = durationNanos / 3_600_000_000_000.0;
        return hours == 0 ? 0 : falsePositiveEvents / hours;
    }
}
