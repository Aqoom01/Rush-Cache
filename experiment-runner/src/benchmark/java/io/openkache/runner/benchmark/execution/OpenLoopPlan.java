package io.openkache.runner.benchmark.execution;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** 응답과 독립적인 세 phase의 fixed-rate 계획. LOAD/WARMUP은 정상 완료해야 측정으로 진행한다. */
public record OpenLoopPlan(String runId, String loadSlot, long warmupRequests, long measuredRequests,
        long loadOpsPerSecond, long warmupOpsPerSecond, long measureOpsPerSecond,
        int maxInFlight, long maxDispatchLagNanos, long minimumWarmupNanos,
        long minimumMeasureNanos, Limits limits) {
    public OpenLoopPlan {
        if (Objects.requireNonNull(runId).isBlank() || Objects.requireNonNull(loadSlot).isBlank()
                || warmupRequests < 0 || measuredRequests < 1 || maxInFlight < 1 || maxInFlight > 100_000
                || maxDispatchLagNanos < 0 || minimumWarmupNanos < 0 || minimumMeasureNanos < 0) {
            throw new IllegalArgumentException("invalid open-loop plan");
        }
        Objects.requireNonNull(limits);
        new FixedRateSchedule(0, loadOpsPerSecond);
        var warm = new FixedRateSchedule(warmupRequests, warmupOpsPerSecond);
        var measure = new FixedRateSchedule(measuredRequests, measureOpsPerSecond);
        if (warm.spanNanos() < minimumWarmupNanos || measure.spanNanos() < minimumMeasureNanos
                || maxDispatchLagNanos > limits.workNanos()) {
            throw new IllegalArgumentException("schedule/minimum/lag profile is not qualified");
        }
    }
    /** 작업 이후 cleanup grace는 별도다. backend/probe/observer의 비차단 계약을 전제로 한다. */
    public record Limits(long workNanos, long outcomeDrainNanos, long barrierNanos,
            long cleanupNanos, long pollNanos) {
        public Limits {
            for (long n : new long[]{workNanos, outcomeDrainNanos, barrierNanos, cleanupNanos, pollNanos}) {
                if (n < 1 || n > TimeUnit.DAYS.toNanos(1)) { throw new IllegalArgumentException("invalid time budget"); }
            }
            if (pollNanos > Math.min(outcomeDrainNanos, Math.min(barrierNanos, cleanupNanos))) {
                throw new IllegalArgumentException("poll exceeds a drain budget");
            }
        }
    }
}

