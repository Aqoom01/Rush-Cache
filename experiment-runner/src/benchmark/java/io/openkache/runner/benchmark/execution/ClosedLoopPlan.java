package io.openkache.runner.benchmark.execution;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** 한 JVM의 fixed-N closed-loop 실험 계획. ns는 real monotonic clock 기준이다. */
public record ClosedLoopPlan(String runId, String loadSlot, int concurrency,
        long warmupRequests, long measuredRequests, long minimumWarmupNanos,
        long minimumMeasureNanos, Limits limits) {
    public ClosedLoopPlan {
        if (Objects.requireNonNull(runId).isBlank() || Objects.requireNonNull(loadSlot).isBlank()
                || concurrency < 1 || concurrency > 1024 || warmupRequests < 0 || measuredRequests < 1
                || minimumWarmupNanos < 0 || minimumMeasureNanos < 0) {
            throw new IllegalArgumentException("invalid closed-loop plan");
        }
        Objects.requireNonNull(limits);
        if (minimumWarmupNanos > limits.workNanos() || minimumMeasureNanos > limits.workNanos()) {
            throw new IllegalArgumentException("minimum duration exceeds work limit");
        }
    }

    /** work는 전체 작업 한도, cleanup은 그 뒤의 별도 종료 grace. realtime 보장은 아니다. */
    public record Limits(long workNanos, long completionWaitNanos, long admissionWaitNanos,
            long barrierWaitNanos, long cleanupNanos, long pollNanos, int maxSubmissionAttempts) {
        public Limits {
            long maximum = TimeUnit.DAYS.toNanos(1);
            for (long n : new long[]{workNanos, completionWaitNanos, admissionWaitNanos,
                    barrierWaitNanos, cleanupNanos, pollNanos}) {
                if (n < 1 || n > maximum) { throw new IllegalArgumentException("duration must be positive and <=1 day"); }
            }
            if (pollNanos > Math.min(admissionWaitNanos, Math.min(barrierWaitNanos, cleanupNanos))
                    || maxSubmissionAttempts < 1 || maxSubmissionAttempts > 1_000_000) {
                throw new IllegalArgumentException("invalid polling/admission bounds");
            }
        }
    }
}

