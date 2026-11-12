package io.openkache.runner.benchmark.execution;

import java.util.concurrent.TimeUnit;

/** 정수 ops/s의 fixed-rate, fixed-N 예정열. 불변이며 thread-safe. 부동소수점/누적 sleep 오차가 없다. */
public record FixedRateSchedule(long requests, long opsPerSecond) {
    private static final long SECOND = 1_000_000_000L;
    public FixedRateSchedule {
        if (requests < 0 || opsPerSecond < 1 || opsPerSecond > SECOND) {
            throw new IllegalArgumentException("requests>=0, rate=1..1e9 required");
        }
        try {
            if (requests > 0 && offset(requests - 1, opsPerSecond) > TimeUnit.DAYS.toNanos(1)) {
                throw new IllegalArgumentException("schedule horizon exceeds one day");
            }
        } catch (ArithmeticException error) { throw new IllegalArgumentException("schedule horizon overflow", error); }
    }
    /** phase 시작으로부터 floor(index*1e9/rate) ns. 첫 요청은0이다. */
    public long offsetNanos(long index) {
        if (index < 0 || index >= requests) { throw new IllegalArgumentException("index outside schedule"); }
        return offset(index, opsPerSecond);
    }
    public long spanNanos() { return requests == 0 ? 0 : offsetNanos(requests - 1); }
    /** elapsed 시점까지 예정된 요청 수. unissued의 due/future 분리에 사용한다. */
    public long dueCountAt(long elapsedNanos) {
        if (elapsedNanos < 0 || requests == 0) { return 0; }
        if (elapsedNanos >= spanNanos()) { return requests; }
        long inclusive = elapsedNanos + 1;
        long whole = (inclusive / SECOND) * opsPerSecond;
        long fraction = ((inclusive % SECOND) * opsPerSecond + SECOND - 1) / SECOND;
        return Math.min(requests, whole + fraction);
    }
    private static long offset(long index, long rate) {
        return Math.addExact(Math.multiplyExact(index / rate, SECOND),
                ((index % rate) * SECOND) / rate);
    }
}

