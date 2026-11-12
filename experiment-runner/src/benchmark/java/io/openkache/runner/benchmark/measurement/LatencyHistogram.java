package io.openkache.runner.benchmark.measurement;

import java.math.BigInteger;
import java.util.*;

/** 전수 ns histogram. 외부 collector lock에서 사용하며 snapshot은 불변이다. */
public final class LatencyHistogram {
    public static final String BIN_VERSION = "ns-ceil-1pct-v1";
    private final long[] bounds;
    private final NavigableMap<Long, Long> buckets = new TreeMap<>();
    private long count, overflow;
    private Long minimum, maximum;
    private BigInteger sum = BigInteger.ZERO;

    /** limit 이하의 명세 recurrence 경계만 사용한다. 실제 finite range는 snapshot에 기록한다. */
    public LatencyHistogram(long limitNanos) {
        if (limitNanos < 1) { throw new IllegalArgumentException("positive histogram range required"); }
        List<Long> values = new ArrayList<>(); values.add(0L);
        long upper = 1;
        while (upper <= limitNanos) {
            values.add(upper);
            long increment = upper / 100 + (upper % 100 == 0 ? 0 : 1);
            if (Long.MAX_VALUE - upper < increment) { break; }
            upper += increment;
        }
        bounds = values.stream().mapToLong(Long::longValue).toArray();
    }

    public void record(long nanos) {
        if (nanos < 0) { throw new IllegalArgumentException("negative latency/clock error"); }
        long nextCount = Math.incrementExact(count);
        int index = Arrays.binarySearch(bounds, nanos);
        if (index < 0) { index = -index - 1; }
        if (index == bounds.length) { overflow = Math.incrementExact(overflow); }
        else { buckets.merge(bounds[index], 1L, Math::addExact); }
        count = nextCount; sum = sum.add(BigInteger.valueOf(nanos));
        minimum = minimum == null ? nanos : Math.min(minimum, nanos);
        maximum = maximum == null ? nanos : Math.max(maximum, nanos);
    }

    public Snapshot snapshot() {
        return new Snapshot(BIN_VERSION, bounds[bounds.length - 1], count,
                count == 0 ? null : sum, minimum, maximum, overflow,
                Collections.unmodifiableNavigableMap(new TreeMap<>(buckets)));
    }

    /** percentile은 nearest-rank bucket 상한이다. raw percentile이나 신뢰구간이 아니다. */
    public record Quantile(Long upperBoundNanos, String reason, boolean comparisonEligible) {}
    public record Snapshot(String binVersion, long highestFiniteNanos, long count,
            BigInteger sumNanos, Long minimumNanos, Long maximumNanos,
            long overflowCount, NavigableMap<Long, Long> buckets) {
        public Snapshot { buckets = Collections.unmodifiableNavigableMap(new TreeMap<>(buckets)); }
        public Double meanNanos() { return count == 0 ? null : sumNanos.doubleValue() / count; }
        public Quantile quantile(int numerator, int denominator) {
            if (numerator < 1 || denominator < numerator || denominator > 1000) {
                throw new IllegalArgumentException("quantile must be in (0,1], denominator<=1000");
            }
            if (count == 0) { return new Quantile(null, "no_samples", false); }
            long rank = (count / denominator) * numerator
                    + ((count % denominator) * numerator + denominator - 1) / denominator;
            long seen = 0;
            boolean eligible = count >= (numerator * 1000L == denominator * 999L ? 100_000 : 1);
            for (var bucket : buckets.entrySet()) {
                seen = Math.addExact(seen, bucket.getValue());
                if (seen >= rank) { return new Quantile(bucket.getKey(), eligible ? null : "insufficient_samples", eligible); }
            }
            return new Quantile(null, "out_of_range", false);
        }
    }
}
