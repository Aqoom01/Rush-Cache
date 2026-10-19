package io.openkache.runner;

import java.util.Arrays;
import java.util.Random;

/**
 * 고정 크기 reservoir에 latency 샘플(나노초)을 저장한다. 전체 요청 수가 커도 메모리 사용량은 {@code capacity}로
 * 고정된다(reservoir sampling, Algorithm R). percentile은 모든 응답을 메모리에 쌓지 않고 이 reservoir로 근사한다.
 *
 * <p>여러 thread에서 동시에 {@link #offer(long)}를 호출할 수 있다.
 */
final class LatencyReservoir {

    private final long[] samples;
    private final Random random;
    private long seen;
    private int filled;

    LatencyReservoir(int capacity, long seed) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1: " + capacity);
        }
        this.samples = new long[capacity];
        this.random = new Random(seed);
    }

    synchronized void offer(long nanos) {
        seen++;
        if (filled < samples.length) {
            samples[filled++] = nanos;
            return;
        }
        int index = random.nextInt((int) Math.min(seen, Integer.MAX_VALUE));
        if (index < samples.length) {
            samples[index] = nanos;
        }
    }

    /** 지금까지 {@link #offer}로 들어온 전체 요청 수(reservoir에 실제로 남아 있는 샘플 수와 다를 수 있다). */
    synchronized long totalObserved() {
        return seen;
    }

    synchronized int sampleCount() {
        return filled;
    }

    /** {@code percentile}(0.0-1.0) 값(나노초). 샘플이 없으면 0. */
    synchronized long percentileNanos(double percentile) {
        if (filled == 0) {
            return 0;
        }
        long[] sorted = Arrays.copyOf(samples, filled);
        Arrays.sort(sorted);
        int index = (int) Math.ceil(percentile * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }
}
