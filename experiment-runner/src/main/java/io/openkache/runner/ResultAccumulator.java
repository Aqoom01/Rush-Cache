package io.openkache.runner;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 측정 구간에서 요청 결과를 결과 종류별로 센다. percentile은 {@link LatencyReservoir}로 근사하므로 모든 latency를
 * 무제한으로 쌓지 않는다.
 *
 * <p>여러 thread에서 동시에 호출해도 안전하다.
 */
final class ResultAccumulator {

    private final Map<Outcome, LongAdder> counts = new EnumMap<>(Outcome.class);
    private final LatencyReservoir latencies;

    ResultAccumulator(int reservoirCapacity, long seed) {
        for (Outcome outcome : Outcome.values()) {
            counts.put(outcome, new LongAdder());
        }
        this.latencies = new LatencyReservoir(reservoirCapacity, seed);
    }

    /** 응답을 실제로 받은 요청. latency는 요청을 보내기 직전부터 응답을 전부 읽은 직후까지다(큐·전송 포함, 재시도는 없다). */
    void recordCompleted(Outcome outcome, long latencyNanos) {
        counts.get(outcome).increment();
        latencies.offer(latencyNanos);
    }

    /** 응답을 전혀 받지 못한 요청({@link Outcome#CLIENT_FAILURE}). latency 통계에는 넣지 않는다. */
    void recordFailure(Outcome outcome) {
        counts.get(outcome).increment();
    }

    long count(Outcome outcome) {
        return counts.get(outcome).sum();
    }

    long totalRequests() {
        return counts.values().stream().mapToLong(LongAdder::sum).sum();
    }

    long p50Nanos() {
        return latencies.percentileNanos(0.50);
    }

    long p99Nanos() {
        return latencies.percentileNanos(0.99);
    }

    int latencySampleCount() {
        return latencies.sampleCount();
    }

    long latencyTotalObserved() {
        return latencies.totalObserved();
    }
}
