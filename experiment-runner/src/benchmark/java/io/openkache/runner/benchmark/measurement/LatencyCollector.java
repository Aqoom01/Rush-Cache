package io.openkache.runner.benchmark.measurement;

import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import java.util.*;
import static io.openkache.runner.benchmark.client.ClientOutcome.*;

/**
 * 검증된 실행기 outcome을 전수 집계하는 thread-safe observer. callback에서 파일 I/O를 하지 않는다.
 * 완료 ID 전수 저장/중복 제거기는 아니며 실행기의 exactly-once 관찰 계약과 최종 계수 대조를 사용한다.
 * freeze 이후 callback은 실패한다. run 종료 후 freeze하고 결과와 대조한 뒤 export한다.
 */
public final class LatencyCollector implements OpenLoopRunner.Observer, ClosedLoopRunner.Observer {
    public enum Kind { SERVICE, SCHEDULED, DISPATCH_LAG }
    public record Population(TracePhase phase, Operation operation, Category outcome,
            Reason reason, String responseStatus, boolean censored, Kind kind) {}
    public record Series(Population population, LatencyHistogram.Snapshot histogram) {}
    public record Key(TracePhase phase, Operation operation) {}
    public record Tally(long success, long errorResponse, long noResponse, long attempts, long retried) {
        public long observed() { return success + errorResponse + noResponse; }
    }
    public record Snapshot(String runId, String loadSlot, String clockId, List<Series> series,
            Map<Key, Tally> counts, String failure) {
        public Snapshot { series = List.copyOf(series); counts = Map.copyOf(counts); }
    }
    private final String runId, loadSlot, clockId;
    private final long range;
    private final Map<Population, LatencyHistogram> histograms = new LinkedHashMap<>();
    private final Map<Key, long[]> counts = new LinkedHashMap<>();
    private String failure;
    private Snapshot frozen;

    public LatencyCollector(String runId, String loadSlot, String clockId, long highestRangeNanos) {
        if (Objects.requireNonNull(runId).isBlank() || Objects.requireNonNull(loadSlot).isBlank()
                || Objects.requireNonNull(clockId).isBlank() || highestRangeNanos < 1) {
            throw new IllegalArgumentException("collector identity/range required");
        }
        this.runId = runId; this.loadSlot = loadSlot; this.clockId = clockId; range = highestRangeNanos;
    }
    @Override public void outcome(OpenLoopResult.ScheduledOutcome event) {
        outcome(event.request(), event.dueNanos(), event.outcome());
    }
    @Override public synchronized void outcome(LogicalRequest request, long dueNanos, ClientOutcome outcome) {
        if (frozen != null) { throw new IllegalStateException("collector already frozen"); }
        try {
            Objects.requireNonNull(request); Objects.requireNonNull(outcome);
            if (!request.id(runId, loadSlot).equals(outcome.id())
                    || Operation.of(request.command()) != outcome.operation()
                    || outcome.invokedNanos() - dueNanos < 0
                    || outcome.decidedNanos() - outcome.invokedNanos() < 0
                    || outcome.terminalNanos() - outcome.decidedNanos() < 0) {
                throw new IllegalArgumentException("outcome identity/clock mismatch");
            }
            long service = outcome.terminalNanos() - outcome.invokedNanos();
            long lag = outcome.invokedNanos() - dueNanos;
            long scheduled = Math.addExact(service, lag);
            boolean censored = outcome.category() == Category.NO_RESPONSE;
            String status = outcome.category() == Category.SUCCESS ? "SEMANTIC_SUCCESS"
                    : outcome.result() == null ? "NONE" : outcome.result().status().name();
            for (Kind kind : Kind.values()) {
                var key = new Population(request.phase(), outcome.operation(), outcome.category(),
                        outcome.reason(), status, censored, kind);
                histograms.computeIfAbsent(key, ignored -> new LatencyHistogram(range)).record(switch (kind) {
                    case SERVICE -> service; case SCHEDULED -> scheduled; case DISPATCH_LAG -> lag;
                });
            }
            long[] tally = counts.computeIfAbsent(new Key(request.phase(), outcome.operation()), ignored -> new long[5]);
            int category = outcome.category().ordinal();
            tally[category] = Math.incrementExact(tally[category]);
            tally[3] = Math.addExact(tally[3], outcome.attempts().size());
            if (outcome.attempts().size() > 1) { tally[4] = Math.incrementExact(tally[4]); }
        } catch (RuntimeException error) {
            failure = "COLLECTOR_" + error.getClass().getSimpleName(); throw error;
        }
    }
    /** 반복 호출은 같은 snapshot을 반환한다. 진행 중 관찰을 완료 결과로 공개하지 않는다. */
    public synchronized Snapshot freeze() {
        if (frozen == null) {
            List<Series> series = new ArrayList<>();
            histograms.forEach((key, histogram) -> series.add(new Series(key, histogram.snapshot())));
            Map<Key, Tally> tallies = new LinkedHashMap<>();
            counts.forEach((key, n) -> tallies.put(key, new Tally(n[0], n[1], n[2], n[3], n[4])));
            frozen = new Snapshot(runId, loadSlot, clockId, series, tallies, failure);
        }
        return frozen;
    }
}
