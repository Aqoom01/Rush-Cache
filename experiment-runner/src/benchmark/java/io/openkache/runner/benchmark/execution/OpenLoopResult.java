package io.openkache.runner.benchmark.execution;

import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.List;
import java.util.Map;

/** open-loop의 수집/발행/서비스 상태를 분리한 불변 관찰. 표준 성능 export가 아니다. */
public record OpenLoopResult(boolean allArrivalsConsidered, boolean collectionValid,
        boolean scheduleQualified, boolean serviceHealthy, String failure,
        long startedNanos, long finishedNanos, Map<TracePhase, Phase> phases,
        List<Event> events, ClientAccounting.Snapshot finalClient,
        DrainProbe.Observation cleanupObservation, int outstandingCallbacks, long expectedWrites) {
    public OpenLoopResult { phases = Map.copyOf(phases); events = List.copyOf(events); }
    public boolean completed() { return allArrivalsConsidered && collectionValid; }
    /** 정상 무손실 시나리오의 편의 판정. 과부하 관찰의 collectionValid와 혼동하지 않는다. */
    public boolean passed() { return completed() && scheduleQualified && serviceHealthy; }

    public enum DecisionKind { SUBMIT_ATTEMPT, CAPACITY_DROP, TIMING_MISS }
    public record Decision(TracePhase phase, long index, ClientOutcome.Operation operation,
            long dueNanos, long decisionNanos, DecisionKind kind) {}
    public record ScheduledOutcome(LogicalRequest request, long dueNanos,
            long driverSubmitNanos, ClientOutcome outcome) {}
    public record Event(ClosedLoopResult.Stage stage, long startedNanos, long endedNanos,
            boolean completed, String reason, DrainProbe.Observation observation) {}

    /** count0/overflow는 sum/min/max=null이며 overflow flag를 보존한다. ns 단위. */
    public record Lag(long count, Long sumNanos, Long minimumNanos, Long maximumNanos, boolean overflow) {}

    /** considered=success+error+noResponse+capacityDrop+timingMiss+clientRejected+submitFailed+unresolved. */
    public record Counts(long considered, long success, long errorResponse, long noResponse,
            long capacityDrop, long timingMiss, long clientRejected, long submitFailed,
            long unresolved, long attempts, long retriedLogical) {
        public long outcomes() { return success + errorResponse + noResponse; }
    }
    public record Phase(TracePhase phase, FixedRateSchedule schedule, long startedNanos,
            long emissionEndedNanos, long outcomeWaitEndedNanos, long unissued,
            long dueButUnissued, long futureCancelled, Map<ClientOutcome.Operation, Counts> byOperation,
            long submissionCalls, long invalidOutcomes, int peakInFlight,
            Lag decisionLag, Lag submissionLag, Lag admittedLag,
            ClientAccounting.Snapshot clientBefore, ClientAccounting.Snapshot clientAfter) {
        public Phase { byOperation = Map.copyOf(byOperation); }
        public long considered() { return byOperation.values().stream().mapToLong(Counts::considered).sum(); }
        public long success() { return byOperation.values().stream().mapToLong(Counts::success).sum(); }
        public long unresolved() { return byOperation.values().stream().mapToLong(Counts::unresolved).sum(); }
        public long timingMisses() { return byOperation.values().stream().mapToLong(Counts::timingMiss).sum(); }
        public long capacityDrops() { return byOperation.values().stream().mapToLong(Counts::capacityDrop).sum(); }
        public long successfulWrites() {
            return byOperation.get(ClientOutcome.Operation.SET).success()
                    + byOperation.get(ClientOutcome.Operation.DELETE).success();
        }
        public long uncertainWrites() {
            long n = 0;
            for (var op : List.of(ClientOutcome.Operation.SET, ClientOutcome.Operation.DELETE)) {
                var c = byOperation.get(op); n += c.errorResponse() + c.noResponse();
            }
            return n;
        }
        public boolean healthy() { return considered() == schedule.requests() && success() == schedule.requests(); }
    }
}

