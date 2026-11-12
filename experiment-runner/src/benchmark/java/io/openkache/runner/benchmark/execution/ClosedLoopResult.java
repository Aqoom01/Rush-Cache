package io.openkache.runner.benchmark.execution;

import io.openkache.runner.benchmark.TracePhase;
import io.openkache.runner.benchmark.client.ClientAccounting;
import io.openkache.runner.benchmark.client.ClientOutcome;
import java.util.List;
import java.util.Map;

/** 고정 크기의 실행 관찰. 성능 result.schema.json export/선형성 판정과 별개다. */
public record ClosedLoopResult(boolean passed, String failure, long startedNanos, long finishedNanos,
        Map<TracePhase, Phase> phases, List<Event> events, ClientAccounting.Snapshot finalClient,
        DrainProbe.Observation cleanupObservation, boolean workersTerminated, long expectedWrites) {
    public ClosedLoopResult { phases = Map.copyOf(phases); events = List.copyOf(events); }

    public enum Stage { PREFLIGHT, LOAD, LOAD_DRAIN, WARMUP, WARMUP_DRAIN, MEASURE, FINAL_DRAIN, CLEANUP }

    public record Event(Stage stage, long startedNanos, long endedNanos,
            boolean passed, String reason, DrainProbe.Observation observation) {}

    /** scheduled = success+errorResponse+noResponse+notAdmitted+unresolved. */
    public record Counts(long scheduled, long success, long errorResponse, long noResponse,
            long notAdmitted, long unresolved, long attempts, long retriedLogical) {
        public long observedOutcomes() { return success + errorResponse + noResponse; }
    }

    public record Phase(TracePhase phase, long requested, long startedNanos, long endedNanos,
            Map<ClientOutcome.Operation, Counts> byOperation, long submissionCalls,
            long admissionRejections, long invalidOutcomes, int peakDriverSlots,
            ClientAccounting.Snapshot clientBefore, ClientAccounting.Snapshot clientAfter) {
        public Phase { byOperation = Map.copyOf(byOperation); }
        public long scheduled() { return byOperation.values().stream().mapToLong(Counts::scheduled).sum(); }
        public long success() { return byOperation.values().stream().mapToLong(Counts::success).sum(); }
        public long unresolved() { return byOperation.values().stream().mapToLong(Counts::unresolved).sum(); }
        public long elapsedNanos() { return endedNanos - startedNanos; }
        public long successfulWrites() {
            return byOperation.get(ClientOutcome.Operation.SET).success()
                    + byOperation.get(ClientOutcome.Operation.DELETE).success();
        }
    }
}

