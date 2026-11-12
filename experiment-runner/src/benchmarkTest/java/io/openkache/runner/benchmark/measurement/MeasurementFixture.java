package io.openkache.runner.benchmark.measurement;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import java.util.*;
import java.util.concurrent.*;

final class MeasurementFixture {
    private MeasurementFixture() {}
    static long ms(long value) { return TimeUnit.MILLISECONDS.toNanos(value); }
    static TraceGenerator generator() { return new TraceGenerator(new TraceConfig(101, 4, 24, 100,
            100, 0, 0, 2, TraceConfig.Distribution.UNIFORM, 0)); }
    static OpenLoopPlan openPlan() { return new OpenLoopPlan("run", "slot", 3, 10, 100, 100, 100,
            8, ms(2000), 0, 0, new OpenLoopPlan.Limits(ms(10_000), ms(1000), ms(1000), ms(500), ms(1))); }
    static ClosedLoopPlan closedPlan() { return new ClosedLoopPlan("run", "slot", 2, 3, 10, 0, 0,
            new ClosedLoopPlan.Limits(ms(10_000), ms(1000), ms(1000), ms(1000), ms(500), ms(1), 10)); }
    static DrainProbe probe() { return writes -> new DrainProbe.Observation(DrainProbe.Status.READY, "model_only", Map.of("writes", writes)); }
    static LatencyCollector collector() { return new LatencyCollector("run", "slot", "fixture-clock", ms(5000)); }
    static BenchmarkRunWriter.Metadata metadata(Set<String> required) { return new BenchmarkRunWriter.Metadata("O00", "local-functional", 3,
            null, null, "fixture-clock", required); }
    static ClientOutcome outcome(LogicalRequest request, ClientOutcome.Category category, long invoke, long terminal) {
        var status = category == ClientOutcome.Category.ERROR_RESPONSE ? ResultStatus.BUSY
                : request.command() instanceof Command.Get ? ResultStatus.MISS : ResultStatus.OK;
        var value = category == ClientOutcome.Category.NO_RESPONSE ? null : Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        var reason = switch (category) { case SUCCESS -> ClientOutcome.Reason.SUCCESS;
            case ERROR_RESPONSE -> ClientOutcome.Reason.CLIENT_ERROR; case NO_RESPONSE -> ClientOutcome.Reason.DEADLINE; };
        return new ClientOutcome(request.id("run", "slot"), ClientOutcome.Operation.of(request.command()), category,
                reason, value, invoke, terminal, terminal, request.clientId(), request.sequence() + 1, 1,
                List.of(new ClientOutcome.Attempt(1, invoke, terminal, value == null ? null : value.status(), null)));
    }
    static final class Client implements BenchmarkClient {
        final ClientAccounting ledger = new ClientAccounting();
        ClientOutcome.Category measureCategory = ClientOutcome.Category.SUCCESS;
        boolean rejectOdd, alwaysReject;
        @Override public CompletionStage<ClientOutcome> submit(LogicalRequest request) {
            if (alwaysReject || rejectOdd && request.phase() == TracePhase.MEASURE && request.globalIndex() % 2 == 1) {
                ledger.rejected(); return CompletableFuture.failedFuture(new RejectedExecutionException("fixture"));
            }
            var op = ClientOutcome.Operation.of(request.command()); long start = System.nanoTime();
            ledger.admitted(op); ledger.attempt(op);
            var category = request.phase() == TracePhase.MEASURE ? measureCategory : ClientOutcome.Category.SUCCESS;
            ledger.terminal(op, category, 1);
            return CompletableFuture.completedFuture(outcome(request, category, start, System.nanoTime()));
        }
        @Override public boolean cancel(LogicalRequest.RequestId id) { return false; }
        @Override public ClientAccounting.Snapshot snapshot() { return ledger.snapshot(); }
        @Override public void close() { }
    }
}
