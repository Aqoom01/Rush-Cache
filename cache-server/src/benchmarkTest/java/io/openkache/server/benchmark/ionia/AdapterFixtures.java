package io.openkache.server.benchmark.ionia;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.ClientOutcome;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

final class AdapterFixtures {
    static final List<NodeId> IDS = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
    static O00BenchmarkClient.Settings settings(int maxInFlight, int rpcLimit, int rounds) {
        return new O00BenchmarkClient.Settings("run", "load-1", 1, 10_000, IDS,
                IDS.getFirst(), IDS.getFirst(), TimeUnit.SECONDS.toNanos(1),
                rounds, 0, maxInFlight, rpcLimit, TimeUnit.MILLISECONDS.toNanos(10));
    }
    static LogicalRequest request(long index, Command command) {
        return new LogicalRequest(TracePhase.MEASURE, index, 0, 2, index, command);
    }
    static Command.Delete delete() { return new Command.Delete(Bytes.utf8("key")); }
    static Result result(ResultStatus status) {
        return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }
    static ClientOutcome get(CompletionStage<ClientOutcome> stage) throws Exception {
        return stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    static void await(BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - end >= 0) { throw new TimeoutException("condition not reached"); }
            Thread.sleep(1);
        }
    }
    static final class Clock implements TimeSource {
        final AtomicLong now = new AtomicLong(1000);
        @Override public long monotonicNanos() { return now.get(); }
        @Override public long epochMillis() { return 0; }
    }
    static final class Fake implements IoniaClientTransport, AutoCloseable {
        final AtomicInteger probes = new AtomicInteger();
        final AtomicInteger records = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final List<Request> executed = new CopyOnWriteArrayList<>();
        final List<NodeId> executeTargets = new CopyOnWriteArrayList<>();
        volatile long view;
        volatile NodeId leader = IDS.getFirst();
        volatile ResultStatus ackStatus = ResultStatus.OK;
        volatile Function<Request, CompletionStage<Result>> execute = request ->
                CompletableFuture.completedFuture(request.command() instanceof Command.Get
                        ? Result.ok(Bytes.utf8("value"), 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO)
                        : result(ResultStatus.OK));
        volatile Supplier<CompletionStage<IoniaEngine.ViewInfo>> probe = () ->
                CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(view, leader, IoniaEngine.Status.NORMAL));

        @Override public CompletionStage<WriteAck> record(NodeId to, Request r) {
            records.incrementAndGet();
            return CompletableFuture.completedFuture(new WriteAck(to, r.view(), r.clientId(), r.operationId(), ackStatus));
        }
        @Override public CompletionStage<Reply> execute(NodeId to, Request r) {
            executed.add(r); executeTargets.add(to);
            return execute.apply(r).thenApply(result ->
                    new Reply(to, r.view(), r.clientId(), r.operationId(), result));
        }
        @Override public CompletionStage<ReadReply> followerRead(NodeId to, Request r) {
            return execute.apply(r).thenApply(result -> new ReadReply(to, r.view(), r.clientId(), r.operationId(), 0, result));
        }
        @Override public CompletionStage<MetaReply> metaQuery(NodeId to, Request r) {
            return CompletableFuture.completedFuture(new MetaReply(to, r.view(), r.clientId(), r.operationId(), 0, null));
        }
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) {
            probes.incrementAndGet(); return probe.get();
        }
        @Override public void close() { closes.incrementAndGet(); }
    }
}

