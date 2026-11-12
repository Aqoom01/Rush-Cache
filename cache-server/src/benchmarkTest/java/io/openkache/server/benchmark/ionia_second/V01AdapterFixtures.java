package io.openkache.server.benchmark.ionia_second;

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

final class V01AdapterFixtures {
    private V01AdapterFixtures() {}
    static List<NodeId> ids(int n) { List<NodeId> ids = new ArrayList<>(); for(int i=1;i<=n;i++){ids.add(new NodeId(i));} return List.copyOf(ids); }
    static V01BenchmarkClient.Settings settings(int n, int rounds, int inFlight, int raw) {
        var ids = ids(n);
        return new V01BenchmarkClient.Settings("run", "slot", 1, 10_000, ids, ids.getFirst(),
                TimeUnit.SECONDS.toNanos(1), rounds, 0, inFlight, raw, TimeUnit.MILLISECONDS.toNanos(5));
    }
    static LogicalRequest request(long index, Command command) { return new LogicalRequest(TracePhase.MEASURE, index, 0, 2, index, command); }
    static Command.Get read() { return new Command.Get(Bytes.utf8("key")); }
    static Command.Delete delete() { return new Command.Delete(Bytes.utf8("key")); }
    static Result result(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
    static ClientOutcome get(CompletionStage<ClientOutcome> future) throws Exception { return future.toCompletableFuture().get(5, TimeUnit.SECONDS); }
    static void await(BooleanSupplier check) throws Exception {
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while(!check.getAsBoolean()){if(System.nanoTime()-until>=0){throw new TimeoutException("fixture await");} Thread.sleep(1);}
    }
    static final class Clock implements TimeSource {
        final AtomicLong now=new AtomicLong(1000);
        @Override public long monotonicNanos(){return now.get();}
        @Override public long epochMillis(){return 0;}
    }
    static final class Fake implements IoniaClientTransport, AutoCloseable {
        final List<NodeId> executeTargets=new CopyOnWriteArrayList<>(), recordTargets=new CopyOnWriteArrayList<>(), probeTargets=new CopyOnWriteArrayList<>();
        final List<Request> executed=new CopyOnWriteArrayList<>();
        final AtomicInteger reads=new AtomicInteger(), metas=new AtomicInteger(), closes=new AtomicInteger();
        volatile long view;
        volatile NodeId leader=new NodeId(1);
        volatile ResultStatus ack=ResultStatus.OK;
        volatile Supplier<CompletionStage<IoniaEngine.ViewInfo>> probe=()->CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(view,leader,IoniaEngine.Status.NORMAL));
        volatile BiFunction<NodeId,Request,CompletionStage<Reply>> execute=(to,r)->CompletableFuture.completedFuture(
                new Reply(to,r.view(),r.clientId(),r.operationId(),r.command() instanceof Command.Get
                        ? Result.ok(Bytes.utf8("value"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO):result(ResultStatus.OK)));
        @Override public CompletionStage<WriteAck> record(NodeId to,Request r){recordTargets.add(to);return CompletableFuture.completedFuture(new WriteAck(to,r.view(),r.clientId(),r.operationId(),ack));}
        @Override public CompletionStage<Reply> execute(NodeId to,Request r){executeTargets.add(to);executed.add(r);return execute.apply(to,r);}
        @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request r){reads.incrementAndGet();return CompletableFuture.completedFuture(new ReadReply(to,r.view(),r.clientId(),r.operationId(),1,Result.ok(Bytes.utf8("value"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO)));}
        @Override public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){metas.incrementAndGet();return CompletableFuture.completedFuture(new MetaReply(to,r.view(),r.clientId(),r.operationId(),1,null));}
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){probeTargets.add(to);return probe.get();}
        @Override public void close(){closes.incrementAndGet();}
    }
}
