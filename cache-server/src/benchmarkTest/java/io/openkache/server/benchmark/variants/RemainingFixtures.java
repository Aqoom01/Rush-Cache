package io.openkache.server.benchmark.variants;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_third.*;
import io.openkache.core.replication.ionia_fourth.*;
import io.openkache.core.replication.ionia_fifth.*;
import io.openkache.core.replication.ionia_sixth.*;
import io.openkache.core.replication.ionia_seventh.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.ionia_third.*;
import io.openkache.server.benchmark.ionia_fourth.*;
import io.openkache.server.benchmark.ionia_fifth.*;
import io.openkache.server.benchmark.ionia_sixth.*;
import io.openkache.server.benchmark.ionia_seventh.*;
import io.openkache.server.benchmark.ionia_eighth.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

final class RemainingFixtures {
    private RemainingFixtures() {}
    enum Variant { V02, V03, V04, V05, V06, V07 }
    static List<NodeId> ids(int n){List<NodeId> out=new ArrayList<>();for(int i=1;i<=n;i++){out.add(new NodeId(i));}return List.copyOf(out);}
    static O00BenchmarkClient.Settings settings(int n,int rounds){return new O00BenchmarkClient.Settings("remaining","slot",1,20_000,
            ids(n),new NodeId(1),new NodeId(2),TimeUnit.SECONDS.toNanos(1),rounds,0,16,128,TimeUnit.MILLISECONDS.toNanos(1));}
    static IoniaReadSelectionSettings selection(IoniaReadSelectionSettings.Mode mode){return new IoniaReadSelectionSettings(mode,16,8,1,1000,100,4);}
    static VariantBenchmarkClient create(Variant v,O00BenchmarkClient.Settings runtime,Fake raw,TimeSource time){return switch(v){
        case V02->new V02BenchmarkClient(runtime,new IoniaHistorySettings(8),raw,time);
        case V03->new V03BenchmarkClient(runtime,new IoniaBatchSettings(4,4096,20_000_000),raw,time);
        case V04->new V04BenchmarkClient(runtime,new IoniaApplySettings(2,2),raw,time);
        case V05->new V05BenchmarkClient(runtime,new IoniaWorkerSettings(4,2),raw,time);
        case V06->new V06BenchmarkClient(runtime,selection(IoniaReadSelectionSettings.Mode.ADAPTIVE),raw,time);
        case V07->new V07BenchmarkClient(runtime,new IoniaMetaBatchSettings(1,65536,0,16,2),raw,time);
    };}
    static LogicalRequest request(long index,Command command){return new LogicalRequest(TracePhase.MEASURE,index,0,2,index,command);}
    static Command.Get read(){return new Command.Get(Bytes.utf8("key"));}
    static Command.Delete delete(){return new Command.Delete(Bytes.utf8("key"));}
    static Result result(ResultStatus s){return Result.of(s,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
    static ClientOutcome get(CompletionStage<ClientOutcome> stage)throws Exception{return stage.toCompletableFuture().get(5,TimeUnit.SECONDS);}
    static void await(BooleanSupplier p)throws Exception{long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!p.getAsBoolean()){if(System.nanoTime()-until>=0){throw new TimeoutException("condition");}Thread.sleep(1);}}
    static final class Clock implements TimeSource{
        final AtomicLong now=new AtomicLong(1000);@Override public long monotonicNanos(){return now.get();}@Override public long epochMillis(){return 0;}
    }
    static final class Fake implements IoniaMetaBatchTransport,AutoCloseable{
        final AtomicInteger closes=new AtomicInteger(),records=new AtomicInteger(),reads=new AtomicInteger(),metas=new AtomicInteger(),probes=new AtomicInteger(),batches=new AtomicInteger();
        final List<Request> executed=new CopyOnWriteArrayList<>();final List<NodeId> executeTargets=new CopyOnWriteArrayList<>(),readTargets=new CopyOnWriteArrayList<>();
        final List<List<Item>> batchesSent=new CopyOnWriteArrayList<>();
        volatile long view;volatile NodeId leader=new NodeId(1);
        volatile Supplier<CompletionStage<IoniaEngine.ViewInfo>> probe=()->CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(view,leader,IoniaEngine.Status.NORMAL));
        volatile BiFunction<NodeId,Request,CompletionStage<Reply>> execute=(to,r)->CompletableFuture.completedFuture(new Reply(to,r.view(),r.clientId(),r.operationId(),r.command() instanceof Command.Get?Result.ok(Bytes.utf8("value"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO):result(ResultStatus.OK)));
        volatile BiFunction<NodeId,Request,CompletionStage<ReadReply>> read=(to,r)->CompletableFuture.completedFuture(new ReadReply(to,r.view(),r.clientId(),r.operationId(),1,Result.ok(Bytes.utf8("value"),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO)));
        volatile BiFunction<NodeId,List<Item>,CompletionStage<List<Answer>>> batch=(to,items)->CompletableFuture.completedFuture(items.stream().map(i->new Answer(i.token(),new MetaReply(to,i.request().view(),i.request().clientId(),i.request().operationId(),1,null))).toList());
        @Override public CompletionStage<WriteAck> record(NodeId to,Request r){records.incrementAndGet();return CompletableFuture.completedFuture(new WriteAck(to,r.view(),r.clientId(),r.operationId(),ResultStatus.OK));}
        @Override public CompletionStage<Reply> execute(NodeId to,Request r){executeTargets.add(to);executed.add(r);return execute.apply(to,r);}
        @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request r){reads.incrementAndGet();readTargets.add(to);return read.apply(to,r);}
        @Override public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){metas.incrementAndGet();return CompletableFuture.completedFuture(new MetaReply(to,r.view(),r.clientId(),r.operationId(),1,null));}
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){probes.incrementAndGet();return probe.get();}
        @Override public CompletionStage<List<Answer>> metaBatch(NodeId to,List<Item> items){batches.incrementAndGet();batchesSent.add(List.copyOf(items));return batch.apply(to,items);}
        @Override public void close(){closes.incrementAndGet();}
    }
}
