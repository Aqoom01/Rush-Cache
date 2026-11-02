package io.openkache.runner.benchmark.measurement;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

final class JvmFixtures {
    private JvmFixtures() {}
    static long ms(long n){return TimeUnit.MILLISECONDS.toNanos(n);}
    static JvmWindowCollector.Settings settings(){return JvmWindowCollector.Settings.local("run","slot","clock",JvmWindowCollector.Role.SHARED_FIXTURE);}
    static final class Source implements JvmSnapshotSource {
        final AtomicLong clock=new AtomicLong(100),calls=new AtomicLong();
        long cpu=10,gc=0,gcTime=0;String identity="process";boolean fail,unsupported,badClock;
        @Override public Snapshot sample(){
            calls.incrementAndGet();if(fail){throw new IllegalStateException("fixture source failure");}
            long start=clock.get();Map<String,Value> values=new HashMap<>();
            for(String key:JvmWindowCollector.SOURCE_KEYS){values.put(key,Value.unsupported(key.endsWith("_ns")?"ns":key.equals("gc_count")?"count":"bytes","fixture_unavailable"));}
            values.put("process_cpu_ns",unsupported?Value.unsupported("ns","fixture_unavailable"):Value.measured(cpu,"ns"));
            values.put("gc_count",Value.measured(gc,"count"));values.put("gc_time_ns",Value.measured(gcTime,"ns"));
            values.put("heap_used_bytes",Value.measured(20,"bytes"));values.put("non_heap_used_bytes",Value.measured(30,"bytes"));
            long end=clock.addAndGet(10);return new Snapshot(identity,1,start,badClock?end+100:end,values);
        }
    }
    static LogicalRequest request(long i,TracePhase phase){return new LogicalRequest(phase,i,0,2,i,new Command.Get(Bytes.utf8("key")));}
    static ClientOutcome outcome(LogicalRequest r,long end){return new ClientOutcome(r.id("run","slot"),ClientOutcome.Operation.GET,ClientOutcome.Category.SUCCESS,ClientOutcome.Reason.SUCCESS,
            Result.of(ResultStatus.MISS,RoutingVersion.ZERO,OwnershipEpoch.ZERO),end-1,end,end,1,r.sequence()+1,1,List.of(new ClientOutcome.Attempt(1,end-1,end,ResultStatus.MISS,null)));}
    static TraceGenerator generator(){return new TraceGenerator(new TraceConfig(101,4,24,100,40,30,30,4,TraceConfig.Distribution.UNIFORM,0));}
    static ClosedLoopPlan closed(){return new ClosedLoopPlan("run","slot",2,3,12,0,0,new ClosedLoopPlan.Limits(ms(10_000),ms(1000),ms(500),ms(1000),ms(1000),ms(1),100));}
    static OpenLoopPlan open(){return new OpenLoopPlan("run","slot",3,12,20,20,50,8,ms(1000),0,0,new OpenLoopPlan.Limits(ms(10_000),ms(1000),ms(1000),ms(1000),ms(1)));}
    static DrainProbe ready(){return n->new DrainProbe.Observation(DrainProbe.Status.READY,"model",Map.of("writes",n));}
    static final class Memory implements BenchmarkClient {
        final ClientAccounting ledger=new ClientAccounting();final Map<Bytes,Bytes> values=new ConcurrentHashMap<>();int closes;boolean fail;
        @Override public CompletionStage<ClientOutcome> submit(LogicalRequest r){
            long start=System.nanoTime();var op=ClientOutcome.Operation.of(r.command());ledger.admitted(op);ledger.attempt(op);Result result;
            if(r.command() instanceof Command.Set s){values.put(s.key(),s.value());result=Result.of(ResultStatus.OK,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
            else if(r.command() instanceof Command.Delete){result=Result.of(values.remove(r.command().key())==null?ResultStatus.MISS:ResultStatus.OK,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
            else{var found=values.get(r.command().key());result=found==null?Result.of(ResultStatus.MISS,RoutingVersion.ZERO,OwnershipEpoch.ZERO):Result.ok(found,1,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
            var category=fail?ClientOutcome.Category.NO_RESPONSE:ClientOutcome.Category.SUCCESS;
            ledger.terminal(op,category,1);long end=System.nanoTime();
            return CompletableFuture.completedFuture(new ClientOutcome(r.id("run","slot"),op,category,fail?ClientOutcome.Reason.DEADLINE:ClientOutcome.Reason.SUCCESS,fail?null:result,start,end,end,r.clientId(),r.sequence()+1,1,List.of(new ClientOutcome.Attempt(1,start,end,fail?null:result.status(),null))));
        }
        @Override public ClientAccounting.Snapshot snapshot(){return ledger.snapshot();}
        @Override public boolean cancel(LogicalRequest.RequestId id){return false;}
        @Override public void close(){closes++;}
    }
}
