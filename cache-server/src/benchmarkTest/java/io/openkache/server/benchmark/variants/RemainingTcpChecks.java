package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.benchmark.variants.RemainingFixtures.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.*;
import io.openkache.server.benchmark.ionia.*;
import io.openkache.server.benchmark.ionia_eighth.V07BenchmarkClient;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.provider.Arguments;
import java.util.stream.Stream;

final class RemainingTcpChecks {
    private RemainingTcpChecks() {}
    static long ms(long n){return TimeUnit.MILLISECONDS.toNanos(n);}
    static Stream<Arguments> scenarios(Variant... variants){List<Arguments> args=new ArrayList<>();for(var v:variants){for(int n:new int[]{3,5}){for(boolean open:new boolean[]{true,false}){for(boolean reads:new boolean[]{true,false}){args.add(Arguments.of(v,n,open,reads));}}}}return args.stream();}
    static O00BenchmarkClient.Settings runtime(List<NodeId> ids,String run,long base,int streams){return new O00BenchmarkClient.Settings(run,"slot",streams,base,ids,ids.getFirst(),ids.get(1),ms(3000),3,ms(10),32,256,ms(5));}
    static void run(Variant variant,int count,boolean open,boolean readOnly,Path temporary)throws Exception{
        try(var cluster=new RemainingTcpCluster(variant,count,temporary)){
            var generator=new TraceGenerator(new TraceConfig(101,4,24,100,readOnly?100:40,readOnly?0:30,readOnly?0:30,4,TraceConfig.Distribution.UNIFORM,0));
            var settings=runtime(cluster.ids,"remaining-"+variant,30_000_000,4);
            var client=cluster.client(settings);var probe=cluster.probe(client);
            var collector=new LatencyCollector(settings.runId(),settings.loadSlot(),"remaining-clock",ms(30_000));
            var metadata=client.metadata(readOnly?"local-read":"local-mixed","remaining-clock",null,null,Set.of("M01","M02","M03","M04","M10","M11"));
            Path parent=Path.of("build","remaining-variant-fixtures");Files.createDirectories(parent);
            Path output=parent.resolve(variant+"-n"+count+"-"+(open?"open":"closed")+"-"+(readOnly?"read":"mixed")+"-"+UUID.randomUUID());
            BenchmarkRunWriter.Export exported;long writes;
            if(open){
                var plan=new OpenLoopPlan(settings.runId(),settings.loadSlot(),3,12,20,20,50,8,ms(2000),0,0,
                        new OpenLoopPlan.Limits(ms(20_000),ms(4000),ms(5000),ms(3000),ms(1)));
                var result=new OpenLoopRunner(generator,plan,client,probe,collector).run();assertTrue(result.passed(),result.toString());writes=result.expectedWrites();
                exported=BenchmarkRunWriter.write(output,metadata,generator.config(),plan,result,collector.freeze());
            }else{
                var plan=new ClosedLoopPlan(settings.runId(),settings.loadSlot(),2,3,12,0,0,
                        new ClosedLoopPlan.Limits(ms(20_000),ms(3000),ms(1000),ms(5000),ms(3000),ms(1),1000));
                var result=new ClosedLoopRunner(generator,plan,client,probe,collector).run();assertTrue(result.passed(),result.toString());writes=result.expectedWrites();
                exported=BenchmarkRunWriter.write(output,metadata,generator.config(),plan,result,collector.freeze());
            }
            assertTrue(exported.valid(),exported.toString());assertEquals(19,client.snapshot().success());assertEquals(0,client.snapshot().pending());
            assertEquals(DrainProbe.Status.READY,probe.afterClose(writes).status());assertTrue(client.protocolPending().values().stream().allMatch(n->n==0));
            for(var source:cluster.progress.values()){
                var p=source.get();assertEquals(writes,p.recordedCount());assertEquals(writes,p.lastLogIndex());assertEquals(writes,p.commitIndex());assertEquals(writes,p.appliedIndex());
            }
            for(var op:ClientOutcome.Operation.values()){
                if(readOnly&&op!=ClientOutcome.Operation.GET){continue;}
                var tally=collector.freeze().counts().get(new LatencyCollector.Key(TracePhase.MEASURE,op));assertNotNull(tally);assertTrue(tally.success()>0,op.toString());
                for(var kind:LatencyCollector.Kind.values()){
                    assertEquals(tally.success(),collector.freeze().series().stream().filter(s->s.population().phase()==TracePhase.MEASURE&&s.population().operation()==op&&s.population().kind()==kind).mapToLong(s->s.histogram().count()).sum());
                }
            }
            cluster.assertFeatureIsActive(client);
            assertTrue(client.diagnostics().rpc().byKind().get(ObservedRpc.Kind.READ).dispatched()>0);
            if(client instanceof V07BenchmarkClient){assertEquals(0,client.diagnostics().rpc().byKind().get(ObservedRpc.Kind.META).dispatched());}
            else{assertTrue(client.diagnostics().rpc().byKind().get(ObservedRpc.Kind.META).dispatched()>0);}
            System.out.println("REMAINING_EXPORT_FIXTURE="+output.toAbsolutePath());
            System.out.println("REMAINING_TCP="+variant+", nodes="+count+", mode="+(open?"open":"closed")+", mix="+(readOnly?"100/0/0":"40/30/30")+", phases=4/3/12, applied="+writes+", healthy=19; functional only");
            // 보조 감사는 각 방식의 한 조건에서만 수행하고 본 benchmark cohort에 합치지 않는다.
            if(count==3&&!open&&!readOnly){audit(cluster,variant);}
        }
    }
    static void audit(RemainingTcpCluster cluster,Variant variant)throws Exception{
        var settings=runtime(cluster.ids,"separate-audit-"+variant,40_000_000,1);
        try(var c=cluster.client(settings)){
            Bytes key=Bytes.utf8("separate-audit-key"),value=Bytes.utf8("separate-value");
            assertEquals(ResultStatus.OK,op(c,0,new Command.Set(key,value)).status());assertEquals(value,op(c,1,new Command.Get(key)).value().orElseThrow());
            assertEquals(ResultStatus.OK,op(c,2,new Command.Delete(key)).status());assertEquals(ResultStatus.MISS,op(c,3,new Command.Delete(key)).status());
            assertEquals(ResultStatus.MISS,op(c,4,new Command.Get(key)).status());assertEquals(ResultStatus.OK,op(c,5,new Command.Set(key,value)).status());
            assertEquals(value,op(c,6,new Command.Get(key)).value().orElseThrow());
        }
    }
    static Result op(VariantBenchmarkClient c,long i,Command command)throws Exception{
        var o=c.submit(request(i,command)).toCompletableFuture().get(5,TimeUnit.SECONDS);assertEquals(ClientOutcome.Category.SUCCESS,o.category());return o.result();
    }
}
