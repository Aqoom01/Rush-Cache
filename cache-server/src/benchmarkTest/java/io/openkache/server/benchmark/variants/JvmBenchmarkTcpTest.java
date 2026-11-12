package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@Timeout(60)
class JvmBenchmarkTcpTest {
    @TempDir Path directory;
    static long ms(long n){return TimeUnit.MILLISECONDS.toNanos(n);}
    @BeforeEach @Timeout(value=620,unit=TimeUnit.SECONDS)
    void gate()throws Exception{VariantPortGate.await("jvm-resource");}
    static Stream<Arguments> scenarios(){List<Arguments> out=new ArrayList<>();for(String v:CorrectnessBenchmarkTcpTest.VARIANTS){for(int n:new int[]{3,5}){for(boolean open:new boolean[]{false,true}){out.add(Arguments.of(v,n,open));}}}return out.stream();}
    @ParameterizedTest @MethodSource("scenarios")
    void allAdaptersExposeOneSharedProcessAndReconciledResourceWindows(String variant,int n,boolean open)throws Exception{
        try(var group=new CorrectnessCluster(variant,n,directory)){
            String run="jvm-"+variant+"-n"+n+"-"+(open?"open":"closed");
            var gen=new TraceGenerator(new TraceConfig(101,4,24,100,40,30,30,4,TraceConfig.Distribution.UNIFORM,0));
            var raw=group.client(group.settings(run,80_000_000,4));var probe=group.probe(raw);
            var jvm=new JvmWindowCollector(JvmWindowCollector.Settings.local(run,"slot","jvm-clock",JvmWindowCollector.Role.SHARED_FIXTURE),JvmSnapshotSource.system(),raw::snapshot);
            var latency=new LatencyCollector(run,"slot","jvm-clock",ms(30_000));
            Path parent=Path.of("build","jvm-tcp-fixtures");Files.createDirectories(parent);Path output=parent.resolve(run+"-"+UUID.randomUUID());JvmRunWriter.Export export;
            if(open){
                var plan=new OpenLoopPlan(run,"slot",3,12,20,20,5,8,ms(2000),0,0,new OpenLoopPlan.Limits(ms(20_000),ms(4000),ms(5000),ms(3000),ms(1)));
                var result=new OpenLoopRunner(gen,plan,raw,probe,new OpenLoopRunner.Observer(){@Override public void outcome(OpenLoopResult.ScheduledOutcome e){jvm.outcome(e);latency.outcome(e);}},jvm).run();assertTrue(result.passed(),result.toString());
                export=JvmRunWriter.write(output,gen.config(),plan,result,jvm.snapshot());
            }else{
                var plan=new ClosedLoopPlan(run,"slot",2,3,12,0,0,new ClosedLoopPlan.Limits(ms(20_000),ms(3000),ms(1000),ms(5000),ms(3000),ms(1),1000));
                var result=new ClosedLoopRunner(gen,plan,raw,probe,(r,d,o)->{jvm.outcome(r,d,o);latency.outcome(r,d,o);},jvm).run();assertTrue(result.passed(),result.toString());
                export=JvmRunWriter.write(output,gen.config(),plan,result,jvm.snapshot());
            }
            var snapshot=jvm.snapshot();assertTrue(export.valid(),export.toString());assertTrue(snapshot.samplerTerminated());assertEquals(19,raw.snapshot().success());assertEquals(19,snapshot.requestWindows().stream().mapToLong(JvmWindowCollector.RequestWindow::outcomes).sum());
            assertEquals(1,snapshot.samples().stream().map(s->s.jvm().processInstanceId()).distinct().count());assertEquals(JvmWindowCollector.Role.SHARED_FIXTURE,snapshot.settings().role());assertEquals(8,snapshot.transitions().size());
            assertTrue(snapshot.samples().stream().allMatch(s->s.jvm().values().get("rss_bytes").value()==null));
            assertEquals(19,latency.freeze().counts().values().stream().mapToLong(LatencyCollector.Tally::observed).sum());
            if(open){assertTrue(snapshot.samples().size()>9);assertTrue(snapshot.requestWindows().stream().anyMatch(w->w.index()>0));}
            System.out.println("JVM_TCP_EXPORT="+export.directory());
            System.out.println("JVM_TCP="+variant+", nodes="+n+", mode="+(open?"open":"closed")+", process1/shared, logical19, samplerTerminated=true; no performance comparison");
        }
    }
}
