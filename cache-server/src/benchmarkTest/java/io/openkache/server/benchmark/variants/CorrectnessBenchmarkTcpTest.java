package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.correctness.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.CorrectnessRunWriter;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@Timeout(60)
class CorrectnessBenchmarkTcpTest {
    @TempDir Path directory;
    static final String CLOCK="local-correctness-clock";
    static final String[] VARIANTS={"O00","V01","V02","V03","V04","V05","V06","V07"};
    static long ms(long n){return TimeUnit.MILLISECONDS.toNanos(n);}
    static HistoryRecorder recorder(String run){return new HistoryRecorder(run,"slot",CLOCK,new HistoryRecorder.Limits(100,1000,1_000_000));}
    static HistoryChecker.Limits limits(){return new HistoryChecker.Limits(24,200_000,ms(2000));}
    @BeforeAll @Timeout(value=620,unit=TimeUnit.SECONDS)
    static void gate()throws Exception{VariantPortGate.await("correctness-history");}
    static Stream<Arguments> runs(){List<Arguments> out=new ArrayList<>();for(var v:VARIANTS){for(int n:new int[]{3,5}){for(boolean open:new boolean[]{false,true}){out.add(Arguments.of(v,n,open));}}}return out.stream();}
    static Stream<Arguments> audits(){List<Arguments> out=new ArrayList<>();for(var v:VARIANTS){for(int n:new int[]{3,5}){out.add(Arguments.of(v,n));}}return out.stream();}
    @ParameterizedTest @MethodSource("runs")
    void allAdaptersRecordBothSchedulersAndCheckTheActualMixedHistory(String variant,int nodes,boolean open)throws Exception{
        try(var group=new CorrectnessCluster(variant,nodes,directory)){
            String run="correctness-"+variant+"-n"+nodes+"-"+(open?"open":"closed");
            var generator=new TraceGenerator(new TraceConfig(101,4,24,100,40,30,30,4,TraceConfig.Distribution.UNIFORM,0));
            var raw=group.client(group.settings(run,60_000_000,4));var probe=group.probe(raw);var history=recorder(run);var client=new RecordingBenchmarkClient(raw,history);
            if(open){
                var p=new OpenLoopPlan(run,"slot",3,12,20,20,50,8,ms(2000),0,0,new OpenLoopPlan.Limits(ms(20_000),ms(4000),ms(5000),ms(3000),ms(1)));
                var result=new OpenLoopRunner(generator,p,client,probe).run();assertTrue(result.passed(),result.toString());
            }else{
                var p=new ClosedLoopPlan(run,"slot",2,3,12,0,0,new ClosedLoopPlan.Limits(ms(20_000),ms(3000),ms(1000),ms(5000),ms(3000),ms(1),1000));
                var result=new ClosedLoopRunner(generator,p,client,probe).run();assertTrue(result.passed(),result.toString());
            }
            var s=history.seal(System.nanoTime());assertEquals(19,s.operations().size());assertEquals(19,raw.snapshot().success());assertTrue(s.collectionComplete(),s.toString());
            var export=CorrectnessRunWriter.write(output(run),s,Map.of(),limits(),null,null);assertEquals(HistoryChecker.Verdict.PASS,export.historyResult().verdict(),export.historyResult().toString());
            System.out.println("CORRECTNESS_EXPORT="+export.directory());
        }
    }
    @ParameterizedTest @MethodSource("audits")
    void reservedAckSetAndDeleteRemainVisibleAfterDrainedFollowerRejoin(String variant,int nodes)throws Exception{
        try(var group=new CorrectnessCluster(variant,nodes,directory)){
            String run="ack-"+variant+"-n"+nodes;Bytes set=Bytes.utf8("reserved/set"),del=Bytes.utf8("reserved/delete"),value=Bytes.utf8("acknowledged-bytes");
            try(var setup=group.client(group.settings(run+"-setup",70_000_000,1))){assertEquals(ResultStatus.OK,op(setup,0,new Command.Set(del,value)).result().status());awaitReady(group.probe(setup),1);}
            var raw=group.client(group.settings(run,71_000_000,1));var history=recorder(run);var client=new RecordingBenchmarkClient(raw,history);
            try(client){
                assertEquals(ResultStatus.OK,op(client,0,new Command.Set(set,value)).result().status());assertEquals(ResultStatus.OK,op(client,1,new Command.Delete(del)).result().status());
                var ready=awaitReady(group.probe(raw),3);var s=history.seal(System.nanoTime());var ledger=AckLedger.build(s,Set.of(set,del),true);assertEquals(2,ledger.entries().size());
                var audit=audit(group,ledger,run+"-before",72_000_000,ready,"all_replicas_applied");assertEquals(HistoryChecker.Verdict.PASS,audit.verdict(),audit.toString());
                var before=CorrectnessRunWriter.write(output(run+"-before-rejoin"),s,Map.of(del,value),limits(),ledger,audit);assertEquals(HistoryChecker.Verdict.PASS,before.historyResult().verdict());System.out.println("CORRECTNESS_EXPORT="+before.directory());
                group.restartFollower();var recovered=awaitReady(group.probe(raw),3);
                var afterAudit=audit(group,ledger,run+"-after",73_000_000,recovered,"drained_follower_close_restart_survivor_recovery");assertEquals(HistoryChecker.Verdict.PASS,afterAudit.verdict(),afterAudit.toString());
                var after=CorrectnessRunWriter.write(output(run+"-after-rejoin"),s,Map.of(del,value),limits(),ledger,afterAudit);System.out.println("CORRECTNESS_EXPORT="+after.directory());
            }
        }
    }
    static AckLedger.Audit audit(CorrectnessCluster group,AckLedger.Ledger ledger,String run,long base,DrainProbe.Observation ready,String label)throws Exception{
        long barrier=System.nanoTime();List<AckLedger.Read> reads=new ArrayList<>();try(var c=group.client(group.settings(run,base,1))){long i=0;for(var e:ledger.entries()){var r=request(i++,new Command.Get(e.key()));reads.add(new AckLedger.Read(r,c.submit(r).toCompletableFuture().get(5,TimeUnit.SECONDS),CLOCK));}awaitReady(group.probe(c),3);}
        return AckLedger.audit(ledger,label,barrier,ready,reads);
    }
    static LogicalRequest request(long i,Command command){return new LogicalRequest(TracePhase.MEASURE,i,0,2,i,command);}
    static ClientOutcome op(BenchmarkClient c,long i,Command command)throws Exception{var o=c.submit(request(i,command)).toCompletableFuture().get(5,TimeUnit.SECONDS);assertEquals(ClientOutcome.Category.SUCCESS,o.category(),o.toString());return o;}
    static DrainProbe.Observation awaitReady(DrainProbe probe,long writes)throws Exception{
        long until=System.nanoTime()+ms(5000);DrainProbe.Observation o;
        while((o=probe.observe(writes)).status()!=DrainProbe.Status.READY){assertNotEquals(DrainProbe.Status.FAILED,o.status(),o.toString());if(System.nanoTime()-until>=0){fail("readiness timeout: "+o);}Thread.sleep(1);}return o;
    }
    static Path output(String run)throws Exception{Path parent=Path.of("build","correctness-fixtures");Files.createDirectories(parent);return parent.resolve(run+"-"+UUID.randomUUID());}
}
