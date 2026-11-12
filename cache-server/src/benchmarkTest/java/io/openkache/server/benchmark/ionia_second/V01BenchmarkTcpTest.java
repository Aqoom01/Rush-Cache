package io.openkache.server.benchmark.ionia_second;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.*;
import io.openkache.server.benchmark.ionia.*;
import io.openkache.server.ionia.IoniaNode;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Timeout(60)
class V01BenchmarkTcpTest {
    @TempDir Path directory;
    private static long ms(long n){return TimeUnit.MILLISECONDS.toNanos(n);}
    /** 원래 전체 gate 이후 다른 회귀가 사용한 포트가 남을 수 있어 새 TCP 그룹 진입 전에만 재확인한다. */
    @BeforeAll @Timeout(value=620,unit=TimeUnit.SECONDS)
    static void qualifyWindowsPortBudgetBeforeThisNewGroup() throws Exception {
        if(!System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows")){return;}
        Path evidenceDirectory=Path.of("build","benchmark-v01-port-gates");Files.createDirectories(evidenceDirectory);
        Path evidence=evidenceDirectory.resolve(UUID.randomUUID()+".json").toAbsolutePath();
        Path script=Path.of("..","experiments","ionia-validation","Wait-LocalTcpBudget.ps1").toAbsolutePath();
        Path processLog=evidence.resolveSibling(evidence.getFileName()+".log");
        Process gate=new ProcessBuilder("pwsh.exe","-NoProfile","-NonInteractive","-ExecutionPolicy","Bypass",
                "-File",script.toString(),"-EvidencePath",evidence.toString(),"-WaitSeconds","600")
                .redirectErrorStream(true).redirectOutput(processLog.toFile()).start();
        System.out.println("V01_TCP_GROUP_GATE="+evidence);
        try {
            assertTrue(gate.waitFor(620,TimeUnit.SECONDS),"local TCP gate process did not finish: "+processLog);
            assertEquals(0,gate.exitValue(),new String(Files.readAllBytes(processLog),java.nio.charset.StandardCharsets.UTF_8));
        } finally {if(gate.isAlive()){gate.destroyForcibly();}}
    }
    @ParameterizedTest
    @CsvSource({"3,true,true","5,true,true","3,false,true","5,false,true",
                "3,true,false","5,true,false","3,false,false","5,false,false"})
    void bothSchedulersExportV01AndPreserveActualOriginalReadControl(int count, boolean open, boolean readOnly) throws Exception {
        List<NodeId> ids=new ArrayList<>();Map<NodeId,InetSocketAddress> addresses=new LinkedHashMap<>();
        List<ServerSocket> reservations=new ArrayList<>();Map<NodeId,IoniaNode> nodes=new LinkedHashMap<>();
        try {
            for(int i=1;i<=count;i++){
                var id=new NodeId(i);ids.add(id);var socket=new ServerSocket(0,1,InetAddress.getLoopbackAddress());
                reservations.add(socket);addresses.put(id,new InetSocketAddress("127.0.0.1",socket.getLocalPort()));
            }
            for(var socket:reservations){socket.close();}
            for(var id:ids){nodes.put(id,new IoniaNode(new IoniaConfig(id,ids.getFirst(),ids,0),addresses,directory.resolve("node"+id.value()),2,10_000));}
            var generator=new TraceGenerator(new TraceConfig(101,4,24,100,readOnly?100:40,readOnly?0:30,readOnly?0:30,
                    4,TraceConfig.Distribution.UNIFORM,0));
            var settings=new V01BenchmarkClient.Settings("v01-local","slot",4,8_000_000,ids,ids.getFirst(),ms(3000),3,ms(10),32,256,ms(5));
            var client=V01TcpClients.open(settings,addresses,1500,TimeSource.system());
            Map<NodeId,IoniaEngine> engines=new LinkedHashMap<>();nodes.forEach((id,node)->engines.put(id,node.engine()));
            var probe=new LocalV01DrainProbe(client,engines,ids.getFirst(),0);
            var collector=new LatencyCollector(settings.runId(),settings.loadSlot(),"v01-local-clock",ms(30_000));
            var metadata=new BenchmarkRunWriter.Metadata("V01",readOnly?"local-get100":"local-crud-mixed",count,
                    null,null,"v01-local-clock",Set.of("M01","M02","M03","M04","M10","M11"));
            Path parent=Path.of("build","benchmark-v01-fixtures");Files.createDirectories(parent);
            Path output=parent.resolve("v01-n"+count+"-"+(open?"open":"closed")+"-"+(readOnly?"read":"mixed")+"-"+UUID.randomUUID());
            BenchmarkRunWriter.Export exported;long writes;
            if(open){
                var plan=new OpenLoopPlan(settings.runId(),settings.loadSlot(),3,30,20,20,50,8,ms(2000),0,0,
                        new OpenLoopPlan.Limits(ms(20_000),ms(4000),ms(5000),ms(3000),ms(1)));
                var result=new OpenLoopRunner(generator,plan,client,probe,collector).run();assertTrue(result.passed(),result.toString());writes=result.expectedWrites();
                exported=BenchmarkRunWriter.write(output,metadata,generator.config(),plan,result,collector.freeze());
            } else {
                var plan=new ClosedLoopPlan(settings.runId(),settings.loadSlot(),2,3,30,0,0,
                        new ClosedLoopPlan.Limits(ms(20_000),ms(3000),ms(1000),ms(5000),ms(3000),ms(1),1000));
                var result=new ClosedLoopRunner(generator,plan,client,probe,collector).run();assertTrue(result.passed(),result.toString());writes=result.expectedWrites();
                exported=BenchmarkRunWriter.write(output,metadata,generator.config(),plan,result,collector.freeze());
            }
            assertTrue(exported.valid(),exported.toString());assertTrue(Files.exists(output.resolve("COMPLETE")));
            assertEquals(37,client.snapshot().success());assertEquals(0,client.snapshot().pending());
            var rpc=client.diagnostics().rpc();assertEquals(0,rpc.byKind().get(ObservedRpc.Kind.READ).calls());
            assertEquals(0,rpc.byKind().get(ObservedRpc.Kind.META).calls());assertTrue(rpc.byKind().get(ObservedRpc.Kind.PROBE).dispatched()>0);
            assertEquals(0,rpc.outstanding());assertTrue(client.diagnostics().closed());
            assertEquals(DrainProbe.Status.READY,probe.afterClose(writes).status());
            for(var node:nodes.values()){assertEquals(writes,node.engine().progress().appliedIndex());assertEquals(writes,node.engine().progress().recordedCount());}
            assertEquals(readOnly?4:22,writes);
            for(var op:ClientOutcome.Operation.values()){
                if(readOnly&&op!=ClientOutcome.Operation.GET){continue;}
                var tally=collector.freeze().counts().get(new LatencyCollector.Key(TracePhase.MEASURE,op));assertNotNull(tally);assertTrue(tally.success()>0);
                for(var kind:LatencyCollector.Kind.values()){
                    assertEquals(tally.success(),collector.freeze().series().stream().filter(s->s.population().phase()==TracePhase.MEASURE
                            &&s.population().operation()==op&&s.population().kind()==kind).mapToLong(s->s.histogram().count()).sum());
                }
            }
            // 보조 감사/대조군은 앞선 37요청 benchmark cohort와 별개의 client·ID namespace다.
            var auditSettings=new V01BenchmarkClient.Settings("v01-audit","slot",1,9_000_000,ids,ids.getFirst(),ms(3000),3,ms(10),8,128,ms(5));
            Bytes key=Bytes.utf8("separate-audit-key"),value=Bytes.utf8("audit-value");
            try(var audit=V01TcpClients.open(auditSettings,addresses,1500,TimeSource.system())){
                assertEquals(ResultStatus.OK,audit(audit,0,new Command.Set(key,value)).status());
                assertEquals(value,audit(audit,1,new Command.Get(key)).value().orElseThrow());
                assertEquals(ResultStatus.OK,audit(audit,2,new Command.Delete(key)).status());
                assertEquals(ResultStatus.MISS,audit(audit,3,new Command.Delete(key)).status());
                assertEquals(ResultStatus.MISS,audit(audit,4,new Command.Get(key)).status());
                assertEquals(ResultStatus.OK,audit(audit,5,new Command.Set(key,value)).status());
                assertEquals(value,audit(audit,6,new Command.Get(key)).value().orElseThrow());
            }
            var originalSettings=new O00BenchmarkClient.Settings("o00-control","slot",1,10_000_000,ids,ids.getFirst(),ids.get(1),ms(3000),3,ms(10),8,128,ms(5));
            try(var original=O00TcpClients.open(originalSettings,addresses,1500,TimeSource.system())){
                var outcome=original.submit(new LogicalRequest(TracePhase.MEASURE,0,0,2,0,new Command.Get(key))).toCompletableFuture().get(5,TimeUnit.SECONDS);
                assertEquals(ClientOutcome.Category.SUCCESS,outcome.category());assertEquals(value,outcome.result().value().orElseThrow());
                assertEquals(1,original.diagnostics().rpc().byKind().get(ObservedRpc.Kind.READ).dispatched());
                assertEquals(1,original.diagnostics().rpc().byKind().get(ObservedRpc.Kind.META).dispatched());
            }
            System.out.println("V01_EXPORT_FIXTURE="+output.toAbsolutePath());
            System.out.println("V01 TCP n="+count+", mode="+(open?"open":"closed")+", mix="+(readOnly?"100/0/0":"40/30/30")
                    +", phases4/3/30, applied="+writes+", V01 READ/META0, control READ/META1, probe="+rpc.byKind().get(ObservedRpc.Kind.PROBE).dispatched()+"; functional only");
        } finally {for(var node:nodes.values()){node.close();}for(var socket:reservations){socket.close();}}
    }
    private static Result audit(V01BenchmarkClient client,long index,Command command) throws Exception {
        var outcome=client.submit(new LogicalRequest(TracePhase.MEASURE,index,0,2,index,command)).toCompletableFuture().get(5,TimeUnit.SECONDS);
        assertEquals(ClientOutcome.Category.SUCCESS,outcome.category());return outcome.result();
    }
}
