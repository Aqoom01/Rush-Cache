package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.observation.TcpTraffic.Metric.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.ObservationJson;
import io.openkache.server.benchmark.network.*;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.ionia_eighth.V07BenchmarkClient;
import io.openkache.server.observation.TcpTraffic;
import java.nio.file.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@Timeout(60)
@Execution(ExecutionMode.SAME_THREAD) // One process/configuration subscription at a time.
@SuppressWarnings("try") // Burst assertions inspect the sealed scope before resource cleanup.
class NetworkBenchmarkTcpTest {
    @TempDir Path directory;
    static long ms(long n){return TimeUnit.MILLISECONDS.toNanos(n);}
    @BeforeEach @Timeout(value=620,unit=TimeUnit.SECONDS)
    void gate()throws Exception{VariantPortGate.await("network-observation");}
    static Stream<Arguments> matrix(){List<Arguments> cases=new ArrayList<>();for(String v:CorrectnessBenchmarkTcpTest.VARIANTS)for(int n:new int[]{3,5})for(boolean open:new boolean[]{false,true})cases.add(Arguments.of(v,n,open));return cases.stream();}
    static TraceGenerator generator(){return new TraceGenerator(new TraceConfig(101,4,24,100,40,30,30,4,TraceConfig.Distribution.UNIFORM,0));}
    static ClosedLoopPlan closed(String run){return new ClosedLoopPlan(run,"slot",2,3,12,0,0,new ClosedLoopPlan.Limits(ms(20_000),ms(3000),ms(1000),ms(5000),ms(3000),ms(1),1000));}
    static OpenLoopPlan open(String run){return new OpenLoopPlan(run,"slot",3,12,20,20,50,8,ms(2000),0,0,new OpenLoopPlan.Limits(ms(20_000),ms(4000),ms(5000),ms(3000),ms(1)));}
    static String configuration(CorrectnessCluster g,BenchmarkClient c){return c instanceof VariantBenchmarkClient v?v.configuration():new IoniaConfig(g.cluster.ids.getFirst(),g.cluster.ids.getFirst(),g.cluster.ids,0).configurationId();}
    static Set<Long> members(CorrectnessCluster g){return g.cluster.ids.stream().map(NodeId::value).collect(Collectors.toSet());}
    static Path output(String name)throws Exception{Path parent=Path.of("build","network-observation-fixtures");Files.createDirectories(parent);return parent.resolve(name+"-"+UUID.randomUUID());}
    @ParameterizedTest @MethodSource("matrix")
    void allMethodsPreserveWireAndSeparateLogicalExchangeAndSenderFrames(String variant,int n,boolean open)throws Exception{
        try(var group=new CorrectnessCluster(variant,n,directory)){
            String run="network-"+variant+"-n"+n+"-"+(open?"open":"closed");var raw=group.client(group.settings(run,90_000_000,4));var gen=generator();
            try(var observation=new TrafficRun(run,"slot","network-clock",configuration(group,raw),members(group))){
                Path out=output(run);TrafficWriter.Export export;
                if(open){var p=open(run);var result=new OpenLoopRunner(gen,p,raw,group.probe(raw),observation,observation).run();assertTrue(result.passed(),result.toString());export=TrafficWriter.write(out,gen.config(),p,result,observation);}
                else{var p=closed(run);var result=new ClosedLoopRunner(gen,p,raw,group.probe(raw),observation,observation).run();assertTrue(result.passed(),result.toString());export=TrafficWriter.write(out,gen.config(),p,result,observation);
                    if(variant.equals("O00")&&n==3)assertThrows(FileAlreadyExistsException.class,()->TrafficWriter.write(out,gen.config(),p,result,observation));}
                assertTrue(export.valid(),export.toString());var s=observation.network();assertTrue(s.valid());assertEquals(19,raw.snapshot().success());
                assertTrue(TrafficWriter.sum(s,true,null,null,OK)>19);assertTrue(TrafficWriter.sum(s,true,null,null,SENT_BYTES)>0);
                assertEquals(TrafficWriter.sum(s,false,null,null,CREATED),TrafficWriter.sum(s,false,null,null,CLOSED));assertTrue(s.peakConnections()>0);
                assertTrue(s.rows().stream().anyMatch(r->r.key().frame()&&r.key().from()==0));assertTrue(s.rows().stream().anyMatch(r->r.key().frame()&&r.key().to()==0));
                assertTrue(s.rows().stream().anyMatch(r->!r.key().frame()&&r.key().kind()==1&&!r.key().peerType().isEmpty()));
                Set<Long> seen=s.rows().stream().filter(r->r.key().frame()).map(r->r.key().from()).collect(Collectors.toSet());assertTrue(seen.containsAll(members(group)));assertTrue(seen.contains(0L));
                if(raw instanceof V07BenchmarkClient v){assertEquals(v.batchFrames().dispatched(),TrafficWriter.sum(s,false,0L,7,BEGUN));assertTrue(v.batchFrames().dispatched()>0);}
                System.out.println("NETWORK_EXPORT="+export.directory());
            }
        }
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void omittedLogicalObserverAndWrongConfigurationCannotProduceValidEvidence(boolean wrongConfig)throws Exception{
        try(var group=new CorrectnessCluster("O00",3,directory)){
            String run="network-control-"+wrongConfig;var raw=group.client(group.settings(run,91_000_000,4));var gen=generator();var p=closed(run);
            try(var obs=new TrafficRun(run,"slot","network-clock",configuration(group,raw)+(wrongConfig?"-wrong":""),members(group))){
                ClosedLoopRunner.Observer listener=wrongConfig?obs:(r,d,o)->{};
                var result=new ClosedLoopRunner(gen,p,raw,group.probe(raw),listener,obs).run();assertTrue(result.passed(),result.toString());
                var e=TrafficWriter.write(output(run),gen.config(),p,result,obs);assertFalse(e.valid());assertTrue(e.issues().contains(wrongConfig?"NO_CLIENT_WIRE_OBSERVATIONS":"FINAL_LOGICAL_ACCOUNTING"));System.out.println("NETWORK_CONTROL="+e.directory());
            }
        }
    }
    @ParameterizedTest @ValueSource(ints={3,5})
    void fourLogicalMetaQueriesRemainOneActualBatchFrame(int n)throws Exception{
        try(var cluster=new RemainingTcpCluster(RemainingFixtures.Variant.V07,n,directory)){
            var settings=new O00BenchmarkClient.Settings("network-burst","slot",1,92_000_000,cluster.ids,cluster.ids.getFirst(),cluster.ids.get(1),ms(5000),3,ms(10),16,256,ms(5));
            try(var client=V07BenchmarkClient.open(settings,new IoniaMetaBatchSettings(4,65536,ms(1000),32,2),cluster.addresses,1500,TimeSource.system())){
                var gen=new TraceGenerator(new TraceConfig(101,4,24,100,100,0,0,1,TraceConfig.Distribution.UNIFORM,0));
                for(int i=0;i<4;i++)assertEquals(ClientOutcome.Category.SUCCESS,RemainingFixtures.get(client.submit(gen.at(TracePhase.LOAD,i))).category());
                var probe=client.localProbe(cluster.meta.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,e->e.getValue().engine())));
                RemainingFixtures.await(()->probe.observe(4).status()==DrainProbe.Status.READY);
                try(var scope=TcpTraffic.open(client.configuration(),cluster.ids.stream().map(NodeId::value).collect(Collectors.toSet()),1000)){
                    List<CompletionStage<ClientOutcome>> calls=new ArrayList<>();for(int i=0;i<4;i++)calls.add(client.submit(gen.at(TracePhase.MEASURE,i)));
                    for(var f:calls)assertEquals(ClientOutcome.Category.SUCCESS,RemainingFixtures.get(f).category());assertTrue(scope.awaitIdle(ms(3000)));scope.close();
                    assertEquals(1,client.batchFrames().dispatched());assertEquals(1,TrafficWriter.sum(scope.snapshot(),false,0L,7,BEGUN));assertEquals(1,TrafficWriter.sum(scope.snapshot(),true,0L,7,OK));assertEquals(4,TrafficWriter.sum(scope.snapshot(),false,0L,4,OK));assertTrue(scope.snapshot().valid());
                    Path file=output("burst-n"+n);Files.createDirectory(file);Files.writeString(file.resolve("burst.json"),ObservationJson.encode(Map.of("schema_version","network-burst-v1","network",scope.snapshot(),"logical_queries",4,"raw_batch_frames",1))+"\n");System.out.println("NETWORK_BURST_EXPORT="+file);
                }
            }
        }
    }
    @Test void failedPreflightConnectionRemainsVisibleWithoutInvalidatingHealthyServiceEvidence()throws Exception{
        try(var group=new CorrectnessCluster("O00",3,directory)){
            String run="network-diagnostic";var raw=group.client(group.settings(run,93_000_000,4));var gen=generator();var plan=closed(run);
            try(var obs=new TrafficRun(run,"slot","network-clock",configuration(group,raw),members(group))){
                int port;try(var reservation=new ServerSocket(0)){port=reservation.getLocalPort();}
                try(var failed=new io.openkache.server.ionia.IoniaTcpTransport(new NodeId(0),configuration(group,raw),Map.of(new NodeId(1),new InetSocketAddress("127.0.0.1",port)),200)){
                    assertThrows(ExecutionException.class,()->failed.probe(new NodeId(1)).toCompletableFuture().get(2,TimeUnit.SECONDS));
                }
                var result=new ClosedLoopRunner(gen,plan,raw,group.probe(raw),obs,obs).run();assertTrue(result.passed(),result.toString());
                var export=TrafficWriter.write(output(run),gen.config(),plan,result,obs);assertTrue(export.valid(),export.toString());assertEquals(1,TrafficWriter.sum(obs.network(),false,0L,null,CONNECT_FAILED));
                assertTrue(Files.readString(export.directory().resolve("summary.json")).contains("diagnostic_only_with_transport_failures"));System.out.println("NETWORK_DIAGNOSTIC_EXPORT="+export.directory());
            }
        }
    }
}
