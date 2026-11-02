package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.benchmark.variants.RemainingFixtures.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.ionia.O00TcpClients;
import io.openkache.server.benchmark.ionia_eighth.V07BenchmarkClient;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@Timeout(60)
class ReadPolicyBenchmarkTcpTest {
    @TempDir Path directory;
    @BeforeAll @Timeout(value=620,unit=TimeUnit.SECONDS)
    static void gate()throws Exception{VariantPortGate.await("read-policy");}
    static Stream<Arguments> scenarios(){return RemainingTcpChecks.scenarios(Variant.V06,Variant.V07);}
    @ParameterizedTest @MethodSource("scenarios")
    void actualAdaptiveAndBatchClientsUseBothSchedulers(Variant variant,int nodes,boolean open,boolean readOnly)throws Exception{
        RemainingTcpChecks.run(variant,nodes,open,readOnly,directory);
    }
    @ParameterizedTest @ValueSource(ints={3,5})
    void realMetaBurstUsesOneFrameForFourDistinctLogicalRequests(int n)throws Exception{
        try(var cluster=new RemainingTcpCluster(Variant.V07,n,directory)){
            var settings=new O00BenchmarkClient.Settings("burst","slot",1,50_000_000,cluster.ids,cluster.ids.getFirst(),cluster.ids.get(1),
                    TimeUnit.SECONDS.toNanos(5),3,10_000_000,16,256,5_000_000);
            try(var client=V07BenchmarkClient.open(settings,new IoniaMetaBatchSettings(4,65536,1_000_000_000,32,2),cluster.addresses,1500,TimeSource.system())){
                var generator=new TraceGenerator(new TraceConfig(101,4,24,100,100,0,0,1,TraceConfig.Distribution.UNIFORM,0));
                for(int i=0;i<4;i++){assertEquals(ClientOutcome.Category.SUCCESS,get(client.submit(generator.at(TracePhase.LOAD,i))).category());}
                var probe=client.localProbe(cluster.meta.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,e->e.getValue().engine())));
                await(()->probe.observe(4).status()==io.openkache.runner.benchmark.execution.DrainProbe.Status.READY);
                List<CompletionStage<ClientOutcome>> stages=new ArrayList<>();for(int i=0;i<4;i++){stages.add(client.submit(generator.at(TracePhase.MEASURE,i)));}
                for(var f:stages){assertEquals(ClientOutcome.Category.SUCCESS,get(f).category());}
                assertEquals(1,client.batchFrames().dispatched());assertEquals(4,client.diagnostics().rpc().byKind().get(io.openkache.server.benchmark.ionia.ObservedRpc.Kind.READ).dispatched());
                assertEquals(4,client.metaDiagnostics().values().stream().mapToLong(d->d.admitted()).sum());
                System.out.println("REMAINING_META_BURST=nodes"+n+", logical4, rawFrames1, freshMeta4");
            }
        }
    }
    @Test void originalWireCannotBeSilentlyUsedAgainstTheMetaBatchProtocol()throws Exception{
        try(var cluster=new RemainingTcpCluster(Variant.V07,3,directory);
                var wrong=O00TcpClients.open(settings(3,1),cluster.addresses,500,TimeSource.system())){
            assertNotEquals(ClientOutcome.Category.SUCCESS,get(wrong.submit(request(0,read()))).category());
            assertTrue(cluster.progress.values().stream().allMatch(p->p.get().recordedCount()==0));
        }
    }
}
