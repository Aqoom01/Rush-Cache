package io.openkache.server.benchmark.variants;

import static io.openkache.server.benchmark.variants.RemainingFixtures.*;
import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.replication.ionia_third.*;
import io.openkache.core.replication.ionia_fourth.*;
import io.openkache.core.replication.ionia_fifth.*;
import io.openkache.core.replication.ionia_sixth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.server.benchmark.ionia_third.V02BenchmarkClient;
import io.openkache.server.benchmark.ionia_fourth.V03BenchmarkClient;
import io.openkache.server.benchmark.ionia_fifth.V04BenchmarkClient;
import io.openkache.server.benchmark.ionia_sixth.V05BenchmarkClient;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

@Timeout(60)
class ServerPolicyBenchmarkTcpTest {
    @TempDir Path directory;
    @BeforeAll @Timeout(value=620,unit=TimeUnit.SECONDS)
    static void gate()throws Exception{VariantPortGate.await("server-policy");}
    static Stream<Arguments> scenarios(){return RemainingTcpChecks.scenarios(Variant.V02,Variant.V03,Variant.V04,Variant.V05);}
    @ParameterizedTest @MethodSource("scenarios")
    void realProfileEnginesUseBothSchedulersAndStandardExport(Variant variant,int nodes,boolean open,boolean readOnly)throws Exception{
        RemainingTcpChecks.run(variant,nodes,open,readOnly,directory);
    }
    @ParameterizedTest @EnumSource(value=Variant.class,names={"V02","V03","V04","V05"})
    void incompatiblePolicyIsRejectedByWireAndLocalProbeBeforeAnyMutation(Variant variant)throws Exception{
        try(var cluster=new RemainingTcpCluster(variant,3,directory)){
            var s=settings(3,1);VariantBenchmarkClient wrong=switch(variant){
                case V02->V02BenchmarkClient.open(s,new IoniaHistorySettings(9),cluster.addresses,500,TimeSource.system());
                case V03->V03BenchmarkClient.open(s,new IoniaBatchSettings(3,4096,20_000_000),cluster.addresses,500,TimeSource.system());
                case V04->V04BenchmarkClient.open(s,new IoniaApplySettings(3,2),cluster.addresses,500,TimeSource.system());
                case V05->V05BenchmarkClient.open(s,new IoniaWorkerSettings(4,1),cluster.addresses,500,TimeSource.system());
                default->throw new IllegalArgumentException("not a server policy");
            };
            try(wrong){assertThrows(IllegalArgumentException.class,()->cluster.probe(wrong));assertNotEquals(ClientOutcome.Category.SUCCESS,get(wrong.submit(request(0,read()))).category());}
            assertTrue(cluster.progress.values().stream().allMatch(p->p.get().recordedCount()==0));
        }
    }
}
