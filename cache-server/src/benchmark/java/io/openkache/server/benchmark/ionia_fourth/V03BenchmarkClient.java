package io.openkache.server.benchmark.ionia_fourth;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_fourth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.variants.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Supplier;

/** V03 Prepare 배치 서버의 정확한 wire profile을 사용한다. client 읽기 정책은 O00다. */
public final class V03BenchmarkClient extends VariantBenchmarkClient {
    private final IoniaBatchSettings policy;
    public V03BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaBatchSettings policy,IoniaClientTransport raw,TimeSource time){this(runtime,policy,raw,time,"unobserved_external_transport");}
    private V03BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaBatchSettings policy,IoniaClientTransport raw,TimeSource time,String timeout){
        super("V03",runtime,policy.configurationId(membership(runtime)),policy("prepare.max.entries",""+policy.maxEntries(),
                "prepare.max.encoded.bytes",""+policy.maxEncodedBytes(),"prepare.max.delay.ns",""+policy.maxDelayNanos(),
                "profile_sha256",policy.profileId(),"socket_timeout_ms",timeout,"read_policy","fixed_replica_original_certification"),raw,time,null,Map::of);this.policy=policy;
    }
    public static V03BenchmarkClient open(O00BenchmarkClient.Settings runtime,IoniaBatchSettings policy,Map<NodeId,InetSocketAddress> addresses,int timeout,TimeSource time){
        return VariantTcp.open(runtime,policy.configurationId(membership(runtime)),addresses,timeout,time,false,raw->new V03BenchmarkClient(runtime,policy,raw,time,""+timeout));
    }
    public LocalVariantDrainProbe localProbe(Map<NodeId,IoniaFourthEngine> engines){
        Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes=new LinkedHashMap<>();
        engines.forEach((id,engine)->{
            if(!engine.batchDiagnostics().settings().equals(policy)){throw new IllegalArgumentException("Prepare profile mismatch");}
            nodes.put(id,()->new LocalVariantDrainProbe.NodeSnapshot(engine.progress(),engine.viewInfo(),engine.failureReason().orElse(null),engine.batchDiagnostics().outstandingSendStages()));
        });return probe(nodes);
    }
}
