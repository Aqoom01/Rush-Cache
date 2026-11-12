package io.openkache.server.benchmark.ionia_fifth;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_fifth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.variants.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Supplier;

/** V04 applying-key K 서버를 사용한다. 고정 저장 배치 S와 K를 혼합하지 않는다. */
public final class V04BenchmarkClient extends VariantBenchmarkClient {
    private final IoniaApplySettings policy;
    public V04BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaApplySettings policy,IoniaClientTransport raw,TimeSource time){this(runtime,policy,raw,time,"unobserved_external_transport");}
    private V04BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaApplySettings policy,IoniaClientTransport raw,TimeSource time,String timeout){
        super("V04",runtime,policy.configurationId(membership(runtime)),policy("apply.max.in.flight.keys",""+policy.maxInFlightKeys(),
                "storage.shards",""+policy.storageShards(),"profile_sha256",policy.profileId(),"socket_timeout_ms",timeout,
                "read_policy","fixed_replica_original_certification"),raw,time,null,Map::of);this.policy=policy;
    }
    public static V04BenchmarkClient open(O00BenchmarkClient.Settings runtime,IoniaApplySettings policy,Map<NodeId,InetSocketAddress> addresses,int timeout,TimeSource time){
        return VariantTcp.open(runtime,policy.configurationId(membership(runtime)),addresses,timeout,time,false,raw->new V04BenchmarkClient(runtime,policy,raw,time,""+timeout));
    }
    public LocalVariantDrainProbe localProbe(Map<NodeId,IoniaFifthEngine> engines){
        Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes=new LinkedHashMap<>();
        engines.forEach((id,engine)->{
            if(!engine.applyDiagnostics().settings().equals(policy)){throw new IllegalArgumentException("Apply profile mismatch");}
            nodes.put(id,()->new LocalVariantDrainProbe.NodeSnapshot(engine.progress(),engine.viewInfo(),engine.failureReason().orElse(null),
                    (long)engine.applyDiagnostics().gate().activeKeys()+engine.applyDiagnostics().gate().readyKeys()));
        });return probe(nodes);
    }
}
