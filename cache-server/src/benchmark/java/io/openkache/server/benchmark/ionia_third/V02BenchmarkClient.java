package io.openkache.server.benchmark.ionia_third;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_third.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.variants.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Supplier;

/** V02 History 정책 서버에 원본 Follower+Meta client를 연결한다. 원래 엔진/정책은 보존한다. */
public final class V02BenchmarkClient extends VariantBenchmarkClient {
    private final IoniaHistorySettings policy;
    public V02BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaHistorySettings policy,IoniaClientTransport raw,TimeSource time){this(runtime,policy,raw,time,"unobserved_external_transport");}
    private V02BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaHistorySettings policy,IoniaClientTransport raw,TimeSource time,String timeout){
        super("V02",runtime,policy.configurationId(membership(runtime)),policy("history.target.entries",""+policy.targetEntries(),
                "profile_sha256",policy.profileId(),"socket_timeout_ms",timeout,"read_policy","fixed_replica_original_certification"),raw,time,null,Map::of);
        this.policy=policy;
    }
    public static V02BenchmarkClient open(O00BenchmarkClient.Settings runtime,IoniaHistorySettings policy,Map<NodeId,InetSocketAddress> addresses,int timeout,TimeSource time){
        return VariantTcp.open(runtime,policy.configurationId(membership(runtime)),addresses,timeout,time,false,
                raw->new V02BenchmarkClient(runtime,policy,raw,time,""+timeout));
    }
    public LocalVariantDrainProbe localProbe(Map<NodeId,IoniaThirdEngine> engines){
        Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes=new LinkedHashMap<>();
        engines.forEach((id,engine)->{
            if(engine.historyDiagnostics().history().targetEntries()!=policy.targetEntries()){throw new IllegalArgumentException("History profile mismatch");}
            nodes.put(id,()->new LocalVariantDrainProbe.NodeSnapshot(engine.progress(),engine.viewInfo(),engine.failureReason().orElse(null),0));
        });return probe(nodes);
    }
}
