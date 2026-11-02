package io.openkache.server.benchmark.ionia_sixth;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_sixth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.variants.*;
import io.openkache.server.ionia_sixth.IoniaSixthNode;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Supplier;

/** V05 실제 I/O worker W 서버에 연결한다. 파일 shard S와 전용 worker W를 별도 기록한다. */
public final class V05BenchmarkClient extends VariantBenchmarkClient {
    private final IoniaWorkerSettings policy;
    public V05BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaWorkerSettings policy,IoniaClientTransport raw,TimeSource time){this(runtime,policy,raw,time,"unobserved_external_transport");}
    private V05BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaWorkerSettings policy,IoniaClientTransport raw,TimeSource time,String timeout){
        super("V05",runtime,policy.configurationId(membership(runtime)),policy("storage.shards",""+policy.storageShards(),
                "storage.io.workers",""+policy.ioWorkers(),"profile_sha256",policy.profileId(),"socket_timeout_ms",timeout,
                "read_policy","fixed_replica_original_certification"),raw,time,null,Map::of);this.policy=policy;
    }
    public static V05BenchmarkClient open(O00BenchmarkClient.Settings runtime,IoniaWorkerSettings policy,Map<NodeId,InetSocketAddress> addresses,int timeout,TimeSource time){
        return VariantTcp.open(runtime,policy.configurationId(membership(runtime)),addresses,timeout,time,false,raw->new V05BenchmarkClient(runtime,policy,raw,time,""+timeout));
    }
    public LocalVariantDrainProbe localProbe(Map<NodeId,IoniaSixthNode> replicas){
        Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes=new LinkedHashMap<>();
        replicas.forEach((id,node)->{
            var d=node.storageDiagnostics();if(d.storageShards()!=policy.storageShards()||d.ioWorkers()!=policy.ioWorkers()){throw new IllegalArgumentException("Worker profile mismatch");}
            nodes.put(id,()->{var e=node.engine();var io=node.storageDiagnostics();return new LocalVariantDrainProbe.NodeSnapshot(e.progress(),e.viewInfo(),e.failureReason().orElse(null),
                    io.activeTasks()+io.workers().stream().mapToLong(w->w.queuedEstimate()).sum());});
        });return probe(nodes);
    }
}
