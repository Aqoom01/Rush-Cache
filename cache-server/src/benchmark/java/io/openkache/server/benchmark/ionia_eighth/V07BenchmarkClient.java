package io.openkache.server.benchmark.ionia_eighth;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.variants.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** V07 실제 Meta 배치 client와 별도 batch wire를 연결한다. 일반 Meta/READ/쓰기 batching으로 치환하지 않는다. */
public final class V07BenchmarkClient extends VariantBenchmarkClient {
    private final Map<Long,IoniaMetaBatchClient> clients;
    private final ObservedMetaBatchFrames frames;
    public V07BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaMetaBatchSettings policy,IoniaClientTransport raw,TimeSource time){
        this(runtime,policy,new ObservedMetaBatchFrames(raw,runtime.maxOutstandingRpc()),time,"unobserved_external_transport",new ConcurrentHashMap<>());
    }
    private V07BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaMetaBatchSettings p,ObservedMetaBatchFrames raw,TimeSource time,String timeout,Map<Long,IoniaMetaBatchClient> clients){
        super("V07",runtime,IoniaMetaBatchSettings.configuration(membership(runtime).configurationId()),policy("meta.batch.entries",""+p.maxEntries(),
                "meta.batch.bytes",""+p.maxBytes(),"meta.batch.delay.ns",""+p.maxDelayNanos(),"meta.pending.limit",""+p.pendingLimit(),
                "meta.inflight.limit",""+p.inFlightLimit(),"profile_sha256",p.profileId(),"socket_timeout_ms",timeout,
                "batch_rpc_limit",""+runtime.maxOutstandingRpc(),"rpc_limits","normal_and_batch_separate",
                "meta_protocol",IoniaMetaBatchSettings.PROTOCOL,"read_policy","original_first_follower",
                "fixed_meta_read_replica",""+runtime.replicas().stream().filter(n->!n.equals(runtime.leader())).findFirst().orElseThrow().value(),
                "runtime_read_replica_ignored","true"),raw,time,(id,observed,clock)->{
                    var client=new IoniaMetaBatchClient(id,runtime.replicas(),runtime.leader(),0,raw.through(observed),clock,p);clients.put(id.value(),client);
                    return VariantSessions.connect(client::refreshView,client::execute,client::onTimer,client::close);
                },()->pending(clients,raw));this.clients=clients;frames=raw;
    }
    private static Map<String,Long> pending(Map<Long,IoniaMetaBatchClient> clients,ObservedMetaBatchFrames raw){
        long queued=0,outstanding=0,inFlight=0;for(var c:clients.values()){var d=c.diagnostics();queued+=d.queued();outstanding+=d.outstanding();inFlight+=d.inFlight();}
        return Map.of("meta_queued",queued,"meta_outstanding",outstanding,"meta_inflight",inFlight,"batch_raw",(long)raw.snapshot().outstanding());
    }
    public ObservedMetaBatchFrames.Snapshot batchFrames(){return frames.snapshot();}
    public Map<Long,IoniaBatchedMetaTransport.Diagnostics> metaDiagnostics(){Map<Long,IoniaBatchedMetaTransport.Diagnostics> out=new TreeMap<>();clients.forEach((id,c)->out.put(id,c.diagnostics()));return Map.copyOf(out);}
    public static V07BenchmarkClient open(O00BenchmarkClient.Settings runtime,IoniaMetaBatchSettings policy,Map<NodeId,InetSocketAddress> addresses,int timeout,TimeSource time){
        return VariantTcp.open(runtime,IoniaMetaBatchSettings.configuration(membership(runtime).configurationId()),addresses,timeout,time,true,
                raw->new V07BenchmarkClient(runtime,policy,new ObservedMetaBatchFrames(raw,runtime.maxOutstandingRpc()),time,""+timeout,new ConcurrentHashMap<>()));
    }
    public LocalVariantDrainProbe localProbe(Map<NodeId,IoniaEngine> engines){
        Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes=new LinkedHashMap<>();
        engines.forEach((id,e)->nodes.put(id,()->new LocalVariantDrainProbe.NodeSnapshot(e.progress(),e.viewInfo(),e.failureReason().orElse(null),0)));return probe(nodes);
    }
}
