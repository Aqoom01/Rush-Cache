package io.openkache.server.benchmark.ionia_seventh;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_seventh.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.variants.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** V06은 실제 adaptive/RR client를 사용한다. RTT·미완료 raw READ의 선택 의미를 재구현하지 않는다. */
public final class V06BenchmarkClient extends VariantBenchmarkClient {
    private final Map<Long,IoniaAdaptiveReadClient> clients;
    public V06BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaReadSelectionSettings policy,IoniaClientTransport raw,TimeSource time){this(runtime,policy,raw,time,"unobserved_external_transport",new ConcurrentHashMap<>());}
    private V06BenchmarkClient(O00BenchmarkClient.Settings runtime,IoniaReadSelectionSettings p,IoniaClientTransport raw,TimeSource time,String timeout,Map<Long,IoniaAdaptiveReadClient> clients){
        super("V06",runtime,membership(runtime).configurationId(),policy("read.selection",p.mode().name(),"read.logical.limit",""+p.logicalLimit(),
                "read.rpc.limit.per.replica",""+p.rpcLimitPerReplica(),"read.rtt.initial.ms",""+p.initialRttMillis(),
                "read.sample.ttl.ms",""+p.sampleTtlMillis(),"read.failure.cooldown.ms",""+p.cooldownMillis(),
                "read.explore.every",""+p.exploreEvery(),"profile_sha256",p.profileId(),"socket_timeout_ms",timeout,
                "read_policy","actual_adaptive_client","runtime_read_replica_ignored","true"),raw,time,
                (id,observed,clock)->{
                    var client=new IoniaAdaptiveReadClient(id,runtime.replicas(),runtime.leader(),0,observed,clock,p);clients.put(id.value(),client);
                    return VariantSessions.connect(client::refreshView,client::execute,client::onTimer,client::close);
                },()->pending(clients));this.clients=clients;
    }
    private static Map<String,Long> pending(Map<Long,IoniaAdaptiveReadClient> clients){
        long logical=0,raw=0;for(var client:clients.values()){var d=client.diagnostics();logical+=d.activeLogical();raw+=d.nodes().stream().mapToLong(n->n.rawPending()).sum();}
        return Map.of("adaptive_logical",logical,"adaptive_raw_reads",raw);
    }
    public Map<Long,IoniaAdaptiveReadClient.Diagnostics> adaptiveDiagnostics(){Map<Long,IoniaAdaptiveReadClient.Diagnostics> out=new TreeMap<>();clients.forEach((id,c)->out.put(id,c.diagnostics()));return Map.copyOf(out);}
    public static V06BenchmarkClient open(O00BenchmarkClient.Settings runtime,IoniaReadSelectionSettings policy,Map<NodeId,InetSocketAddress> addresses,int timeout,TimeSource time){
        return VariantTcp.open(runtime,membership(runtime).configurationId(),addresses,timeout,time,false,
                raw->new V06BenchmarkClient(runtime,policy,raw,time,""+timeout,new ConcurrentHashMap<>()));
    }
    public LocalVariantDrainProbe localProbe(Map<NodeId,IoniaEngine> engines){
        Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes=new LinkedHashMap<>();
        engines.forEach((id,e)->nodes.put(id,()->new LocalVariantDrainProbe.NodeSnapshot(e.progress(),e.viewInfo(),e.failureReason().orElse(null),0)));return probe(nodes);
    }
}
