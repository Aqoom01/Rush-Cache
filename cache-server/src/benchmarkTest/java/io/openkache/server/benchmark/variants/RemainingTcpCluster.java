package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.benchmark.variants.RemainingFixtures.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_third.*;
import io.openkache.core.replication.ionia_fourth.*;
import io.openkache.core.replication.ionia_fifth.*;
import io.openkache.core.replication.ionia_sixth.*;
import io.openkache.core.replication.ionia_seventh.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.benchmark.ionia_third.V02BenchmarkClient;
import io.openkache.server.benchmark.ionia_fourth.V03BenchmarkClient;
import io.openkache.server.benchmark.ionia_fifth.V04BenchmarkClient;
import io.openkache.server.benchmark.ionia_sixth.V05BenchmarkClient;
import io.openkache.server.benchmark.ionia_seventh.V06BenchmarkClient;
import io.openkache.server.benchmark.ionia_eighth.V07BenchmarkClient;
import io.openkache.server.ionia.IoniaNode;
import io.openkache.server.ionia_third.IoniaThirdNode;
import io.openkache.server.ionia_fourth.IoniaFourthNode;
import io.openkache.server.ionia_fifth.IoniaFifthNode;
import io.openkache.server.ionia_sixth.IoniaSixthNode;
import io.openkache.server.ionia_eighth.IoniaEighthNode;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;

/** 각 fixture는 기존 Node 조립부와 새 임시 저장소만 사용한다. 알고리즘 구현을 복제하지 않는다. */
final class RemainingTcpCluster implements AutoCloseable {
    final Variant variant;
    final List<NodeId> ids;
    final Map<NodeId,InetSocketAddress> addresses=new LinkedHashMap<>();
    final Map<NodeId,Supplier<IoniaEngine.Progress>> progress=new LinkedHashMap<>();
    final Map<NodeId,IoniaThirdNode> history=new LinkedHashMap<>();
    final Map<NodeId,IoniaFourthNode> batches=new LinkedHashMap<>();
    final Map<NodeId,IoniaFifthNode> applying=new LinkedHashMap<>();
    final Map<NodeId,IoniaSixthNode> workers=new LinkedHashMap<>();
    final Map<NodeId,IoniaNode> original=new LinkedHashMap<>();
    final Map<NodeId,IoniaEighthNode> meta=new LinkedHashMap<>();
    final List<AutoCloseable> owners=new ArrayList<>();
    final IoniaHistorySettings historyPolicy=new IoniaHistorySettings(8);
    final IoniaBatchSettings batchPolicy=new IoniaBatchSettings(2,4096,20_000_000);
    final IoniaApplySettings applyPolicy=new IoniaApplySettings(2,2);
    final IoniaWorkerSettings workerPolicy=new IoniaWorkerSettings(4,2);
    final IoniaReadSelectionSettings selectionPolicy=selection(IoniaReadSelectionSettings.Mode.ADAPTIVE);
    final IoniaMetaBatchSettings metaPolicy=new IoniaMetaBatchSettings(4,65536,2_000_000,32,2);
    RemainingTcpCluster(Variant variant,int n,Path directory)throws Exception{
        this.variant=variant;ids=ids(n);List<ServerSocket> reservations=new ArrayList<>();
        try{
            for(var id:ids){var socket=new ServerSocket(0,1,InetAddress.getLoopbackAddress());reservations.add(socket);addresses.put(id,new InetSocketAddress("127.0.0.1",socket.getLocalPort()));}
            for(var socket:reservations){socket.close();}
            for(var id:ids){
                var config=new IoniaConfig(id,ids.getFirst(),ids,0);Path path=directory.resolve("node"+id.value());
                switch(variant){
                    case V02->{var node=new IoniaThirdNode(config,addresses,path,2,10_000,historyPolicy);history.put(id,node);owners.add(node);progress.put(id,node.engine()::progress);}
                    case V03->{var node=new IoniaFourthNode(config,addresses,path,2,10_000,batchPolicy);batches.put(id,node);owners.add(node);progress.put(id,node.engine()::progress);}
                    case V04->{var node=new IoniaFifthNode(config,addresses,path,2,10_000,applyPolicy);applying.put(id,node);owners.add(node);progress.put(id,node.engine()::progress);}
                    case V05->{var node=new IoniaSixthNode(config,addresses,path,10_000,workerPolicy);workers.put(id,node);owners.add(node);progress.put(id,node.engine()::progress);}
                    case V06->{var node=new IoniaNode(config,addresses,path,2,10_000);original.put(id,node);owners.add(node);progress.put(id,node.engine()::progress);}
                    case V07->{var node=new IoniaEighthNode(config,addresses,path,2,10_000);meta.put(id,node);owners.add(node);progress.put(id,node.engine()::progress);}
                }
            }
        }catch(Exception failure){close();throw failure;}finally{for(var socket:reservations){socket.close();}}
    }
    VariantBenchmarkClient client(O00BenchmarkClient.Settings settings){return switch(variant){
        case V02->V02BenchmarkClient.open(settings,historyPolicy,addresses,1500,TimeSource.system());
        case V03->V03BenchmarkClient.open(settings,batchPolicy,addresses,1500,TimeSource.system());
        case V04->V04BenchmarkClient.open(settings,applyPolicy,addresses,1500,TimeSource.system());
        case V05->V05BenchmarkClient.open(settings,workerPolicy,addresses,1500,TimeSource.system());
        case V06->V06BenchmarkClient.open(settings,selectionPolicy,addresses,1500,TimeSource.system());
        case V07->V07BenchmarkClient.open(settings,metaPolicy,addresses,1500,TimeSource.system());
    };}
    DrainProbe probe(VariantBenchmarkClient client){return switch(variant){
        case V02->{Map<NodeId,IoniaThirdEngine> engines=new LinkedHashMap<>();history.forEach((id,n)->engines.put(id,n.engine()));yield ((V02BenchmarkClient)client).localProbe(engines);}
        case V03->{Map<NodeId,IoniaFourthEngine> engines=new LinkedHashMap<>();batches.forEach((id,n)->engines.put(id,n.engine()));yield ((V03BenchmarkClient)client).localProbe(engines);}
        case V04->{Map<NodeId,IoniaFifthEngine> engines=new LinkedHashMap<>();applying.forEach((id,n)->engines.put(id,n.engine()));yield ((V04BenchmarkClient)client).localProbe(engines);}
        case V05->((V05BenchmarkClient)client).localProbe(workers);
        case V06->{Map<NodeId,IoniaEngine> engines=new LinkedHashMap<>();original.forEach((id,n)->engines.put(id,n.engine()));yield ((V06BenchmarkClient)client).localProbe(engines);}
        case V07->{Map<NodeId,IoniaEngine> engines=new LinkedHashMap<>();meta.forEach((id,n)->engines.put(id,n.engine()));yield ((V07BenchmarkClient)client).localProbe(engines);}
    };}
    void assertFeatureIsActive(VariantBenchmarkClient client){
        switch(variant){
            case V02->history.values().forEach(n->assertEquals(8,n.engine().historyDiagnostics().history().targetEntries()));
            case V03->{batches.values().forEach(n->assertEquals(batchPolicy,n.engine().batchDiagnostics().settings()));assertTrue(batches.get(ids.getFirst()).engine().batchDiagnostics().batches().attempts()>0);}
            case V04->applying.values().forEach(n->{var d=n.engine().applyDiagnostics();assertEquals(applyPolicy,d.settings());assertTrue(d.gate().peakActive()>0);assertTrue(d.gate().peakActive()<=2);});
            case V05->workers.values().forEach(n->{var d=n.storageDiagnostics();assertEquals(4,d.storageShards());assertEquals(2,d.ioWorkers());assertEquals(2,d.workers().size());assertTrue(d.workers().stream().mapToLong(w->w.finished()).sum()>0);});
            case V06->{var c=(V06BenchmarkClient)client;assertTrue(c.adaptiveDiagnostics().values().stream().mapToLong(d->d.logicalStarts()).sum()>0);assertTrue(c.adaptiveDiagnostics().values().stream().allMatch(d->d.activeLogical()==0));}
            case V07->{var c=(V07BenchmarkClient)client;assertTrue(c.batchFrames().dispatched()>0);assertEquals(0,c.batchFrames().outstanding());assertTrue(c.metaDiagnostics().values().stream().mapToLong(d->d.admitted()).sum()>0);}
        }
        System.out.println("REMAINING_FEATURE="+variant+", profile="+client.adapterConfiguration().get("profile_sha256")+", nodes="+ids.size()+", active=true");
    }
    @Override public void close(){IllegalStateException failure=null;for(int i=owners.size()-1;i>=0;i--){try{owners.get(i).close();}catch(Exception error){if(failure==null){failure=new IllegalStateException("fixture close",error);}else{failure.addSuppressed(error);}}}if(failure!=null){throw failure;}}
}
