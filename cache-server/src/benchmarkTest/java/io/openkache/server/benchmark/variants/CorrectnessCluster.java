package io.openkache.server.benchmark.variants;

import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.client.BenchmarkClient;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.server.benchmark.ionia.*;
import io.openkache.server.benchmark.ionia_second.*;
import io.openkache.server.ionia.IoniaNode;
import io.openkache.server.ionia_third.IoniaThirdNode;
import io.openkache.server.ionia_fourth.IoniaFourthNode;
import io.openkache.server.ionia_fifth.IoniaFifthNode;
import io.openkache.server.ionia_sixth.IoniaSixthNode;
import io.openkache.server.ionia_eighth.IoniaEighthNode;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** 기존 TCP fixture/실제 Node를 재사용한다. 새 replication 알고리즘을 만들지 않는다. */
final class CorrectnessCluster implements AutoCloseable {
    final String variant;final Path directory;final RemainingTcpCluster cluster;
    CorrectnessCluster(String variant,int n,Path directory)throws Exception{
        this.variant=variant;this.directory=directory;
        cluster=new RemainingTcpCluster(variant.equals("O00")||variant.equals("V01")?RemainingFixtures.Variant.V06:RemainingFixtures.Variant.valueOf(variant),n,directory);
    }
    O00BenchmarkClient.Settings settings(String run,long base,int streams){return RemainingTcpChecks.runtime(cluster.ids,run,base,streams);}
    BenchmarkClient client(O00BenchmarkClient.Settings s){
        if(variant.equals("O00")){return O00TcpClients.open(s,cluster.addresses,1500,TimeSource.system());}
        if(variant.equals("V01")){return V01TcpClients.open(new V01BenchmarkClient.Settings(s.runId(),s.loadSlot(),s.streamCount(),s.wireClientBase(),s.replicas(),s.leader(),s.totalTimeoutNanos(),s.maxRounds(),s.retryBackoffNanos(),s.maxInFlight(),s.maxOutstandingRpc(),s.timerPeriodNanos()),cluster.addresses,1500,TimeSource.system());}
        return cluster.client(s);
    }
    DrainProbe probe(BenchmarkClient c){
        if(c instanceof O00BenchmarkClient o){return new LocalO00DrainProbe(o,engines(),cluster.ids.getFirst(),0);}
        if(c instanceof V01BenchmarkClient v){return new LocalV01DrainProbe(v,engines(),cluster.ids.getFirst(),0);}
        return cluster.probe((VariantBenchmarkClient)c);
    }
    private Map<NodeId,IoniaEngine> engines(){Map<NodeId,IoniaEngine> out=new LinkedHashMap<>();cluster.original.forEach((id,n)->out.put(id,n.engine()));return out;}
    /** 단일 JVM follower의 정상 close 후 같은 파일로 재기동한다. 전원 손실/동시 장애 주장이 아니다. */
    void restartFollower()throws Exception{
        var id=cluster.ids.get(1);var config=new IoniaConfig(id,cluster.ids.getFirst(),cluster.ids,0);var path=directory.resolve("node"+id.value());
        AutoCloseable old=switch(cluster.variant){case V02->cluster.history.get(id);case V03->cluster.batches.get(id);case V04->cluster.applying.get(id);case V05->cluster.workers.get(id);case V06->cluster.original.get(id);case V07->cluster.meta.get(id);};
        old.close();cluster.owners.remove(old);
        switch(cluster.variant){
            case V02->{var n=new IoniaThirdNode(config,cluster.addresses,path,2,10_000,cluster.historyPolicy);cluster.history.put(id,n);cluster.owners.add(n);cluster.progress.put(id,n.engine()::progress);}
            case V03->{var n=new IoniaFourthNode(config,cluster.addresses,path,2,10_000,cluster.batchPolicy);cluster.batches.put(id,n);cluster.owners.add(n);cluster.progress.put(id,n.engine()::progress);}
            case V04->{var n=new IoniaFifthNode(config,cluster.addresses,path,2,10_000,cluster.applyPolicy);cluster.applying.put(id,n);cluster.owners.add(n);cluster.progress.put(id,n.engine()::progress);}
            case V05->{var n=new IoniaSixthNode(config,cluster.addresses,path,10_000,cluster.workerPolicy);cluster.workers.put(id,n);cluster.owners.add(n);cluster.progress.put(id,n.engine()::progress);}
            case V06->{var n=new IoniaNode(config,cluster.addresses,path,2,10_000);cluster.original.put(id,n);cluster.owners.add(n);cluster.progress.put(id,n.engine()::progress);}
            case V07->{var n=new IoniaEighthNode(config,cluster.addresses,path,2,10_000);cluster.meta.put(id,n);cluster.owners.add(n);cluster.progress.put(id,n.engine()::progress);}
        }
        Supplier<IoniaEngine.ViewInfo> view=switch(cluster.variant){case V02->cluster.history.get(id).engine()::viewInfo;case V03->cluster.batches.get(id).engine()::viewInfo;case V04->cluster.applying.get(id).engine()::viewInfo;case V05->cluster.workers.get(id).engine()::viewInfo;case V06->cluster.original.get(id).engine()::viewInfo;case V07->cluster.meta.get(id).engine()::viewInfo;};
        // 정상 상태 전용 probe는 RECOVERING을 실패로 처리한다. 복구 준비 gate를 먼저 통과시킨다.
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while(view.get().status()!=IoniaEngine.Status.NORMAL){if(System.nanoTime()-until>=0){throw new java.util.concurrent.TimeoutException("follower recovery: "+view.get());}Thread.sleep(1);}
    }
    @Override public void close(){cluster.close();}
}
