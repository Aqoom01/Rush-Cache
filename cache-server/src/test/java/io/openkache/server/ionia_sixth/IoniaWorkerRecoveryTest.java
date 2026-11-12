package io.openkache.server.ionia_sixth;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.ionia_sixth.IoniaWorkerStorageTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_sixth.IoniaWorkerSettings;
import io.openkache.core.runtime.*;
import io.openkache.core.spi.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class IoniaWorkerRecoveryTest {
    @TempDir Path directory;
    static void await(BooleanSupplier condition)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(!condition.getAsBoolean()&&System.nanoTime()-end<0)Thread.sleep(2);assertTrue(condition.getAsBoolean());}
    @Test void V04B_10_originalEngineWaitsForRealOldIoBeforeResetAndReplay()throws Exception{
        List<NodeId> ids=List.of(new NodeId(1),new NodeId(2),new NodeId(3));NodeId b=ids.get(1),c=ids.getLast();
        Map<NodeId,IoniaEngine> engines=new ConcurrentHashMap<>();Map<NodeId,IoniaWorkerFileStorage> stores=new HashMap<>();List<OwnerWorker> owners=new ArrayList<>();
        CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);AtomicBoolean first=new AtomicBoolean(true),released=new AtomicBoolean();AtomicInteger resetCalls=new AtomicInteger();
        TimeSource time=TimeSource.system();Transport peers=envelope->{engines.get(envelope.to()).onPeerMessage(envelope.from(),envelope.message());return CompletableFuture.completedFuture(null);};
        IoniaClientTransport clients=new IoniaClientTransport(){
            public CompletionStage<WriteAck> record(NodeId to,Request request){return engines.get(to).recordWrite(request);}
            public CompletionStage<Reply> execute(NodeId to,Request request){return engines.get(to).execute(request);}
            public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){return CompletableFuture.completedFuture(engines.get(to).viewInfo());}
        };
        try{
            for(NodeId id:ids){
                var storage=new IoniaWorkerFileStorage(id,directory.resolve("n"+id.value()),new IoniaWorkerSettings(4,2),time,false,(shard,reset)->{
                    if(id.equals(b)){
                        if(reset){if(!released.get())throw new IOException("reset raced old write");resetCalls.incrementAndGet();}
                        else if(first.compareAndSet(true,false)){entered.countDown();if(!release.await(8,TimeUnit.SECONDS))throw new IOException("fixture timeout");}
                    }
                });stores.put(id,storage);var owner=new OwnerWorker(new WorkerId(0),65536);owners.add(owner);
                engines.put(id,new IoniaEngine(new IoniaConfig(id,ids.getFirst(),ids,0),storage,owner,peers,time,IoniaViewStore.simulation(),false,0,Long.MAX_VALUE));
            }
            try(var client=new IoniaClient(new ClientId(9),ids,ids.getFirst(),0,clients,time)){
                Bytes key=keyFor(0,4);assertEquals(ResultStatus.OK,client.execute(new Command.Set(key,Bytes.utf8("latest")),context(1)).toCompletableFuture().get(5,TimeUnit.SECONDS).status());
                assertTrue(entered.await(5,TimeUnit.SECONDS));engines.get(b).startViewChange(1);engines.get(c).startViewChange(1);
                await(()->engines.get(b).viewInfo().view()==1&&engines.get(b).viewInfo().status()==IoniaEngine.Status.RECOVERING);assertEquals(0,resetCalls.get());
                released.set(true);release.countDown();await(()->engines.get(b).viewInfo().status()==IoniaEngine.Status.NORMAL);
                assertEquals(4,resetCalls.get());assertEquals(1,engines.get(b).progress().appliedIndex());
                assertEquals(Bytes.utf8("latest"),run(stores.get(b),new Command.Get(key),10).value().orElseThrow());
                for(var engine:engines.values())assertTrue(engine.failureReason().isEmpty(),engine.failureReason().toString());
            }
        }finally{released.set(true);release.countDown();engines.values().forEach(IoniaEngine::close);owners.forEach(OwnerWorker::close);stores.values().forEach(IoniaWorkerFileStorage::close);}
    }
}
