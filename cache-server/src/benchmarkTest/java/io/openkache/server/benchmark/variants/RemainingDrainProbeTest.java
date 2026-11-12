package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.benchmark.variants.RemainingFixtures.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.server.benchmark.ionia_eighth.V07BenchmarkClient;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class RemainingDrainProbeTest {
    private static Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes(long frontier,AtomicLong pending){
        Map<NodeId,Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes=new LinkedHashMap<>();for(var id:ids(3)){
            nodes.put(id,()->new LocalVariantDrainProbe.NodeSnapshot(new IoniaEngine.Progress(id,(int)frontier,frontier,frontier,frontier),
                    new IoniaEngine.ViewInfo(0,new NodeId(1),IoniaEngine.Status.NORMAL),null,pending.get()));
        }return nodes;
    }
    @Test void membershipUsesClientConfigurationAndDoesNotSilentlyAcceptAMissingReplica(){
        try(var client=create(Variant.V02,settings(3,1),new Fake(),new Clock())){
            var nodes=nodes(0,new AtomicLong());nodes.remove(new NodeId(3));
            var probe=new LocalVariantDrainProbe(client,nodes,new NodeId(1),0);assertEquals(DrainProbe.Status.FAILED,probe.observe(0).status());
        }
    }
    @Test void pendingNodeWorkIsWaitAndExpectedFrontiersAreExact(){
        try(var client=create(Variant.V03,settings(3,1),new Fake(),new Clock())){
            var pending=new AtomicLong(1);var probe=new LocalVariantDrainProbe(client,nodes(1,pending),new NodeId(1),0);
            assertEquals(DrainProbe.Status.WAIT,probe.observe(1).status());pending.set(0);assertEquals(DrainProbe.Status.READY,probe.observe(1).status());
            assertEquals(DrainProbe.Status.FAILED,probe.observe(0).status());assertEquals(DrainProbe.Status.WAIT,probe.observe(2).status());
            assertThrows(IllegalArgumentException.class,()->probe.observe(-1));
        }
    }
    @Test void wrongViewOrNodeIdentityCannotBeReady(){
        try(var client=create(Variant.V04,settings(3,1),new Fake(),new Clock())){
            var nodes=nodes(0,new AtomicLong());nodes.put(new NodeId(1),()->new LocalVariantDrainProbe.NodeSnapshot(new IoniaEngine.Progress(new NodeId(2),0,0,0,0),
                    new IoniaEngine.ViewInfo(1,new NodeId(2),IoniaEngine.Status.NORMAL),null,0));
            assertEquals(DrainProbe.Status.FAILED,new LocalVariantDrainProbe(client,nodes,new NodeId(1),0).observe(0).status());
        }
    }
    @Test void afterCloseStillWaitsForUncooperativeRawMetaBatch()throws Exception{
        Fake raw=new Fake();Clock clock=new Clock();var held=new CompletableFuture<List<IoniaMetaBatchTransport.Answer>>();raw.batch=(to,r)->held;
        var client=new V07BenchmarkClient(settings(3,1),new IoniaMetaBatchSettings(1,65536,0,16,1),raw,clock);
        var probe=new LocalVariantDrainProbe(client,nodes(0,new AtomicLong()),new NodeId(1),0);
        var stage=client.submit(request(0,read()));clock.now.addAndGet(TimeUnit.SECONDS.toNanos(2));get(stage);client.close();
        assertEquals(DrainProbe.Status.WAIT,probe.afterClose(0).status());assertEquals(1L,probe.afterClose(0).evidence().get("protocol_batch_raw"));
        held.completeExceptionally(new IllegalStateException("done"));await(()->probe.afterClose(0).status()==DrainProbe.Status.READY);
        assertEquals(DrainProbe.Status.FAILED,probe.observe(0).status());
    }
}
