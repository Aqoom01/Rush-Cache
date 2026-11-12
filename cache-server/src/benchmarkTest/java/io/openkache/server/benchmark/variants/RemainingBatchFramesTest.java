package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.benchmark.variants.RemainingFixtures.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class RemainingBatchFramesTest {
    private static List<IoniaMetaBatchTransport.Item> items(){return List.of(new IoniaMetaBatchTransport.Item(1,
            new IoniaClientTransport.Request(0,read(),new ClientId(2),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,1_000_000)));}
    @Test void callerCancellationCloseAndCapacityCannotReleaseARawFramePermit()throws Exception{
        Fake raw=new Fake();var held=new CompletableFuture<List<IoniaMetaBatchTransport.Answer>>();raw.batch=(to,r)->held;
        var frames=new ObservedMetaBatchFrames(raw,1);var stage=frames.metaBatch(new NodeId(1),items());stage.toCompletableFuture().cancel(true);
        assertEquals(1,frames.snapshot().outstanding());assertThrows(ExecutionException.class,()->frames.metaBatch(new NodeId(1),items()).toCompletableFuture().get());
        frames.close();frames.close();assertEquals(1,raw.closes.get());assertEquals(1,frames.snapshot().outstanding());
        assertThrows(ExecutionException.class,()->frames.metaBatch(new NodeId(1),items()).toCompletableFuture().get());
        held.complete(List.of());await(()->frames.snapshot().outstanding()==0);
        var d=frames.snapshot();assertEquals(3,d.calls());assertEquals(1,d.dispatched());assertEquals(1,d.completed());assertEquals(2,d.rejected());assertEquals(1,d.peak());
    }
    @Test void throwingOrNullDelegateStagesAreRecordedAndReleaseExactlyOnePermit()throws Exception{
        Fake raw=new Fake();raw.batch=(to,r)->{throw new IllegalStateException("boom");};
        try(var frames=new ObservedMetaBatchFrames(raw,1)){
            assertThrows(ExecutionException.class,()->frames.metaBatch(new NodeId(1),items()).toCompletableFuture().get());
            raw.batch=(to,r)->null;assertThrows(ExecutionException.class,()->frames.metaBatch(new NodeId(1),items()).toCompletableFuture().get());
            assertEquals(0,frames.snapshot().outstanding());assertEquals(2,frames.snapshot().completed());assertEquals(2,frames.snapshot().failed());
        }
    }
    @Test void normalCallsPassThroughRuntimeObservationAndAreNotCountedAsFrames()throws Exception{
        Fake raw=new Fake();try(var frames=new ObservedMetaBatchFrames(raw,1)){
            var through=frames.through(raw);through.probe(new NodeId(1)).toCompletableFuture().get();
            assertEquals(1,raw.probes.get());assertEquals(0,frames.snapshot().calls());assertEquals(0,frames.snapshot().outstanding());
        }
    }
}
