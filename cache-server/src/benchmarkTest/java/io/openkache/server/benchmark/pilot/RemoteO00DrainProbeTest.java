package io.openkache.server.benchmark.pilot;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.runner.benchmark.client.ClientAccounting;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.server.benchmark.ionia.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RemoteO00DrainProbeTest {
    private static final List<NodeId> NODES=List.of(new NodeId(1),new NodeId(2),new NodeId(3));
    private static O00BenchmarkClient.Diagnostics diagnostics(boolean closed,int active){return new O00BenchmarkClient.Diagnostics(new ClientAccounting().snapshot(),new ObservedRpc.Snapshot(Map.of(),0,0,closed),0,active,0,0,closed,null);}
    private static final class Fake implements IoniaClientTransport {
        long applied,view; boolean error,badIdentity,blocked; int calls;
        final List<Runnable> completions=new ArrayList<>();
        public CompletionStage<WriteAck> record(NodeId to,Request r){throw new UnsupportedOperationException();}
        public CompletionStage<Reply> execute(NodeId to,Request r){throw new UnsupportedOperationException();}
        public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){calls++;return CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(view,new NodeId(1),IoniaEngine.Status.NORMAL));}
        public CompletionStage<ReadReply> followerRead(NodeId to,Request r){
            calls++; if(error)return CompletableFuture.failedFuture(new IllegalStateException("offline"));
            var reply=new ReadReply(badIdentity?new NodeId(9):to,0,r.clientId(),r.operationId(),applied,Result.of(ResultStatus.MISS,RoutingVersion.ZERO,OwnershipEpoch.ZERO));
            var result=new CompletableFuture<ReadReply>(); if(blocked)completions.add(()->result.complete(reply));else result.complete(reply);return result;
        }
    }
    private static RemoteO00DrainProbe probe(Fake f){return new RemoteO00DrainProbe(f,()->diagnostics(false,0),NODES,NODES.getFirst(),"test");}
    @Test void readyRequiresNewMatchingRemoteSampleAndClientIdle(){
        Fake f=new Fake();try(var p=probe(f)){
            assertEquals(DrainProbe.Status.WAIT,p.observe(0).status());
            assertEquals(DrainProbe.Status.READY,p.observe(0).status());assertEquals(6,f.calls);
            assertEquals(DrainProbe.Status.WAIT,p.observe(1).status()); // old frontier cannot satisfy a new phase
            assertEquals(DrainProbe.Status.WAIT,p.observe(1).status());
        }
        try(var p=new RemoteO00DrainProbe(new Fake(),()->diagnostics(false,1),NODES,NODES.getFirst(),"busy")){
            p.observe(0);assertEquals(DrainProbe.Status.WAIT,p.observe(0).status());
        }
    }
    @Test void missingReplicaAndLagNeverBecomeReady(){
        Fake f=new Fake();f.error=true;try(var p=probe(f)){p.observe(0);assertEquals(DrainProbe.Status.WAIT,p.observe(0).status());}
        try(var p=probe(new Fake())){p.observe(1);assertEquals(DrainProbe.Status.WAIT,p.observe(1).status());}
    }
    @Test void extraWritesViewChangeAndIdentityMismatchFail(){
        for(int mode=0;mode<3;mode++){Fake f=new Fake();f.applied=mode==0?1:0;f.view=mode==1?1:0;f.badIdentity=mode==2;
            try(var p=probe(f)){p.observe(0);assertEquals(DrainProbe.Status.FAILED,p.observe(0).status());}
        }
    }
    @Test void observeDoesNotBlockOrAccumulateRpcBatches(){
        Fake f=new Fake();f.blocked=true;try(var p=probe(f)){
            for(int i=0;i<100;i++)assertEquals(DrainProbe.Status.WAIT,p.observe(i%2).status());assertEquals(6,f.calls);
            f.completions.forEach(Runnable::run);assertEquals(DrainProbe.Status.READY,p.observe(0).status());
        }
    }
    @Test void closedClientOnlyAllowedForAfterClose(){
        try(var p=new RemoteO00DrainProbe(new Fake(),()->diagnostics(true,0),NODES,NODES.getFirst(),"closed")){
            assertEquals(DrainProbe.Status.FAILED,p.observe(0).status());assertEquals(DrainProbe.Status.READY,p.afterClose(0).status());
        }
    }
}
