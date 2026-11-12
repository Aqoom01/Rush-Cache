package io.openkache.server.ionia_third;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import java.net.BindException;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** A port allocation failure is UNAVAILABLE, not an OK/MISS read with a corrupt value. */
class IoniaReadTransportFailureTest {
    private static final List<NodeId> IDS=List.of(new NodeId(1),new NodeId(2),new NodeId(3));
    private static final ClientId CLIENT=new ClientId(10);
    private static final Bytes VALUE=Bytes.utf8("v32");
    private static final TimeSource CLOCK=new TimeSource(){
        @Override public long monotonicNanos(){return 0;}
        @Override public long epochMillis(){return 0;}
    };
    private static RequestContext context(){return new RequestContext(CLIENT,new OperationId(40),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(Long.MAX_VALUE));}
    private static Result ok(){return Result.ok(VALUE,32,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
    private static Result get(FaultTransport transport){
        try(var client=new IoniaClient(CLIENT,IDS,IDS.getFirst(),0,transport,CLOCK,new IoniaClient.Options(IDS.getLast(),true,50_000_000))){
            return client.execute(new Command.Get(Bytes.utf8("hot-key")),context()).toCompletableFuture().join();
        }
    }
    @Test void metaPortAllocationFailureCannotReturnTheAvailableFollowerValue(){
        var transport=new FaultTransport(true,false,false);Result value=get(transport);
        assertEquals(ResultStatus.UNAVAILABLE,value.status());assertEquals(0,value.valueVersion());assertTrue(value.value().isEmpty());
        assertEquals(0,transport.leaderCalls);
    }
    @Test void twoFailedViewProbesFenceAnOtherwiseFreshRead(){
        var transport=new FaultTransport(false,true,false);Result value=get(transport);
        assertEquals(ResultStatus.UNAVAILABLE,value.status());assertEquals(0,value.valueVersion());assertTrue(value.value().isEmpty());
        assertEquals(0,transport.leaderCalls);
    }
    @Test void failedFollowerCanStillUseExistingCertifiedLeaderFallback(){
        var transport=new FaultTransport(false,false,true);Result value=get(transport);
        assertEquals(ResultStatus.OK,value.status());assertEquals(32,value.valueVersion());assertEquals(VALUE,value.value().orElseThrow());
        assertEquals(1,transport.leaderCalls);
    }
    private static final class FaultTransport implements IoniaClientTransport {
        private final boolean failMeta,failProbes,failFollower;
        private int leaderCalls;
        FaultTransport(boolean meta,boolean probes,boolean follower){failMeta=meta;failProbes=probes;failFollower=follower;}
        private static <T> CompletionStage<T> unavailable(){return CompletableFuture.failedFuture(new BindException("Address already in use: getsockopt"));}
        @Override public CompletionStage<WriteAck> record(NodeId to,Request request){throw new AssertionError("GET must not record");}
        @Override public CompletionStage<Reply> execute(NodeId to,Request request){
            assertEquals(IDS.getFirst(),to);leaderCalls++;
            return CompletableFuture.completedFuture(new Reply(to,0,request.clientId(),request.operationId(),ok()));
        }
        @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request request){
            return failFollower?unavailable():CompletableFuture.completedFuture(new ReadReply(to,0,request.clientId(),request.operationId(),32,ok()));
        }
        @Override public CompletionStage<MetaReply> metaQuery(NodeId to,Request request){
            return failMeta?unavailable():CompletableFuture.completedFuture(new MetaReply(to,0,request.clientId(),request.operationId(),32,null));
        }
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){
            return failProbes&&!to.equals(IDS.getFirst())?unavailable():CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(0,IDS.getFirst(),IoniaEngine.Status.NORMAL));
        }
    }
}
