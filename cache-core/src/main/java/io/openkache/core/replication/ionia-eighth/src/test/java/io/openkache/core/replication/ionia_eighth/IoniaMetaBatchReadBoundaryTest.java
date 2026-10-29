package io.openkache.core.replication.ionia_eighth;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_eighth.IoniaMetaBatchTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

class IoniaMetaBatchReadBoundaryTest {
    static final NodeId D=new NodeId(3);static final List<NodeId> MEMBERS=List.of(A,B,D);
    static class ReadRaw extends Raw {
        long applied,index;int leaderGets;Result data;boolean wrongRead,wrongView;
        final CompletableFuture<ReadReply> read=new CompletableFuture<>();
        @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request r){
            if(!read.isDone())read.complete(new ReadReply(wrongRead?A:to,r.view(),r.clientId(),r.operationId(),applied,value("old")));
            return read;
        }
        @Override public CompletionStage<Reply> execute(NodeId to,Request r){leaderGets++;return CompletableFuture.completedFuture(new Reply(to,r.view(),r.clientId(),r.operationId(),value("fresh")));}
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){return CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(wrongView?1:0,wrongView?B:A,IoniaEngine.Status.NORMAL));}
        void reply(){var c=calls.getFirst();var i=c.items().getFirst();c.future().complete(List.of(new Answer(i.token(),new MetaReply(c.to(),i.request().view(),i.request().clientId(),i.request().operationId(),index,data))));}
    }
    static Result value(String v){return Result.ok(Bytes.utf8(v),1,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
    static RequestContext context(Clock clock,long id){return new RequestContext(C,new OperationId(id),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(clock.now+1000));}
    static IoniaMetaBatchClient client(ReadRaw raw,Clock clock){return new IoniaMetaBatchClient(C,MEMBERS,A,0,raw,clock,settings(1,1000,0,8,2));}
    @Test void capturedPrefixLessEqualAndGreaterThanFreshMetaUseOriginalDecision(){
        for(long applied:new long[]{4,5,6}){ReadRaw raw=new ReadRaw();raw.applied=applied;raw.index=5;Clock clock=new Clock();
            try(var client=client(raw,clock)){var result=client.execute(new Command.Get(Bytes.utf8("k")),context(clock,1));raw.reply();
                assertEquals(Bytes.utf8(applied<5?"fresh":"old"),result.toCompletableFuture().join().value().orElseThrow());assertEquals(applied<5?1:0,raw.leaderGets);}
        }
    }
    @Test void metaDataIncludingMissDoesNotRequireFollowerCompletion(){
        for(Result data:new Result[]{value("data"),Result.of(ResultStatus.MISS,RoutingVersion.ZERO,OwnershipEpoch.ZERO)}){
            ReadRaw raw=new ReadRaw(){@Override public CompletionStage<ReadReply> followerRead(NodeId n,Request r){return new CompletableFuture<>();}};raw.data=data;Clock clock=new Clock();
            try(var client=client(raw,clock)){var result=client.execute(new Command.Get(Bytes.utf8("k")),context(clock,1));raw.reply();assertEquals(data,result.toCompletableFuture().join());assertEquals(0,raw.leaderGets);}
        }
    }
    @Test void wrongFollowerIdentityForcesLeaderFallback(){
        ReadRaw raw=new ReadRaw();raw.wrongRead=true;Clock clock=new Clock();
        try(var client=client(raw,clock)){var result=client.execute(new Command.Get(Bytes.utf8("k")),context(clock,1));raw.reply();assertEquals(Bytes.utf8("fresh"),result.toCompletableFuture().join().value().orElseThrow());assertEquals(1,raw.leaderGets);}
    }
    @Test void oldViewCertificateAndEndpointAdvanceCannotAcceptLateBatch(){
        for(boolean advance:new boolean[]{false,true}){ReadRaw raw=new ReadRaw();raw.wrongView=!advance;Clock clock=new Clock();
            try(var client=client(raw,clock)){var result=client.execute(new Command.Get(Bytes.utf8("k")),context(clock,1));if(advance)client.updateView(1,B);raw.reply();assertEquals(ResultStatus.UNAVAILABLE,result.toCompletableFuture().join().status());}
        }
    }
    @Test void publicCancelAndTimerDoNotReleaseRawBatchSlot(){
        ReadRaw raw=new ReadRaw();Clock clock=new Clock();
        try(var client=client(raw,clock)){var result=client.execute(new Command.Get(Bytes.utf8("k")),context(clock,1));result.toCompletableFuture().cancel(false);
            clock.now=1000;client.onTimer(new TimerEvent(IoniaEngine.TICK,1000));assertEquals(1,client.diagnostics().inFlight());raw.reply();assertEquals(0,client.diagnostics().inFlight());assertTrue(result.toCompletableFuture().isCancelled());}
    }
    @Test void expiredAndWrongClientNeverReachBatchAdmission(){
        ReadRaw raw=new ReadRaw();Clock clock=new Clock();
        try(var client=client(raw,clock)){
            var wrong=new RequestContext(new ClientId(9),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(1000));
            assertEquals(ResultStatus.BAD_REQUEST,client.execute(new Command.Get(Bytes.utf8("k")),wrong).toCompletableFuture().join().status());
            clock.now=2000;assertEquals(ResultStatus.TIMEOUT,client.execute(new Command.Get(Bytes.utf8("k")),new RequestContext(C,new OperationId(2),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(1000))).toCompletableFuture().join().status());assertEquals(0,client.diagnostics().admitted());
        }
    }
}
