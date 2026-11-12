package io.openkache.core.replication.ionia_seventh;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** V05: 목적지 선택만 변경한다. O00의 요청별 Meta/index/과반 View 인증을 직접 재사용한다.
 * 제어 상태는 lock으로 직렬화한다. borrowed transport는 비차단이어야 하며 소유자가 닫는다.
 * public cancel/timeout은 미완료 raw READ stage 허가를 반환하지 않는다. */
public final class IoniaAdaptiveReadClient implements AutoCloseable {
    public record NodeDiagnostics(NodeId node,int rawPending,long rawStarts,long rawTerminal,long rawSuccess,long rawFailure,
            long ignoredGeneration,long ignoredSequence,long selections,long ewmaNanos,long sampleAgeNanos,boolean fresh,
            long cooldownRemainingNanos,long feedbackSequence) { }
    public record Diagnostics(long view,NodeId leader,long generation,boolean closed,String profile,
            int activeLogical,long logicalStarts,long logicalTerminal,long busy,long publicCancels,
            long leaderGetCalls,long metaCalls,long metaDataReplies,long probeCalls,String lastReason,NodeId lastSelected,
            Map<ResultStatus,Long> outcomes,List<NodeDiagnostics> nodes) {
        public Diagnostics {outcomes=Map.copyOf(outcomes);nodes=List.copyOf(nodes);}
    }
    private final Object lock=new Object();private final ArrayDeque<Runnable> completions=new ArrayDeque<>();
    private final Set<CompletableFuture<IoniaEngine.ViewInfo>> refreshes=new HashSet<>();
    private final ClientId clientId;private final IoniaConfig membership;private final TimeSource time;
    private final IoniaReadSelectionSettings settings;private final ReplicaSelector selector;private final IoniaClient delegate;
    private final ObservedTransport observed;private long view;private NodeId leader;private boolean closed;
    private int activeLogical;private long logicalStarts,logicalTerminal,busy,publicCancels,leaderGets,metaCalls,metaData,probes;
    private String lastReason="NONE";private NodeId lastSelected;private final EnumMap<ResultStatus,Long> outcomes=new EnumMap<>(ResultStatus.class);

    public IoniaAdaptiveReadClient(ClientId id,List<NodeId> replicas,NodeId initialLeader,long initialView,
            IoniaClientTransport transport,TimeSource time,IoniaReadSelectionSettings settings){
        clientId=Objects.requireNonNull(id);membership=new IoniaConfig(initialLeader,initialLeader,replicas,initialView);
        this.time=Objects.requireNonNull(time);this.settings=Objects.requireNonNull(settings);view=initialView;leader=initialLeader;
        selector=new ReplicaSelector(membership.replicas(),id,settings);observed=new ObservedTransport(Objects.requireNonNull(transport));
        delegate=new IoniaClient(id,replicas,initialLeader,initialView,observed,time,new IoniaClient.Options(initialLeader,true,IoniaReadSelectionSettings.WRITE_FALLBACK_NANOS));
    }
    /** 같은 ID/총 deadline을 유지한다. GET의 local admission 실패는 새 RPC 없이 BUSY다. */
    public CompletionStage<Result> execute(Command command,RequestContext context){
        Objects.requireNonNull(command);Objects.requireNonNull(context);boolean read=command instanceof Command.Get;
        CompletableFuture<Result> output=new CompletableFuture<>();CompletionStage<Result> stage=null;Result rejected=null;boolean admitted=false;
        synchronized(lock){
            if(closed)rejected=result(ResultStatus.UNAVAILABLE);
            else if(!clientId.equals(context.clientId()))rejected=result(ResultStatus.BAD_REQUEST);
            else if(context.deadline().isExpired(time))rejected=result(ResultStatus.TIMEOUT);
            else try{
                if(read){
                    ReplicaSelector.Choice selected=activeLogical>=settings.logicalLimit()?null:selector.choose(leader,time.monotonicNanos());
                    if(selected==null){busy++;rejected=result(ResultStatus.BUSY);}
                    else{
                        activeLogical++;logicalStarts++;admitted=true;lastSelected=selected.node();lastReason=selected.reason();
                        try{stage=delegate.readAtFollower(selected.node(),(Command.Get)command,context);}
                        catch(RuntimeException error){stage=CompletableFuture.failedFuture(error);}
                    }
                }else stage=delegate.execute(command,context);
            }catch(ArithmeticException exhausted){exhaust();rejected=result(ResultStatus.UNAVAILABLE);}
            catch(RuntimeException error){stage=CompletableFuture.failedFuture(error);}
        }
        final boolean accepted=admitted;
        if(rejected!=null){Result value=rejected;if(read)synchronized(lock){outcomes.merge(value.status(),1L,Long::sum);}publish(()->output.complete(value));}
        else{
            output.whenComplete((value,error)->{if(output.isCancelled())synchronized(lock){publicCancels++;}});
            Objects.requireNonNull(stage).whenComplete((value,error)->{
                Result answer=error==null&&value!=null?value:result(ResultStatus.UNAVAILABLE);
                synchronized(lock){if(accepted){activeLogical--;logicalTerminal++;}if(read)outcomes.merge(answer.status(),1L,Long::sum);}
                publish(()->output.complete(answer));
            });
        }
        drain();return output;
    }
    /** 정적 leaderFor 규칙과 monotonic View를 검사하며 delegate와 선택 세대를 함께 바꾼다. */
    public void updateView(long next,NodeId nextLeader){
        try{synchronized(lock){updateEndpoint(next,nextLeader);}}finally{drain();}
    }
    private void updateEndpoint(long next,NodeId nextLeader){
        if(closed)throw new IllegalStateException("client closed");
        if(next<view||!membership.leaderFor(next).equals(nextLeader))throw new IllegalArgumentException("invalid static view/leader");
        if(next==view){if(!leader.equals(nextLeader))throw new IllegalArgumentException("two leaders");return;}
        try{selector.newView();}catch(ArithmeticException exhausted){exhaust();throw new IllegalStateException("generation exhausted",exhausted);}
        delegate.updateView(next,nextLeader);view=next;leader=nextLeader;
    }
    /** 발견은 목적지 업데이트뿐이다. GET 인증은 O00의 과반 probe를 따른다. */
    public CompletionStage<IoniaEngine.ViewInfo> refreshView(){
        CompletableFuture<IoniaEngine.ViewInfo> output=new CompletableFuture<>();
        synchronized(lock){if(closed)return CompletableFuture.failedFuture(new IllegalStateException("client closed"));refreshes.add(output);}
        output.whenComplete((v,e)->{synchronized(lock){refreshes.remove(output);}});
        List<CompletableFuture<IoniaEngine.ViewInfo>> observations=membership.replicas().stream()
                .map(n->observed.probe(n).exceptionally(e->null).toCompletableFuture()).toList();
        CompletableFuture.allOf(observations.toArray(CompletableFuture<?>[]::new)).whenComplete((ignored,error)->{
            try{
                IoniaEngine.ViewInfo newest=observations.stream().map(CompletableFuture::join).filter(Objects::nonNull)
                        .filter(i->i.status()==IoniaEngine.Status.NORMAL&&membership.leaderFor(i.view()).equals(i.leaderId()))
                        .max(Comparator.comparingLong(IoniaEngine.ViewInfo::view)).orElseThrow(()->new IllegalStateException("no normal primary"));
                synchronized(lock){updateEndpoint(newest.view(),newest.leaderId());}
                publish(()->output.complete(newest));
            }catch(RuntimeException failure){publish(()->output.completeExceptionally(failure));}
        });drain();return output;
    }
    /** 호출자의 timer로 O00 deadline/fast-write fallback을 진행한다. */
    public void onTimer(TimerEvent event){delegate.onTimer(Objects.requireNonNull(event));drain();}
    private void exhaust(){closed=true;completions.add(this::close);}
    private void publish(Runnable action){synchronized(lock){completions.add(action);}drain();}
    private void drain(){
        if(Thread.holdsLock(lock))return;
        while(true){Runnable action;synchronized(lock){action=completions.poll();}if(action==null)return;action.run();}
    }
    @Override public void close(){
        List<CompletableFuture<IoniaEngine.ViewInfo>> pending;
        synchronized(lock){if(closed){pending=List.copyOf(refreshes);}else{closed=true;pending=List.copyOf(refreshes);}}
        delegate.close();for(var future:pending)publish(()->future.completeExceptionally(new IllegalStateException("client closed")));drain();
    }
    /** 일관된 local snapshot. rawPending은 stage 수이며 socket/SSD queue 수가 아니다. */
    public Diagnostics diagnostics(){
        synchronized(lock){
            long now=time.monotonicNanos();List<NodeDiagnostics> nodes=new ArrayList<>();
            selector.states.forEach((n,s)->nodes.add(new NodeDiagnostics(n,s.pending,s.starts,s.terminal,s.success,s.failure,s.ignoredGeneration,s.ignoredSequence,s.selections,
                    s.ewma,s.sampled?Math.max(0,now-s.at):-1,selector.fresh(s,now),s.cooling?Math.max(0,s.cooldownUntil-now):0,s.feedbackSeq)));
            return new Diagnostics(view,leader,selector.generation,closed,settings.profileId(),activeLogical,logicalStarts,logicalTerminal,busy,publicCancels,
                    leaderGets,metaCalls,metaData,probes,lastReason,lastSelected,outcomes,nodes);
        }
    }
    private static Result result(ResultStatus status){return Result.of(status,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
    private final class ObservedTransport implements IoniaClientTransport {
        private final IoniaClientTransport borrowed;ObservedTransport(IoniaClientTransport borrowed){this.borrowed=borrowed;}
        private <T> CompletionStage<T> forward(Supplier<CompletionStage<T>> call){
            synchronized(lock){
                if(closed)return CompletableFuture.failedFuture(new IllegalStateException("client closed"));
                try{return Objects.requireNonNull(call.get(),"null transport stage");}
                catch(RuntimeException error){return CompletableFuture.failedFuture(error);}
            }
        }
        @Override public CompletionStage<WriteAck> record(NodeId n,Request r){return forward(()->borrowed.record(n,r));}
        @Override public CompletionStage<Reply> execute(NodeId n,Request r){
            return forward(()->{if(r.command() instanceof Command.Get)leaderGets++;return borrowed.execute(n,r);});
        }
        @Override public CompletionStage<MetaReply> metaQuery(NodeId n,Request r){
            return forward(()->{metaCalls++;return borrowed.metaQuery(n,r);}).whenComplete((reply,error)->{if(error==null&&reply!=null&&reply.data()!=null)synchronized(lock){metaData++;}});
        }
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId n){
            return forward(()->{probes++;return borrowed.probe(n);});
        }
        @Override public CompletionStage<ReadReply> followerRead(NodeId n,Request request){
            CompletableFuture<ReadReply> checked=new CompletableFuture<>();ReplicaSelector.Ticket ticket;
            synchronized(lock){
                if(closed)return CompletableFuture.failedFuture(new IllegalStateException("client closed"));
                try{ticket=selector.begin(n,request,time.monotonicNanos());}
                catch(ArithmeticException overflow){exhaust();return CompletableFuture.failedFuture(new IllegalStateException("dispatch sequence exhausted",overflow));}
                catch(RuntimeException invalid){return CompletableFuture.failedFuture(invalid);}
            }
            try{
                CompletionStage<ReadReply> raw=forward(()->borrowed.followerRead(n,request));
                raw.whenComplete((reply,error)->finish(ticket,checked,reply,error));
            }catch(RuntimeException error){finish(ticket,checked,null,error);}
            return checked;
        }
        private void finish(ReplicaSelector.Ticket ticket,CompletableFuture<ReadReply> checked,ReadReply reply,Throwable error){
            boolean first;synchronized(lock){first=selector.finish(ticket,reply,error,time.monotonicNanos());}
            if(first)publish(()->{if(error!=null)checked.completeExceptionally(error);else checked.complete(reply);});
        }
    }
}
