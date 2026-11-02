package io.openkache.core.replication.chain;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static io.openkache.core.replication.chain.ChainMessage.*;

/**
 * OSDI 2004 Chain의 SET/DELETE·tail 읽기 연구 구현. owner에서만 프로토콜 상태를 변경한다.
 * tail 적용이 확정이며 client future는 tail→head 결과 중계로 완료한다. Ack는 사슬을 거꾸로 흐른다.
 * 재구성은 신뢰하는 외부 master만 호출한다. 제거 노드는 영구 중단됐다는 전제가 필요하며
 * 분단 감지기·master 합의·재시작·새 노드 추가·CRAQ·영속화는 제공하지 않는다.
 */
public final class ChainEngine implements CacheEngine {
    public static final String TICK="chain.tick";
    public enum State { RUNNING, QUIESCING, FROZEN, REPAIRING, FAILED }
    /** 모든 시각은 TimeSource nanos다. sent는 적용했지만 tail Ack가 아직 없는 항목 수다. */
    public record Observation(long epoch,List<NodeId> chain,State state,int logLength,int applied,int acknowledged,
                              int sent,long readyAt) {
        public Observation { chain=List.copyOf(chain); }
    }
    private record Key(ClientId client,OperationId operation) {}
    private record Waiting(Request request,RequestContext context,CompletableFuture<Result> future) {}
    private final NodeId self;
    private final CacheEngine kv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource time;
    private final long retry;
    private final List<Request> log=new ArrayList<>();
    private final List<Result> results=new ArrayList<>();
    private final Map<Key,Integer> positions=new HashMap<>();
    private final List<Waiting> waiting=new ArrayList<>();
    private final Set<CompletableFuture<Result>> futures=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed=new AtomicBoolean();
    private List<NodeId> chain;
    private long epoch,readyAt=-1,lastControl=Long.MIN_VALUE,flightDue;
    private int applied,acknowledged,successorApplied,flight;
    private boolean applying,successorKnown=true;
    private State state=State.RUNNING;
    private CompletableFuture<Integer> configuration;
    private volatile Observation observation;

    /** 초기 구성은 모든 노드에서 동일해야 하며, retry는 데이터/제어 재전송 간격이다. */
    public ChainEngine(NodeId self,List<NodeId> chain,long epoch,CacheEngine kv,Mailbox owner,Transport transport,
                       TimeSource time,Duration retry) {
        this.self=Objects.requireNonNull(self); this.chain=checked(chain);
        if(!chain.contains(self) || epoch<1 || retry.isNegative() || retry.isZero()) throw new IllegalArgumentException();
        this.epoch=epoch; this.kv=Objects.requireNonNull(kv); this.owner=Objects.requireNonNull(owner);
        this.transport=Objects.requireNonNull(transport); this.time=Objects.requireNonNull(time); this.retry=retry.toNanos();
        readyAt=time.monotonicNanos(); publish();
    }
    private static List<NodeId> checked(List<NodeId> ids) {
        var copy=List.copyOf(ids);
        if(copy.isEmpty() || new HashSet<>(copy).size()!=copy.size()) throw new IllegalArgumentException("distinct nonempty chain required");
        return copy;
    }
    @Override public CompletionStage<Result> execute(Command command,RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        CompletableFuture<Result> f=new CompletableFuture<>(); futures.add(f); f.whenComplete((r,e)->futures.remove(f));
        if(!owner.offer(()->request(command,context,f))) f.complete(status(ResultStatus.BUSY));
        return f;
    }
    private void request(Command command,RequestContext context,CompletableFuture<Result> f) {
        if(closed.get() || state==State.FAILED) { f.complete(status(ResultStatus.UNAVAILABLE)); return; }
        if(context.deadline().isExpired(time)) { f.complete(status(ResultStatus.TIMEOUT)); return; }
        if(context.ownershipEpoch().value()<epoch) { f.complete(status(ResultStatus.STALE_EPOCH)); return; }
        if(state!=State.RUNNING) { f.complete(status(ResultStatus.UNAVAILABLE)); return; }
        boolean write=command instanceof Command.Set || command instanceof Command.Delete;
        NodeId target=write?head():tail();
        if(!self.equals(target)) { f.complete(Result.redirect(ResultStatus.NOT_PRIMARY,new Redirect(target),RoutingVersion.ZERO,epoch())); return; }
        if(!write) {
            kv.execute(command,context).whenComplete((r,error)->{
                if(!owner.offer(()->{
                    if(closed.get() || state!=State.RUNNING || context.ownershipEpoch().value()!=epoch) f.complete(status(ResultStatus.UNAVAILABLE));
                    else if(error!=null) f.complete(status(ResultStatus.UNAVAILABLE));
                    else f.complete(context.deadline().isExpired(time)?status(ResultStatus.TIMEOUT):withEpoch(r));
                })) f.complete(status(ResultStatus.UNAVAILABLE));
            });
            return;
        }
        Request r=new Request(context.clientId(),context.operationId(),command); Integer position=positions.get(key(r));
        if(position!=null) {
            if(!log.get(position-1).equals(r)) { f.complete(status(ResultStatus.BAD_REQUEST)); return; }
            if(position<=acknowledged) { f.complete(withEpoch(results.get(position-1))); return; }
            waiting.add(new Waiting(r,context,f));
            if(self.equals(tail()) && position<=applied) finish(r,results.get(position-1)); else sendDown();
            return;
        }
        if(applied!=log.size() || acknowledged!=applied) { f.complete(status(ResultStatus.BUSY)); return; }
        log.add(r); positions.put(key(r),log.size()); waiting.add(new Waiting(r,context,f)); applyNext(); publish();
    }
    @Override public void onPeerMessage(NodeId from,PeerMessage message) {
        if(message instanceof ChainMessage c) owner.offer(()->handle(from,c));
    }
    private void handle(NodeId from,ChainMessage message) {
        if(closed.get() || state==State.FAILED || state==State.QUIESCING || state==State.FROZEN || message.epoch()!=epoch) return;
        if(message instanceof Forward f && from.equals(previous())) {
            if(f.index()<1 || f.index()>log.size()+1) { sendProgress(); return; }
            if(f.index()<=log.size()) {
                if(!log.get(f.index()-1).equals(f.request())) throw new IllegalStateException("conflicting chain prefix");
                if(f.index()<=applied) {
                    sendProgress();
                    if(self.equals(tail())) completeAtTail(f.index()); else sendDown();
                }
            } else {
                if(positions.containsKey(key(f.request()))) throw new IllegalStateException("request at multiple chain positions");
                log.add(f.request()); positions.put(key(f.request()),log.size()); applyNext();
            }
        } else if(message instanceof Progress p && from.equals(next()) && p.applied()<=applied) {
            successorKnown=true;
            successorApplied=Math.max(successorApplied,p.applied());
            if(flight<=successorApplied) flight=0;
            sendDown();
        } else if(message instanceof Ack a && from.equals(next()) && a.committed()<=applied) {
            successorKnown=true;
            acknowledged=Math.max(acknowledged,a.committed()); successorApplied=Math.max(successorApplied,acknowledged);
            if(flight<=acknowledged) flight=0;
            if(previous()!=null) send(previous(),new Ack(epoch,acknowledged));
            // tail 중계가 유실돼도 역방향 Ack로 결과를 다시 제공할 수 있다.
            if(self.equals(head())) for(int i=0;i<acknowledged;i++) finish(log.get(i),results.get(i));
            sendDown();
        } else if(message instanceof Completed c && from.equals(tail()) && self.equals(head())
                && c.index()>0 && c.index()<=applied && log.get(c.index()-1).client().equals(c.client())
                && log.get(c.index()-1).operation().equals(c.operation())) {
            if(!results.get(c.index()-1).equals(c.result())) throw new IllegalStateException("chain execution results differ");
            finish(log.get(c.index()-1),c.result());
        }
        publish();
    }
    private void applyNext() {
        if(closed.get() || state==State.FAILED || state==State.FROZEN || applying) return;
        if(applied==log.size()) { freezeIfDrained(); return; }
        Request request=log.get(applied); int index=applied; applying=true;
        kv.execute(request.command(),new RequestContext(request.client(),request.operation(),RoutingVersion.ZERO,epoch(),
                Deadline.after(time,Duration.ofDays(1)))).whenComplete((r,error)->{
            if(!owner.offer(()->{
                applying=false; if(closed.get()) return;
                if(error!=null || r.status()!=ResultStatus.OK && r.status()!=ResultStatus.MISS) {
                    state=State.FAILED; finishWaiting(ResultStatus.UNAVAILABLE);
                    if(configuration!=null) configuration.completeExceptionally(new IllegalStateException("KV apply failed",error));
                    publish(); return;
                }
                if(index!=applied) throw new IllegalStateException("apply order changed");
                results.add(r); applied++;
                if(state!=State.QUIESCING) {
                    sendProgress(); if(self.equals(tail())) completeAtTail(applied); else sendDown();
                }
                applyNext(); publish();
            })) { closed.set(true); futures.forEach(f->f.complete(status(ResultStatus.UNAVAILABLE))); }
        });
    }
    private void completeAtTail(int index) {
        acknowledged=applied;
        if(previous()!=null) send(previous(),new Ack(epoch,acknowledged));
        Request request=log.get(index-1); Result result=results.get(index-1);
        if(self.equals(head())) finish(request,result);
        else send(head(),new Completed(epoch,index,request.client(),request.operation(),result));
    }
    private void sendProgress() { if(previous()!=null) send(previous(),new Progress(epoch,applied)); }
    private void sendDown() {
        if(next()==null || !successorKnown || state!=State.RUNNING && state!=State.REPAIRING || successorApplied>=applied) return;
        long now=time.monotonicNanos(); if(flight>successorApplied && now<flightDue) return;
        flight=successorApplied+1; flightDue=now+retry;
        send(next(),new Forward(epoch,flight,log.get(flight-1)));
    }
    /**
     * 신뢰하는 master 전용: 입장을 닫고 이전에 받아들인 KV 적용을 끝낸 뒤 새 구성을 FROZEN으로 설치한다.
     * 제거 노드는 이미 영구 중단되어야 한다. 살아 있는 노드의 임의 제외/분단 fencing으로 사용하면 안 된다.
     * master는 모든 생존자의 future 완료 후에만 beginRepair를 호출한다.
     */
    public CompletionStage<Integer> installConfiguration(long newEpoch,List<NodeId> survivors) {
        var copy=checked(survivors); CompletableFuture<Integer> f=new CompletableFuture<>();
        if(!owner.offer(()->{
            if(closed.get() || state!=State.RUNNING || newEpoch<=epoch || !copy.contains(self)
                    || !chain.containsAll(copy) || !chain.stream().filter(copy::contains).toList().equals(copy)) {
                f.completeExceptionally(new IllegalStateException("invalid serialized crash-only reconfiguration")); return;
            }
            finishWaiting(ResultStatus.UNAVAILABLE); chain=copy; epoch=newEpoch; readyAt=-1;
            state=State.QUIESCING; successorApplied=flight=0; successorKnown=false; lastControl=Long.MIN_VALUE; configuration=f;
            applyNext(); freezeIfDrained(); publish();
        })) f.completeExceptionally(new IllegalStateException("configuration mailbox full"));
        return f;
    }
    private void freezeIfDrained() {
        if(state==State.QUIESCING && !applying && applied==log.size()) {
            state=State.FROZEN; acknowledged=Math.min(acknowledged,applied); publish(); configuration.complete(applied);
        }
    }
    /** 모든 생존자가 FROZEN인 뒤 호출한다. suffix는 peer 링크로 전달되며 master가 데이터를 복사하지 않는다. */
    public CompletionStage<Void> beginRepair(long expectedEpoch) {
        CompletableFuture<Void> f=new CompletableFuture<>();
        if(!owner.offer(()->{
            if(closed.get() || epoch!=expectedEpoch || state!=State.FROZEN) { f.completeExceptionally(new IllegalStateException("not frozen")); return; }
            state=State.REPAIRING; sendProgress();
            if(self.equals(tail())) { acknowledged=applied; if(previous()!=null) send(previous(),new Ack(epoch,acknowledged)); }
            sendDown(); publish(); f.complete(null);
        })) f.completeExceptionally(new IllegalStateException("repair mailbox full"));
        return f;
    }
    /** master가 전 생존자의 로그 길이/적용/ACK 일치를 확인한 뒤 호출하는 쓰기·읽기 재개 barrier다. */
    public CompletionStage<Void> openConfiguration(long expectedEpoch,int expectedLength) {
        CompletableFuture<Void> f=new CompletableFuture<>();
        if(!owner.offer(()->{
            if(closed.get() || epoch!=expectedEpoch || state!=State.REPAIRING || applied!=expectedLength || acknowledged!=expectedLength
                    || log.size()!=expectedLength) { f.completeExceptionally(new IllegalStateException("chain repair incomplete")); return; }
            state=State.RUNNING; readyAt=time.monotonicNanos(); publish(); f.complete(null);
        })) f.completeExceptionally(new IllegalStateException("open mailbox full"));
        return f;
    }
    @Override public void onTimer(TimerEvent event) {
        if(!TICK.equals(event.name())) return;
        owner.offer(()->{
            if(closed.get() || state==State.FAILED) return;
            waiting.removeIf(w->{ if(w.context().deadline().isExpired(time)) w.future().complete(status(ResultStatus.TIMEOUT)); return w.future().isDone(); });
            if(state==State.RUNNING || state==State.REPAIRING) {
                long now=time.monotonicNanos();
                if(lastControl==Long.MIN_VALUE || now-lastControl>=retry) {
                    lastControl=now; sendProgress();
                    if(previous()!=null) send(previous(),new Ack(epoch,acknowledged));
                }
                sendDown();
            }
            applyNext(); publish();
        });
    }
    private void finish(Request request,Result result) {
        waiting.removeIf(w->{ if(!w.request().equals(request)) return false;
            w.future().complete(w.context().deadline().isExpired(time)?status(ResultStatus.TIMEOUT):withEpoch(result)); return true; });
    }
    private void finishWaiting(ResultStatus status) { waiting.forEach(w->w.future().complete(status(status))); waiting.clear(); }
    private Key key(Request r) { return new Key(r.client(),r.operation()); }
    private NodeId head() { return chain.getFirst(); }
    private NodeId tail() { return chain.getLast(); }
    private NodeId previous() { int i=chain.indexOf(self); return i==0?null:chain.get(i-1); }
    private NodeId next() { int i=chain.indexOf(self)+1; return i==chain.size()?null:chain.get(i); }
    private OwnershipEpoch epoch() { return new OwnershipEpoch(epoch); }
    private Result status(ResultStatus status) { return Result.of(status,RoutingVersion.ZERO,new OwnershipEpoch(observation==null?epoch:observation.epoch())); }
    private Result withEpoch(Result r) { return new Result(r.status(),r.value(),r.valueVersion(),r.routingVersion(),epoch(),r.redirect()); }
    private void send(NodeId node,ChainMessage message) { transport.send(new PeerEnvelope(self,node,message)); }
    private void publish() { observation=new Observation(epoch,chain,state,log.size(),applied,acknowledged,applied-acknowledged,readyAt); }
    /** 다른 thread에서도 읽을 수 있는 마지막 owner 발행 값이다. */
    public Observation observation() { return observation; }
    /** owner에서 복사하는 불변 진단 로그. 실제 KV 적용 위치는 observation.applied로 구분한다. */
    public CompletionStage<List<Request>> logSnapshot() {
        CompletableFuture<List<Request>> f=new CompletableFuture<>();
        if(!owner.offer(()->f.complete(List.copyOf(log)))) f.completeExceptionally(new IllegalStateException("diagnostic mailbox full"));
        return f;
    }
    @Override public EngineSnapshot snapshot() { return new EngineSnapshot(self,time.monotonicNanos(),RoutingVersion.ZERO,new OwnershipEpoch(observation.epoch())); }
    @Override public void close() {
        closed.set(true); futures.forEach(f->f.complete(status(ResultStatus.UNAVAILABLE)));
        owner.offer(()->{ if(configuration!=null) configuration.completeExceptionally(new IllegalStateException("closed")); });
    }
}
