package io.openkache.core.replication.rose;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static io.openkache.core.replication.rose.RoseMessage.*;

/**
 * Rosé의 bounded queue에서 착안한 단일 파티션·op 단위 연구 변형이다.
 * L>0: 예약한 로그 길이 - 모든 Backup의 최소 적용 ACK < L일 때 수락하고 로컬 적용 후 응답한다.
 * L=0: 비교용 전원 적용 ACK 동기 모드, L=-1: 무제한 비동기 모드다.
 * 다중 파티션 coordinated apply·snapshot 서비스·지역/트랜잭션·영속 WAL을 재현하지 않는다.
 * 외부 master는 정확한 영구 Primary crash만 제외하고 가장 긴 적용 prefix의 Backup을 선택한다.
 * 상태 변경과 KV 완료 처리는 owner mailbox에서만 실행한다.
 */
public final class RoseEngine implements CacheEngine {
    public static final String TICK="rose.tick";
    public enum State { RUNNING, QUIESCING, FROZEN, REPAIRING, FAILED }
    /** op 수 기준 관측값. peakLag는 미응답 예약도 포함하며 L=0에서는 1까지 가능하다. 시각은 nanos다. */
    public record Observation(long epoch,NodeId primary,State state,int logLength,int applied,int minAck,
                              int peakLag,int backpressureCount,boolean deferred,long readyAt) {}
    private record Key(ClientId client,OperationId operation) {}
    private record Waiting(Request request,RequestContext context,CompletableFuture<Result> future) {}
    private final NodeId self;
    private List<NodeId> members;
    private NodeId primary;
    private final CacheEngine kv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource time;
    private final int limit;
    private final long retry;
    private final List<Request> log=new ArrayList<>();
    private final List<Result> results=new ArrayList<>();
    private final Map<Key,Integer> positions=new HashMap<>();
    private final Map<NodeId,Integer> acknowledgments=new HashMap<>(),flights=new HashMap<>();
    private final Map<NodeId,Long> due=new HashMap<>();
    private final List<Waiting> waiting=new ArrayList<>();
    private final Set<CompletableFuture<Result>> futures=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed=new AtomicBoolean();
    private State state=State.RUNNING;
    private long epoch,lastAck=Long.MIN_VALUE,readyAt;
    private int applied,peakLag,backpressureCount;
    private boolean applying;
    private Request deferred;
    private CompletableFuture<Integer> freeze;
    private volatile Observation observation;

    /** limit는 -1(무제한), 0(동기), 양수(op 단위 상한) 중 하나다. 초기 로그는 비어 있어야 한다. */
    public RoseEngine(NodeId self,List<NodeId> members,NodeId primary,long epoch,int limit,CacheEngine kv,
                      Mailbox owner,Transport transport,TimeSource time,Duration retry) {
        this.self=Objects.requireNonNull(self); this.members=List.copyOf(members); this.primary=Objects.requireNonNull(primary);
        if(this.members.size()<2 || new HashSet<>(members).size()!=members.size() || !members.contains(self) || !members.contains(primary)
                || epoch<1 || limit< -1 || retry.isZero() || retry.isNegative()) throw new IllegalArgumentException();
        this.epoch=epoch; this.limit=limit; this.kv=Objects.requireNonNull(kv); this.owner=Objects.requireNonNull(owner);
        this.transport=Objects.requireNonNull(transport); this.time=Objects.requireNonNull(time); this.retry=retry.toNanos();
        for(NodeId n:members) if(!n.equals(primary)) acknowledgments.put(n,0);
        readyAt=time.monotonicNanos(); publish();
    }
    @Override public CompletionStage<Result> execute(Command command,RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        CompletableFuture<Result> f=new CompletableFuture<>(); futures.add(f); f.whenComplete((r,e)->futures.remove(f));
        if(!owner.offer(()->request(command,context,f))) f.complete(status(ResultStatus.BUSY)); return f;
    }
    private void request(Command command,RequestContext context,CompletableFuture<Result> f) {
        if(closed.get() || state!=State.RUNNING) { f.complete(status(ResultStatus.UNAVAILABLE)); return; }
        if(context.deadline().isExpired(time)) { f.complete(status(ResultStatus.TIMEOUT)); return; }
        if(context.ownershipEpoch().value()<epoch) { f.complete(status(ResultStatus.STALE_EPOCH)); return; }
        if(!self.equals(primary)) { f.complete(Result.redirect(ResultStatus.NOT_PRIMARY,new Redirect(primary),RoutingVersion.ZERO,epoch())); return; }
        if(!(command instanceof Command.Set) && !(command instanceof Command.Delete)) {
            kv.execute(command,context).whenComplete((r,error)->{
                if(!owner.offer(()->f.complete(closed.get() || state!=State.RUNNING || error!=null?status(ResultStatus.UNAVAILABLE):
                        context.deadline().isExpired(time)?status(ResultStatus.TIMEOUT):withEpoch(r)))) f.complete(status(ResultStatus.UNAVAILABLE));
            }); return;
        }
        Request r=new Request(context.clientId(),context.operationId(),command); Integer position=positions.get(key(r));
        if(position!=null) {
            if(!log.get(position-1).equals(r)) f.complete(status(ResultStatus.BAD_REQUEST));
            else if(position<=applied && (limit!=0 || position<=minAck())) f.complete(withEpoch(results.get(position-1)));
            else waiting.add(new Waiting(r,context,f));
            return;
        }
        if(deferred!=null) {
            if(key(deferred).equals(key(r))) {
                if(!deferred.equals(r)) f.complete(status(ResultStatus.BAD_REQUEST)); else waiting.add(new Waiting(r,context,f));
            } else f.complete(status(ResultStatus.BUSY));
            return;
        }
        if(applying) { f.complete(status(ResultStatus.BUSY)); return; }
        waiting.add(new Waiting(r,context,f));
        if(!room()) { deferred=r; backpressureCount++; publish(); return; }
        admit(r);
    }
    private boolean room() { return limit<0 || log.size()-minAck()<(limit==0?1:limit); }
    private void admit(Request r) {
        log.add(r); positions.put(key(r),log.size()); publish(); applyNext();
    }
    private void admitDeferred() {
        if(deferred==null || applying || !self.equals(primary) || state!=State.RUNNING) return;
        waiting.removeIf(w->{ if(w.context().deadline().isExpired(time)) w.future().complete(status(ResultStatus.TIMEOUT)); return w.future().isDone(); });
        Request r=deferred;
        if(waiting.stream().noneMatch(w->w.request().equals(r))) { deferred=null; return; }
        if(room()) { deferred=null; admit(r); }
    }
    @Override public void onPeerMessage(NodeId from,PeerMessage message) {
        if(message instanceof RoseMessage r) owner.offer(()->handle(from,r));
    }
    private void handle(NodeId from,RoseMessage message) {
        if(closed.get() || state!=State.RUNNING && state!=State.REPAIRING || message.epoch()!=epoch || !members.contains(from)) return;
        if(message instanceof Append a && from.equals(primary) && !self.equals(primary)) {
            if(a.index()>log.size()+1) { sendAck(); return; }
            if(a.index()<=log.size()) {
                if(!log.get(a.index()-1).equals(a.request())) throw new IllegalStateException("conflicting replica prefix");
                sendAck();
            } else {
                if(positions.containsKey(key(a.request()))) throw new IllegalStateException("duplicate log identity");
                log.add(a.request()); positions.put(key(a.request()),log.size()); applyNext();
            }
        } else if(message instanceof Applied a && self.equals(primary) && !from.equals(primary) && a.index()<=applied) {
            acknowledgments.merge(from,a.index(),Math::max);
            if(flights.getOrDefault(from,0)<=a.index()) flights.remove(from);
            completeEligible(); sendTail(from); admitDeferred();
        }
        publish();
    }
    private void applyNext() {
        if(closed.get() || state==State.FAILED || state==State.FROZEN || applying) return;
        if(applied==log.size()) { freezeIfDrained(); return; }
        int index=applied; Request r=log.get(index); applying=true;
        kv.execute(r.command(),new RequestContext(r.client(),r.operation(),RoutingVersion.ZERO,epoch(),Deadline.after(time,Duration.ofDays(1))))
                .whenComplete((result,error)->{
                    if(!owner.offer(()->{
                        applying=false; if(closed.get()) return;
                        if(error!=null || result.status()!=ResultStatus.OK && result.status()!=ResultStatus.MISS) {
                            state=State.FAILED; finishWaiting(ResultStatus.UNAVAILABLE);
                            if(freeze!=null) freeze.completeExceptionally(new IllegalStateException("KV apply failed",error)); publish(); return;
                        }
                        if(index!=applied) throw new IllegalStateException("apply order changed");
                        results.add(result); applied++;
                        if(state!=State.QUIESCING) {
                            if(self.equals(primary)) { completeEligible(); replicate(); }
                            else sendAck();
                        }
                        applyNext(); admitDeferred(); publish();
                    })) { closed.set(true); futures.forEach(f->f.complete(status(ResultStatus.UNAVAILABLE))); }
                });
    }
    private void completeEligible() {
        if(!self.equals(primary)) return;
        int through=limit==0?Math.min(applied,minAck()):applied;
        waiting.removeIf(w->{ Integer index=positions.get(key(w.request()));
            if(index==null || index>through) return false;
            w.future().complete(w.context().deadline().isExpired(time)?status(ResultStatus.TIMEOUT):withEpoch(results.get(index-1))); return true;
        });
    }
    private int minAck() { return acknowledgments.values().stream().mapToInt(Integer::intValue).min().orElse(applied); }
    private void replicate() { for(NodeId n:members) if(!n.equals(primary)) sendTail(n); }
    private void sendTail(NodeId node) {
        if(!self.equals(primary) || state!=State.RUNNING && state!=State.REPAIRING) return;
        int ack=acknowledgments.getOrDefault(node,-1); if(ack<0 || ack>=applied) return;
        long now=time.monotonicNanos(); if(flights.containsKey(node) && now<due.getOrDefault(node,0L)) return;
        int index=ack+1; flights.put(node,index); due.put(node,now+retry); send(node,new Append(epoch,index,log.get(index-1)));
    }
    private void sendAck() { if(!self.equals(primary)) send(primary,new Applied(epoch,applied)); }
    /**
     * 신뢰 master 전용. 영구 Primary crash 후 생존 Backup 전부를 freeze하고 수락한 KV 적용을 끝낸다.
     * 살아 있는 Primary를 제거하는 partition fencing으로 사용하지 않는다.
     */
    public CompletionStage<Integer> freezeAfterCrash(long newEpoch) {
        CompletableFuture<Integer> f=new CompletableFuture<>();
        if(!owner.offer(()->{
            if(closed.get() || state!=State.RUNNING || self.equals(primary) || newEpoch<=epoch) {
                f.completeExceptionally(new IllegalStateException("invalid crash freeze")); return;
            }
            finishWaiting(ResultStatus.UNAVAILABLE); deferred=null; epoch=newEpoch; readyAt=-1; state=State.QUIESCING; freeze=f;
            applyNext(); freezeIfDrained(); publish();
        })) f.completeExceptionally(new IllegalStateException("freeze mailbox full"));
        return f;
    }
    private void freezeIfDrained() {
        if(state==State.QUIESCING && !applying && applied==log.size()) { state=State.FROZEN; publish(); freeze.complete(applied); }
    }
    /** 모든 Backup을 freeze한 뒤 가장 긴 적용 prefix의 노드를 newPrimary로 지정한다. suffix는 peer로 복구한다. */
    public CompletionStage<Void> beginRepair(long expectedEpoch,NodeId newPrimary,List<NodeId> survivors) {
        List<NodeId> copy=List.copyOf(survivors); CompletableFuture<Void> f=new CompletableFuture<>();
        if(!owner.offer(()->{
            if(closed.get() || state!=State.FROZEN || epoch!=expectedEpoch || copy.size()<2 || new HashSet<>(copy).size()!=copy.size()
                    || !copy.contains(self) || !copy.contains(newPrimary) || copy.contains(primary) || !members.containsAll(copy)) {
                f.completeExceptionally(new IllegalStateException("invalid crash repair")); return;
            }
            primary=newPrimary; members=copy; acknowledgments.clear(); flights.clear(); due.clear();
            for(NodeId n:members) if(!n.equals(primary)) acknowledgments.put(n,-1);
            state=State.REPAIRING; lastAck=Long.MIN_VALUE; if(!self.equals(primary)) sendAck(); publish(); f.complete(null);
        })) f.completeExceptionally(new IllegalStateException("repair mailbox full"));
        return f;
    }
    /** master가 전 생존자의 prefix/적용 일치와 새 Primary의 ACK를 확인한 뒤 호출한다. */
    public CompletionStage<Void> openAfterRepair(long expectedEpoch,int length) {
        CompletableFuture<Void> f=new CompletableFuture<>();
        if(!owner.offer(()->{
            if(closed.get() || epoch!=expectedEpoch || state!=State.REPAIRING || applied!=length || log.size()!=length
                    || self.equals(primary) && minAck()!=length) { f.completeExceptionally(new IllegalStateException("repair incomplete")); return; }
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
                if(self.equals(primary)) { replicate(); admitDeferred(); }
                else if(lastAck==Long.MIN_VALUE || time.monotonicNanos()-lastAck>=retry) { lastAck=time.monotonicNanos(); sendAck(); }
            }
            applyNext(); publish();
        });
    }
    private void finishWaiting(ResultStatus status) { waiting.forEach(w->w.future().complete(status(status))); waiting.clear(); }
    private Key key(Request r) { return new Key(r.client(),r.operation()); }
    private OwnershipEpoch epoch() { return new OwnershipEpoch(epoch); }
    private Result status(ResultStatus status) { return Result.of(status,RoutingVersion.ZERO,new OwnershipEpoch(observation==null?epoch:observation.epoch())); }
    private Result withEpoch(Result r) { return new Result(r.status(),r.value(),r.valueVersion(),r.routingVersion(),epoch(),r.redirect()); }
    private void send(NodeId to,RoseMessage message) { transport.send(new PeerEnvelope(self,to,message)); }
    private void publish() {
        int ack=minAck(); if(self.equals(primary) && ack>=0) peakLag=Math.max(peakLag,log.size()-ack);
        observation=new Observation(epoch,primary,state,log.size(),applied,ack,peakLag,backpressureCount,deferred!=null,readyAt);
    }
    /** 다른 thread에서 읽을 수 있는 마지막 owner 발행 관측값이다. */
    public Observation observation() { return observation; }
    /** owner에서 복사하는 불변 진단 로그다. 적용 완료 위치는 observation.applied와 구분한다. */
    public CompletionStage<List<Request>> logSnapshot() {
        CompletableFuture<List<Request>> f=new CompletableFuture<>();
        if(!owner.offer(()->f.complete(List.copyOf(log)))) f.completeExceptionally(new IllegalStateException("diagnostic mailbox full")); return f;
    }
    @Override public EngineSnapshot snapshot() { return new EngineSnapshot(self,time.monotonicNanos(),RoutingVersion.ZERO,new OwnershipEpoch(observation.epoch())); }
    @Override public void close() {
        closed.set(true); futures.forEach(f->f.complete(status(ResultStatus.UNAVAILABLE)));
        owner.offer(()->{ if(freeze!=null) freeze.completeExceptionally(new IllegalStateException("closed")); });
    }
}
