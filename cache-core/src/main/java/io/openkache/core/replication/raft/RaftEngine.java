package io.openkache.core.replication.raft;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static io.openkache.core.replication.raft.RaftMessage.*;

/**
 * Raft 논문 Figure 2의 고정 3노드·메모리 유지 연구 구현. 모든 명령을 로그에 기록한다.
 * seed별 선거 timeout, 투표 최신성 검사, 현재 term만 직접 commit, 충돌 suffix 교체를 제공한다.
 * term/vote/log는 메모리에만 있다. 영구 crash와 메모리 유지 partition/repair만 지원하며
 * 프로세스 재시작, snapshot, 구성 변경, PreVote, ReadIndex는 지원하지 않는다.
 */
public final class RaftEngine implements CacheEngine {
    public static final String TICK="raft.tick";
    public enum Role { FOLLOWER, CANDIDATE, LEADER, FAILED }
    public record Observation(long term, Role role, NodeId leader, int logLength, int commit, int applied,
                              int elections, long detectedAt, long readyAt) {}
    /** 최근 로컬 선거 하나의 시각. 초기 선거 누적 관측과 구분하며 같은 JVM 안에서만 차감한다. */
    public record ElectionObservation(long term,long detectedAt,long readyAt) {}
    private long localElectionTerm,localElectionAt=-1,localReadyAt=-1;
    private volatile ElectionObservation electionObservation=new ElectionObservation(0,-1,-1);
    private record Key(ClientId client,OperationId operation) {}
    private record Waiting(Request request,RequestContext context,CompletableFuture<Result> future) {}
    private record Flight(Append message,long due,long interval) {}
    private final NodeId self;
    private final List<NodeId> members;
    private final CacheEngine kv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource time;
    private final long heartbeat,minimumTimeout;
    private final Random random;
    private final List<Entry> log=new ArrayList<>();
    private final Map<Key,Request> logged=new HashMap<>(),executed=new HashMap<>();
    private final Map<Key,Result> replies=new HashMap<>();
    private final List<Waiting> waiting=new ArrayList<>();
    private final Set<NodeId> votes=new HashSet<>();
    private final Map<NodeId,Integer> next=new HashMap<>(),match=new HashMap<>();
    private final Map<NodeId,Flight> data=new HashMap<>();
    private final Map<NodeId,Append> pulses=new HashMap<>();
    private final Set<CompletableFuture<Result>> futures=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed=new AtomicBoolean();
    private Role role=Role.FOLLOWER;
    private NodeId votedFor,leader;
    private long term,electionDue,lastPulse=Long.MIN_VALUE,rpc,detectedAt=-1,readyAt=-1;
    private int commit,applied,elections;
    private boolean applying;
    private volatile Observation observation;

    /** election timeout은 [minimumTimeout, 2*minimumTimeout), 난수는 노드별 seed로 고정한다. */
    public RaftEngine(NodeId self,List<NodeId> members,CacheEngine kv,Mailbox owner,Transport transport,
                      TimeSource time,Duration heartbeat,Duration minimumTimeout,long seed) {
        this.self=Objects.requireNonNull(self); this.members=List.copyOf(members);
        if(members.size()!=3 || new HashSet<>(members).size()!=3 || !members.contains(self)
                || heartbeat.isNegative() || heartbeat.isZero() || minimumTimeout.compareTo(heartbeat)<=0)
            throw new IllegalArgumentException("Raft requires three distinct members and timeout > heartbeat > 0");
        this.kv=Objects.requireNonNull(kv); this.owner=Objects.requireNonNull(owner);
        this.transport=Objects.requireNonNull(transport); this.time=Objects.requireNonNull(time);
        this.heartbeat=heartbeat.toNanos(); this.minimumTimeout=minimumTimeout.toNanos(); random=new Random(seed);
        resetElection(); publish();
    }
    @Override public CompletionStage<Result> execute(Command command,RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        CompletableFuture<Result> f=new CompletableFuture<>(); futures.add(f);
        f.whenComplete((r,e)->futures.remove(f));
        if(!owner.offer(()->request(new Request(context.clientId(),context.operationId(),command),context,f)))
            f.complete(status(ResultStatus.BUSY));
        return f;
    }
    private void request(Request request,RequestContext context,CompletableFuture<Result> f) {
        if(closed.get() || role==Role.FAILED) { f.complete(status(ResultStatus.UNAVAILABLE)); return; }
        if(context.deadline().isExpired(time)) { f.complete(status(ResultStatus.TIMEOUT)); return; }
        if(context.ownershipEpoch().value()<term) { f.complete(status(ResultStatus.STALE_EPOCH)); return; }
        if(role!=Role.LEADER) {
            f.complete(leader==null?status(ResultStatus.UNAVAILABLE):Result.redirect(ResultStatus.NOT_PRIMARY,
                    new Redirect(leader),RoutingVersion.ZERO,epoch())); return;
        }
        Key key=key(request); Request old=executed.get(key);
        if(old!=null) { f.complete(old.equals(request)?withEpoch(replies.get(key)):status(ResultStatus.BAD_REQUEST)); return; }
        old=logged.get(key);
        if(old!=null) {
            if(!old.equals(request)) f.complete(status(ResultStatus.BAD_REQUEST));
            else waiting.add(new Waiting(request,context,f));
            return;
        }
        if(applied!=log.size()) { f.complete(status(ResultStatus.BUSY)); return; }
        log.add(new Entry(term,Optional.of(request))); logged.put(key,request);
        waiting.add(new Waiting(request,context,f)); replicate(); publish();
    }
    @Override public void onPeerMessage(NodeId from,PeerMessage message) {
        if(message instanceof RaftMessage raft && members.contains(from) && !self.equals(from)) owner.offer(()->handle(from,raft));
    }
    private void handle(NodeId from,RaftMessage message) {
        if(closed.get() || role==Role.FAILED) return;
        if(message.term()>term) follow(message.term());
        if(message instanceof Vote v) {
            boolean fresh=v.lastTerm()>lastTerm() || v.lastTerm()==lastTerm() && v.lastIndex()>=log.size();
            boolean granted=v.term()==term && fresh && (votedFor==null || votedFor.equals(from));
            if(granted) { votedFor=from; resetElection(); }
            send(from,new VoteReply(term,granted));
        } else if(message instanceof VoteReply v) {
            if(v.term()==term && role==Role.CANDIDATE && v.granted()) {
                votes.add(from); if(votes.size()>=2) becomeLeader();
            }
        } else if(message instanceof Append a) {
            if(a.term()<term) send(from,new AppendReply(term,a.rpc(),false,log.size()));
            else {
                if(role!=Role.FOLLOWER) follow(term);
                leader=from; resetElection(); receiveAppend(from,a);
            }
        } else if(message instanceof AppendReply a && a.term()==term && role==Role.LEADER) {
            Flight flight=data.get(from); Append pulse=pulses.get(from);
            Append sent=flight!=null && flight.message().rpc()==a.rpc()?flight.message():pulse!=null && pulse.rpc()==a.rpc()?pulse:null;
            if(sent!=null) {
                int end=sent.prevIndex()+sent.entries().size();
                if(a.success() && a.matched()==end) {
                    match.merge(from,end,Math::max); next.merge(from,end+1,Math::max);
                    if(flight!=null && end>=flight.message().prevIndex()+flight.message().entries().size()) data.remove(from);
                    advanceCommit(); sendData(from);
                } else if(!a.success() && next.get(from)==sent.prevIndex()+1) {
                    next.put(from,Math.max(match.getOrDefault(from,0)+1,Math.min(next.get(from)-1,a.matched()+1)));
                    data.remove(from); sendData(from);
                }
            }
        }
        publish();
    }
    private void receiveAppend(NodeId from,Append a) {
        if(a.prevIndex()>log.size() || termAt(a.prevIndex())!=a.prevTerm()) {
            send(from,new AppendReply(term,a.rpc(),false,log.size())); return;
        }
        for(int i=0;i<a.entries().size();i++) {
            int index=a.prevIndex()+i; Entry incoming=a.entries().get(i);
            if(index<log.size() && log.get(index).term()!=incoming.term()) {
                if(index<commit) throw new IllegalStateException("committed prefix conflict");
                log.subList(index,log.size()).clear(); rebuildLogged();
            }
            if(index==log.size()) { log.add(incoming); incoming.request().ifPresent(r->logged.putIfAbsent(key(r),r)); }
            else if(!log.get(index).equals(incoming)) throw new IllegalStateException("same term/index has conflicting command");
        }
        int verified=a.prevIndex()+a.entries().size();
        commit=Math.max(commit,Math.min(a.commit(),verified)); applyNext();
        send(from,new AppendReply(term,a.rpc(),true,verified));
    }
    private void resetElection() { electionDue=Math.addExact(time.monotonicNanos(),minimumTimeout+random.nextLong(minimumTimeout)); }
    private void follow(long newTerm) {
        if(newTerm>term) { term=newTerm; votedFor=null; }
        role=Role.FOLLOWER; leader=null; votes.clear(); data.clear(); pulses.clear();
        finishWaiting(ResultStatus.UNAVAILABLE); resetElection();
    }
    private void elect() {
        term=Math.addExact(term,1); role=Role.CANDIDATE; votedFor=self; leader=null; elections++;
        localElectionTerm=term; localElectionAt=time.monotonicNanos(); localReadyAt=-1;
        if(detectedAt<0) detectedAt=time.monotonicNanos(); readyAt=-1;
        finishWaiting(ResultStatus.UNAVAILABLE); votes.clear(); votes.add(self); data.clear(); pulses.clear(); resetElection();
        for(NodeId n:members) if(!n.equals(self)) send(n,new Vote(term,log.size(),lastTerm()));
    }
    private void becomeLeader() {
        role=Role.LEADER; leader=self; data.clear(); pulses.clear(); next.clear(); match.clear();
        for(NodeId n:members) if(!n.equals(self)) { next.put(n,log.size()+1); match.put(n,0); }
        log.add(new Entry(term,Optional.empty())); // 과거 term 항목도 현재 term no-op의 commit으로 확정한다.
        lastPulse=time.monotonicNanos(); replicate();
    }
    private void advanceCommit() {
        for(int index=log.size();index>commit;index--) {
            if(termAt(index)!=term) continue;
            int copies=1; for(int m:match.values()) if(m>=index) copies++;
            if(copies>=2) { commit=index; applyNext(); break; }
        }
    }
    private void replicate() { for(NodeId n:members) if(!n.equals(self)) sendData(n); }
    private void sendData(NodeId node) {
        if(role!=Role.LEADER) return;
        Flight old=data.get(node); long now=time.monotonicNanos();
        if(old!=null && now<old.due()) return;
        int first=next.get(node); if(first>log.size()) return;
        // 하나의 follower 전송 창. heartbeat는 별도 빈 RPC로 유지한다.
        long interval=old==null?Duration.ofSeconds(1).toNanos():Math.min(Duration.ofSeconds(4).toNanos(),old.interval()*2);
        Append a=new Append(term,++rpc,first-1,termAt(first-1),log.subList(first-1,log.size()),commit);
        data.put(node,new Flight(a,now+interval,interval)); send(node,a);
    }
    private void pulse() {
        for(NodeId n:members) if(!n.equals(self)) {
            int prev=next.get(n)-1;
            Append a=new Append(term,++rpc,prev,termAt(prev),List.of(),commit);
            pulses.put(n,a); send(n,a); sendData(n);
        }
    }
    private void applyNext() {
        if(closed.get() || role==Role.FAILED || applying) return;
        while(applied<commit) {
            Entry entry=log.get(applied);
            if(entry.request().isEmpty()) { applied++; continue; }
            Request r=entry.request().orElseThrow(); Key key=key(r);
            if(executed.containsKey(key)) {
                if(!executed.get(key).equals(r)) throw new IllegalStateException("request identity reused in committed log");
                applied++; complete(r,replies.get(key)); continue;
            }
            int index=applied; applying=true;
            kv.execute(r.command(),new RequestContext(r.client(),r.operation(),RoutingVersion.ZERO,epoch(),
                    Deadline.after(time,Duration.ofDays(1)))).whenComplete((result,error)->{
                if(!owner.offer(()->{
                    applying=false; if(closed.get()) return;
                    if(error!=null || (result.status()!=ResultStatus.OK && result.status()!=ResultStatus.MISS)) {
                        role=Role.FAILED; finishWaiting(ResultStatus.UNAVAILABLE); publish(); return;
                    }
                    if(index!=applied || !log.get(index).equals(entry)) throw new IllegalStateException("applying entry changed");
                    applied++; executed.put(key,r); replies.put(key,result); complete(r,result); applyNext(); publish();
                })) { closed.set(true); futures.forEach(f->f.complete(status(ResultStatus.UNAVAILABLE))); }
            });
            return;
        }
        publish();
    }
    private void complete(Request request,Result result) {
        waiting.removeIf(w->{
            if(!w.request().equals(request)) return false;
            w.future().complete(w.context().deadline().isExpired(time)?status(ResultStatus.TIMEOUT):withEpoch(result)); return true;
        });
    }
    @Override public void onTimer(TimerEvent event) {
        if(!TICK.equals(event.name())) return;
        owner.offer(()->{
            if(closed.get() || role==Role.FAILED) return;
            waiting.removeIf(w->{ if(w.context().deadline().isExpired(time)) w.future().complete(status(ResultStatus.TIMEOUT)); return w.future().isDone(); });
            long now=time.monotonicNanos();
            if(role!=Role.LEADER && now>=electionDue) elect();
            if(role==Role.LEADER && (lastPulse==Long.MIN_VALUE || now-lastPulse>=heartbeat)) { lastPulse=now; pulse(); }
            applyNext(); publish();
        });
    }
    private void rebuildLogged() { logged.clear(); for(Entry e:log) e.request().ifPresent(r->logged.putIfAbsent(key(r),r)); }
    private long termAt(int index) { return index==0?0:log.get(index-1).term(); }
    private long lastTerm() { return termAt(log.size()); }
    private Key key(Request r) { return new Key(r.client(),r.operation()); }
    private OwnershipEpoch epoch() { return new OwnershipEpoch(term); }
    private Result status(ResultStatus status) { return Result.of(status,RoutingVersion.ZERO,new OwnershipEpoch(observation==null?term:observation.term())); }
    private Result withEpoch(Result r) { return new Result(r.status(),r.value(),r.valueVersion(),r.routingVersion(),epoch(),r.redirect()); }
    private void send(NodeId node,RaftMessage message) { transport.send(new PeerEnvelope(self,node,message)); }
    private void finishWaiting(ResultStatus status) { waiting.forEach(w->w.future().complete(status(status))); waiting.clear(); }
    private void publish() {
        if(role==Role.LEADER && applied==log.size() && readyAt<0) readyAt=time.monotonicNanos();
        if(role==Role.LEADER && term==localElectionTerm && applied==log.size() && localReadyAt<0)
            localReadyAt=time.monotonicNanos();
        electionObservation=new ElectionObservation(localElectionTerm,localElectionAt,localReadyAt);
        observation=new Observation(term,role,leader,log.size(),commit,applied,elections,detectedAt,readyAt);
    }
    public Observation observation() { return observation; }
    /** owner가 발행한 불변 값. 장애 후 term과 일치하는 경우에만 해당 선거의 duration으로 사용한다. */
    public ElectionObservation electionObservation() { return electionObservation; }
    public CompletionStage<List<Entry>> logSnapshot() {
        CompletableFuture<List<Entry>> f=new CompletableFuture<>();
        if(!owner.offer(()->f.complete(List.copyOf(log)))) f.completeExceptionally(new IllegalStateException("diagnostic mailbox full"));
        return f;
    }
    @Override public EngineSnapshot snapshot() { return new EngineSnapshot(self,time.monotonicNanos(),RoutingVersion.ZERO,new OwnershipEpoch(observation.term())); }
    @Override public void close() { closed.set(true); futures.forEach(f->f.complete(status(ResultStatus.UNAVAILABLE))); }
}
