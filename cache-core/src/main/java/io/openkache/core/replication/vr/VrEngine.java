package io.openkache.core.replication.vr;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static io.openkache.core.replication.vr.VrMessage.*;

/**
 * VR Revisited §4.1–4.2 기반 고정 3노드 연구 구현. 모든 명령(읽기 포함)을 로그/과반수로 처리한다.
 * 미확정 로그는 KV에 적용하지 않으며 view change는 last-normal-view, 로그 길이 순으로 선택한다.
 * owner mailbox만 프로토콜 상태를 변경한다. KV callback은 owner로 돌아온다.
 * 메모리를 유지한 분단/복구를 지원하며 프로세스 재시작·구성 변경·checkpoint·TCP codec은 포함하지 않는다.
 * 기본은 증분 PREPARE/작은 COMMIT/확인 기반 재전송이다. FULL_LOG는 개선 전 실험 재현용이다.
 */
public final class VrEngine implements CacheEngine {
    /** FULL_LOG는 기존 비교 재현, INCREMENTAL은 증분 전송·작은 heartbeat·재전송 제어를 사용한다. */
    public enum Mode { FULL_LOG, INCREMENTAL }
    /** 외부 실행기가 주기적으로 전달하는 timer 이름. tick 단위는 TimeSource nanos다. */
    public static final String TICK = "vr.tick";
    /** NORMAL은 정상 프로토콜, INSTALLING은 새 로그의 과반수 확인 대기다. FAILED는 로컬 적용 실패다. */
    public enum State { NORMAL, VIEW_CHANGE, INSTALLING, FAILED }
    private record Key(ClientId client, OperationId operation) {}
    private record Waiting(Request request, RequestContext context, CompletableFuture<Result> future) {}
    /** owner가 발행하는 불변 관측값. 시각은 TimeSource의 nanos, 미발생은 -1. */
    public record Observation(long view, State state, int logLength, int commit, int applied,
            long detectedAt, long readyAt) {}

    private final NodeId self;
    private final List<NodeId> members;
    private final CacheEngine kv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource time;
    private final long heartbeat, timeout;
    private final Mode mode;
    private final long viewChangeWait;
    private static final long RETRY_BASE=Duration.ofSeconds(1).toNanos();
    private static final long RETRY_MAX=Duration.ofSeconds(4).toNanos();
    private record Transfer(VrMessage message,int length,long due,long interval) {}
    private final Map<NodeId,Transfer> transfers=new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<CompletableFuture<Result>> futures = ConcurrentHashMap.newKeySet();
    private final List<Waiting> waiting = new ArrayList<>();
    private final Map<Key, Result> replies = new HashMap<>();
    private final Map<Key, Request> appliedRequests = new HashMap<>();
    private final Map<Key,Request> loggedRequests=new HashMap<>();
    private final Set<NodeId> starters = new HashSet<>();
    private final Map<NodeId, DoViewChange> changes = new HashMap<>();
    private final Map<NodeId, Integer> acknowledgments = new HashMap<>();
    private List<Request> log = new ArrayList<>();
    private long view, lastNormal, lastContact, lastSent = Long.MIN_VALUE, detectedAt = -1, readyAt = -1;
    private State state = State.NORMAL;
    private int commit, applied;
    private boolean applying;
    private volatile Observation observation = new Observation(0, State.NORMAL, 0, 0, 0, -1, -1);

    /** 모든 노드에 같은 members를 제공한다. NodeId 정렬 후 view mod 3으로 Primary를 결정한다. */
    public VrEngine(NodeId self, List<NodeId> members, CacheEngine kv, Mailbox owner, Transport transport,
            TimeSource time, Duration heartbeat, Duration timeout) {
        this(self,members,kv,owner,transport,time,heartbeat,timeout,Mode.INCREMENTAL);
    }
    /** 실험 재현용 전송 모드를 선택한다. INCREMENTAL의 view-change 대기 예산은 최소 3초다. */
    public VrEngine(NodeId self, List<NodeId> members, CacheEngine kv, Mailbox owner, Transport transport,
            TimeSource time, Duration heartbeat, Duration timeout,Mode mode) {
        this.self = Objects.requireNonNull(self);
        this.members = members.stream().sorted(Comparator.comparingLong(NodeId::value)).toList();
        if (this.members.size() != 3 || new HashSet<>(members).size() != 3 || !members.contains(self)
                || heartbeat.isNegative() || heartbeat.isZero() || timeout.compareTo(heartbeat) <= 0)
            throw new IllegalArgumentException("VR requires 3 distinct members and timeout > heartbeat > 0");
        this.kv = Objects.requireNonNull(kv); this.owner = Objects.requireNonNull(owner);
        this.transport = Objects.requireNonNull(transport); this.time = Objects.requireNonNull(time);
        this.heartbeat = heartbeat.toNanos(); this.timeout = timeout.toNanos();
        this.mode=Objects.requireNonNull(mode);
        viewChangeWait=mode==Mode.FULL_LOG?this.timeout:Math.max(this.timeout,Duration.ofSeconds(3).toNanos());
        lastContact = time.monotonicNanos();
    }

    @Override public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        CompletableFuture<Result> future = new CompletableFuture<>();
        futures.add(future); future.whenComplete((r, e) -> futures.remove(future));
        if (!owner.offer(() -> request(new Request(context.clientId(), context.operationId(), command), context, future)))
            future.complete(status(ResultStatus.BUSY));
        return future;
    }
    private void request(Request request, RequestContext context, CompletableFuture<Result> future) {
        if (closed.get() || state == State.FAILED) { future.complete(status(ResultStatus.UNAVAILABLE)); return; }
        if (context.deadline().isExpired(time)) { future.complete(status(ResultStatus.TIMEOUT)); return; }
        if (context.ownershipEpoch().value() < view) { future.complete(status(ResultStatus.STALE_EPOCH)); return; }
        if (state != State.NORMAL) { future.complete(status(ResultStatus.UNAVAILABLE)); return; }
        if (!primary().equals(self)) {
            future.complete(Result.redirect(ResultStatus.NOT_PRIMARY, new Redirect(primary()), RoutingVersion.ZERO, epoch())); return;
        }
        Key key = key(request);
        Request previous = appliedRequests.get(key);
        if (previous != null) {
            future.complete(previous.equals(request) ? withEpoch(replies.get(key)) : status(ResultStatus.BAD_REQUEST)); return;
        }
        Request entry=loggedRequests.get(key);
        if(entry!=null) {
            if (!entry.equals(request)) future.complete(status(ResultStatus.BAD_REQUEST));
            else waiting.add(new Waiting(request, context, future));
            return;
        }
        // 순차 실행 기준형: 미확정/미적용 suffix가 있을 때 새 요청은 재시도한다.
        if (applied != log.size()) { future.complete(status(ResultStatus.BUSY)); return; }
        log.add(request); loggedRequests.put(key,request); waiting.add(new Waiting(request, context, future));
        acknowledgments.put(self, log.size()); replicate(); publish();
    }

    @Override public void onPeerMessage(NodeId from, PeerMessage message) {
        if (!(message instanceof VrMessage vr) || !members.contains(from) || from.equals(self)) return;
        owner.offer(() -> handle(from, vr));
    }
    private void handle(NodeId from, VrMessage message) {
        if (closed.get() || state == State.FAILED) return;
        if (message instanceof GetState) {
            if (primary().equals(self) && (state == State.NORMAL || state == State.INSTALLING)) sendSnapshot(from);
            return;
        }
        if (message.view() < view) {
            if (primary().equals(self) && state == State.NORMAL) sendSnapshot(from);
            return;
        }
        // ACK만으로 더 높은 view를 채택하지 않는다.
        if (message instanceof PrepareOk ack) {
            if (ack.view() == view && primary().equals(self) && (state == State.NORMAL || state == State.INSTALLING)
                    && ack.length() <= log.size()) {
                acknowledgeTransfer(from,ack.length(),false);
                acknowledgments.merge(from, ack.length(), Math::max); advanceCommit();
                if(mode==Mode.INCREMENTAL && state==State.NORMAL) sendTail(from);
            }
        } else if(message instanceof ViewChangeAck ack) {
            if(ack.view()==view && from.equals(primary())) acknowledgeTransfer(from,0,true);
        } else if (message instanceof StartViewChange change) {
            if (change.view() > view) beginView(change.view());
            if (state == State.VIEW_CHANGE) { starters.add(from); sendChange(); }
        } else if (message instanceof DoViewChange change) {
            if (change.view() > view) beginView(change.view());
            if (state == State.VIEW_CHANGE && primary().equals(self)) {
                changes.putIfAbsent(from, change);
                if(mode==Mode.INCREMENTAL) send(from,new ViewChangeAck(view));
                selectLog();
            }
            else if(mode==Mode.INCREMENTAL && primary().equals(self) && (state==State.NORMAL || state==State.INSTALLING)) {
                send(from,new ViewChangeAck(view)); sendSnapshot(from);
            }
        } else if (message instanceof StartView start) {
            if (!from.equals(primary(start.view()))) return;
            if (start.view() > view) beginView(start.view());
            if (state == State.VIEW_CHANGE) {
                installLog(start.log(), start.commit()); state = State.NORMAL; lastNormal = view;
                acknowledgeTransfer(from,0,true);
                lastContact = time.monotonicNanos(); applyNext();
                send(from, new PrepareOk(view, log.size()));
            } else if (state == State.NORMAL) receiveLog(from, start.log(), start.commit());
        } else if (message instanceof Prepare prepare) {
            if (!from.equals(primary(prepare.view()))) return;
            if (prepare.view() > view) beginView(prepare.view());
            if (state == State.NORMAL) receiveLog(from, prepare.log(), prepare.commit());
            else send(from, new GetState(view));
        } else if(message instanceof DeltaPrepare prepare) {
            if(!from.equals(primary(prepare.view()))) return;
            if(prepare.view()>view) beginView(prepare.view());
            if(state==State.NORMAL) receiveDelta(from,prepare);
            else send(from,new GetState(view));
        } else if(message instanceof Commit notice) {
            if(!from.equals(primary(notice.view())) || from.equals(self)) return;
            if(notice.view()>view) beginView(notice.view());
            lastContact=time.monotonicNanos();
            if(state==State.NORMAL && notice.commit()<=log.size()) {
                commit=Math.max(commit,notice.commit()); applyNext();
                send(from,new PrepareOk(view,log.size()));
            } else send(from,new GetState(view));
        }
        publish();
    }

    private void receiveDelta(NodeId from,DeltaPrepare prepare) {
        if(primary().equals(self)) return;
        lastContact=time.monotonicNanos(); int start=prepare.firstOp()-1;
        if(start>log.size()) { send(from,new GetState(view)); return; }
        for(int i=0;i<prepare.entries().size();i++) {
            Request entry=prepare.entries().get(i); int position=start+i;
            if(position<log.size()) {
                if(!log.get(position).equals(entry)) throw new IllegalStateException("conflicting incremental log prefix");
            } else {
                if(loggedRequests.containsKey(key(entry))) throw new IllegalStateException("duplicate incremental request");
                log.add(entry); loggedRequests.put(key(entry),entry);
            }
        }
        if(prepare.commit()>log.size()) { send(from,new GetState(view)); return; }
        commit=Math.max(commit,prepare.commit()); applyNext(); send(from,new PrepareOk(view,log.size()));
    }

    private void receiveLog(NodeId from, List<Request> entries, int committed) {
        if (primary().equals(self)) return;
        lastContact = time.monotonicNanos();
        if (entries.size() >= log.size()) {
            // 같은 view에서는 append-only다. 지연된 짧은 메시지는 로그를 줄이지 않는다.
            checkPrefix(log, entries, log.size());
            installLog(entries, Math.max(commit, committed));
        } else checkPrefix(entries, log, entries.size());
        applyNext(); send(from, new PrepareOk(view, log.size()));
    }
    private void beginView(long next) {
        if (next <= view) return;
        view = next; state = State.VIEW_CHANGE; detectedAt = time.monotonicNanos(); readyAt = -1;
        lastContact = detectedAt; lastSent = Long.MIN_VALUE;
        starters.clear(); starters.add(self); changes.clear(); acknowledgments.clear();
        transfers.clear();
        finishWaiting(ResultStatus.UNAVAILABLE);
        broadcast(new StartViewChange(view)); publish();
    }
    private void sendChange() {
        if (starters.size() < 2 || state != State.VIEW_CHANGE) return;
        if(mode==Mode.INCREMENTAL) {
            if(primary().equals(self) && changes.containsKey(self)) { selectLog(); return; }
            Transfer previous=transfers.get(primary());
            if(!primary().equals(self) && previous!=null && time.monotonicNanos()<previous.due()) return;
        }
        var change = new DoViewChange(view, lastNormal, log, commit);
        if (primary().equals(self)) { changes.putIfAbsent(self, change); selectLog(); }
        else sendTransfer(primary(), change,log.size());
    }
    private void selectLog() {
        if (changes.size() < 2 || state != State.VIEW_CHANGE) return;
        DoViewChange chosen = changes.values().stream().max(Comparator.comparingLong(DoViewChange::lastNormal)
                .thenComparingInt(c -> c.log().size())).orElseThrow();
        int safeCommit = changes.values().stream().mapToInt(DoViewChange::commit).max().orElseThrow();
        // 받은 모든 commit prefix가 선택된 로그와 같아야 한다. 짧은 최신 로그를 임의로 덧붙이지 않는다.
        for (var report : changes.values()) checkPrefix(report.log(), chosen.log(), report.commit());
        installLog(chosen.log(), safeCommit);
        state = State.INSTALLING; lastNormal = view; acknowledgments.clear(); acknowledgments.put(self, log.size());
        applyNext(); installView(); publish();
    }
    private void installLog(List<Request> entries, int committed) {
        checkPrefix(log, entries, commit);
        if (committed > entries.size()) throw new IllegalStateException("committed prefix missing");
        Set<Key> unique = new HashSet<>();
        for (Request request : entries) if (!unique.add(key(request))) throw new IllegalStateException("duplicate request in log");
        log = new ArrayList<>(entries); commit = Math.max(commit, committed);
        loggedRequests.clear(); for(Request request:log) loggedRequests.put(key(request),request);
    }
    private static void checkPrefix(List<Request> a, List<Request> b, int length) {
        if (a.size() < length || b.size() < length || !a.subList(0, length).equals(b.subList(0, length)))
            throw new IllegalStateException("conflicting committed/normal-view log prefix");
    }
    private void advanceCommit() {
        List<Integer> lengths = acknowledgments.values().stream().sorted(Comparator.reverseOrder()).toList();
        if (lengths.size() < 2) return;
        int quorumLength = lengths.get(1);
        if (state == State.INSTALLING && quorumLength == log.size()) {
            state = State.NORMAL;
        }
        if (quorumLength > commit) {
            commit = quorumLength;
            broadcast(mode==Mode.FULL_LOG?new Prepare(view,log,commit):new Commit(view,commit));
        }
        applyNext(); publish();
    }

    private void applyNext() {
        if (closed.get() || state == State.FAILED || applying || applied >= commit) return;
        Request request = log.get(applied); int index = applied;
        applying = true;
        var context = new RequestContext(request.client(), request.operation(), RoutingVersion.ZERO, epoch(),
                Deadline.after(time, Duration.ofDays(1)));
        kv.execute(request.command(), context).whenComplete((r, error) -> {
            if (!owner.offer(() -> {
                applying = false;
                if (closed.get()) return;
                if (error != null || (r.status() != ResultStatus.OK && r.status() != ResultStatus.MISS)) {
                    state = State.FAILED; finishWaiting(ResultStatus.UNAVAILABLE); publish(); return;
                }
                if (applied != index || !log.get(index).equals(request)) throw new IllegalStateException("applying entry changed");
                applied++; replies.put(key(request), r); appliedRequests.put(key(request), request);
                // KV callback이 view change 뒤 도착해도 이전 요청에 늦은 OK를 보내지 않는다.
                Iterator<Waiting> iterator = waiting.iterator();
                while (iterator.hasNext()) {
                    Waiting w = iterator.next();
                    if (w.request().equals(request)) {
                        w.future().complete(w.context().deadline().isExpired(time) ? status(ResultStatus.TIMEOUT) : withEpoch(r)); iterator.remove();
                    }
                }
                publish(); applyNext();
            })) {
                // 적용 결과를 잃은 상태에서 진행하지 않는다. 실패 시 외부 재조립/recovery가 필요하다.
                closed.set(true); futures.forEach(f -> f.complete(status(ResultStatus.UNAVAILABLE)));
            }
        });
    }
    @Override public void onTimer(TimerEvent event) {
        if (!TICK.equals(event.name())) return;
        owner.offer(() -> {
            if (closed.get() || state == State.FAILED) return;
            waiting.removeIf(w -> {
                if (w.context().deadline().isExpired(time)) { w.future().complete(status(ResultStatus.TIMEOUT)); return true; }
                return w.future().isDone();
            });
            long now = time.monotonicNanos();
            long budget=state==State.VIEW_CHANGE || state==State.INSTALLING?viewChangeWait:timeout;
            if ((state == State.VIEW_CHANGE || state == State.INSTALLING || !primary().equals(self)) && now-lastContact >= budget)
                beginView(Math.addExact(view, 1));
            if (lastSent == Long.MIN_VALUE || now-lastSent >= heartbeat) {
                lastSent = now;
                if (state == State.VIEW_CHANGE) { broadcast(new StartViewChange(view)); sendChange(); }
                else if (primary().equals(self)) {
                    if(state==State.INSTALLING) installView();
                    else if(mode==Mode.FULL_LOG) broadcast(new Prepare(view,log,commit));
                    else { broadcast(new Commit(view,commit)); replicate(); }
                }
            }
            applyNext(); publish();
        });
    }
    private void replicate() {
        if(mode==Mode.FULL_LOG) broadcast(new Prepare(view,log,commit));
        else for(NodeId node:members) if(!node.equals(self)) sendTail(node);
    }
    private void sendTail(NodeId node) {
        int length=acknowledgments.getOrDefault(node,0);
        if(length>=log.size()) return;
        Transfer previous=transfers.get(node);
        if(previous!=null && time.monotonicNanos()<previous.due()) return;
        if(previous!=null && previous.message() instanceof StartView) sendSnapshot(node);
        else sendTransfer(node,new DeltaPrepare(view,length+1,log.subList(length,log.size()),commit),log.size());
    }
    private void installView() {
        for(NodeId node:members) if(!node.equals(self)) sendSnapshot(node);
    }
    private void sendSnapshot(NodeId node) {
        Transfer previous=transfers.get(node);
        if(mode==Mode.INCREMENTAL && previous!=null && previous.message() instanceof StartView && time.monotonicNanos()<previous.due()) return;
        sendTransfer(node,new StartView(view,log,commit),log.size());
    }
    private void sendTransfer(NodeId node,VrMessage message,int length) {
        if(mode==Mode.FULL_LOG) { send(node,message); return; }
        long now=time.monotonicNanos(); Transfer previous=transfers.get(node);
        // 상태 요청은 기존 증분 전송을 한 번 승격한다. 다음 timer가 다시 증분으로 바꾸어
        // STARTVIEW를 굶기지 않도록, 설치 ACK 전에는 snapshot 재전송 우선순위를 유지한다.
        if(previous!=null && previous.message() instanceof StartView && message instanceof DeltaPrepare) {
            message=new StartView(view,log,commit); length=log.size();
        }
        boolean upgrade=previous!=null && previous.message() instanceof DeltaPrepare && message instanceof StartView;
        if(previous!=null && !upgrade && now<previous.due()) return;
        long interval=previous==null || upgrade?RETRY_BASE:Math.min(RETRY_MAX,previous.interval()*2);
        transfers.put(node,new Transfer(message,length,now+interval,interval)); send(node,message);
    }
    private void acknowledgeTransfer(NodeId from,int length,boolean viewChange) {
        Transfer transfer=transfers.get(from);
        if(transfer==null) return;
        boolean dvc=transfer.message() instanceof DoViewChange;
        if(viewChange?dvc:!dvc && length>=transfer.length()) transfers.remove(from);
    }
    private void finishWaiting(ResultStatus result) { waiting.forEach(w -> w.future().complete(status(result))); waiting.clear(); }
    private Key key(Request request) { return new Key(request.client(), request.operation()); }
    private NodeId primary() { return primary(view); }
    private NodeId primary(long v) { return members.get((int)(v % members.size())); }
    private OwnershipEpoch epoch() { return new OwnershipEpoch(view); }
    private Result status(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, new OwnershipEpoch(observation.view())); }
    private Result withEpoch(Result r) { return new Result(r.status(), r.value(), r.valueVersion(), r.routingVersion(), epoch(), r.redirect()); }
    private void send(NodeId to, VrMessage message) { transport.send(new PeerEnvelope(self, to, message)); }
    private void broadcast(VrMessage message) { for (NodeId node : members) if (!node.equals(self)) send(node, message); }
    private void publish() {
        if (view > 0 && state == State.NORMAL && primary().equals(self) && readyAt < 0 && applied == log.size())
            readyAt = time.monotonicNanos();
        observation = new Observation(view, state, log.size(), commit, applied, detectedAt, readyAt);
    }
    /** 마지막 owner 작업이 발행한 상태 스냅샷. 다른 thread에서 읽어도 된다. */
    public Observation observation() { return observation; }
    /** 진단용 로그 사본. owner에서 원자적으로 읽으며 실험의 보존 검사에 사용한다. 호출 thread를 block하지 않는다. */
    public CompletionStage<List<Request>> logSnapshot() {
        CompletableFuture<List<Request>> result=new CompletableFuture<>();
        if(!owner.offer(()->result.complete(List.copyOf(log))))
            result.completeExceptionally(new IllegalStateException("diagnostic mailbox full"));
        return result;
    }
    @Override public EngineSnapshot snapshot() { return new EngineSnapshot(self, time.monotonicNanos(), RoutingVersion.ZERO, new OwnershipEpoch(observation.view())); }
    @Override public void close() { closed.set(true); futures.forEach(f -> f.complete(status(ResultStatus.UNAVAILABLE))); }
}
