package io.openkache.core.replication.ionia;

import io.openkache.core.api.CacheEngine;
import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.EngineSnapshot;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.replication.ionia.IoniaLog.RequestId;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.spi.Transport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 고정 leader/view의 3노드 메모리 복제 상태 머신. 모든 복제 상태는 주입한 owner에서만 변경한다.
 *
 * <p>Client SET은 recordWrite로 기록 ACK를 받고, leader는 별도로 순서 제안/과반수 수락/commit을 진행한다.
 * commit된 쓰기는 순서대로 로컬 KV에 적용한다. execute의 GET/DELETE는 leader에서 처리하며 GET은 앞선 쓰기의
 * 적용을 기다린다. follower GET은 NOT_PRIMARY로 거절하므로 오래된 로컬 값을 성공으로 반환하지 않는다.
 *
 * <p>주기적인 TICK timer는 외부에서 전달한다. 미확인 순서 로그/commit을 재전송하고 요청 deadline을 검사한다.
 * 로그 압축, follower 읽기 검증, leader 변경, 재시작 복구, 영속화는 없다. KV와 전송 계층은 외부 소유다.
 * KV 적용 실패 또는 내부 완료 이벤트의 mailbox 거절 시 재적용을 추측하지 않고 Engine을 fail-stop한다.
 */
public final class IoniaEngine implements CacheEngine {

    /** 외부 실행부가 주기적으로 전달할 timer 이름. */
    public static final String TICK = "ionia.tick";

    /** 어느 thread에서나 읽을 수 있는 불변 복제 진행 정보. index는 KV의 valueVersion과 별개다. */
    public record Progress(NodeId nodeId, int recordedCount, long lastLogIndex, long commitIndex, long appliedIndex) {}

    private record Waiting(RequestContext context, CompletableFuture<Result> future) {}
    private record ReadWait(Command.Get command, Waiting waiting, long barrier) {}
    private record RecordCall(IoniaClientTransport.Request request, Deadline deadline) {}

    private final IoniaConfig config;
    private final IoniaLog log = new IoniaLog();
    private final CacheEngine localKv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource timeSource;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<CompletableFuture<Result>, Deadline> pending = new ConcurrentHashMap<>();
    private final Map<CompletableFuture<IoniaClientTransport.WriteAck>, RecordCall> recordCalls =
            new ConcurrentHashMap<>();
    // 아래 상태는 owner에서만 접근한다.
    private final Map<NodeId, Long> matched = new HashMap<>();
    private final Map<RequestId, Result> appliedResults = new HashMap<>();
    private final Map<RequestId, List<Waiting>> writes = new HashMap<>();
    private final List<ReadWait> reads = new ArrayList<>();
    private long announcedCommit;
    private boolean applying;
    private volatile Progress progress;

    /** 노드별 구성 요소를 연결한다. owner/KV/transport의 생성·종료는 외부 조립부가 맡는다. */
    public IoniaEngine(
            IoniaConfig config, CacheEngine localKv, Mailbox owner, Transport transport, TimeSource timeSource) {
        this.config = Objects.requireNonNull(config, "config");
        this.localKv = Objects.requireNonNull(localKv, "localKv");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
        config.replicas().forEach(node -> matched.put(node, 0L));
        publish();
    }

    /**
     * SET의 메모리 기록 ACK 진입점. 같은 ID/내용의 재전송은 OK, 같은 ID의 다른 내용은 BAD_REQUEST다.
     * 완료된 ACK는 commit/KV 적용을 기다리지 않는다. 상대 timeout은 이 메서드 진입 시 수신 JVM 시계로 변환한다.
     */
    public CompletionStage<IoniaClientTransport.WriteAck> recordWrite(IoniaClientTransport.Request request) {
        Objects.requireNonNull(request, "request");
        RequestContext context = request.contextAt(timeSource);
        CompletableFuture<IoniaClientTransport.WriteAck> future = new CompletableFuture<>();
        recordCalls.put(future, new RecordCall(request, context.deadline()));
        future.whenComplete((reply, error) -> recordCalls.remove(future));
        if (closed.get()) {
            future.complete(ack(request, ResultStatus.UNAVAILABLE));
        } else if (!offerOwner(() -> {
            if (closed.get()) {
                future.complete(ack(request, ResultStatus.UNAVAILABLE));
            } else if (context.deadline().isExpired(timeSource)) {
                future.complete(ack(request, ResultStatus.TIMEOUT));
            } else if (request.view() != config.view() || !(request.command() instanceof Command.Set)
                    || !supportedContext(context)) {
                future.complete(ack(request, ResultStatus.BAD_REQUEST));
            } else {
                try {
                    log.record(request.clientId(), request.operationId(), request.command());
                } catch (IllegalArgumentException error) {
                    future.complete(ack(request, ResultStatus.BAD_REQUEST));
                    return;
                }
                future.complete(ack(request, ResultStatus.OK));
                if (config.isLeader()) {
                    order(request.command(), context);
                }
            }
        })) {
            future.complete(ack(request, ResultStatus.BUSY));
        }
        return future;
    }

    /** Client 전송 어댑터용 실행 진입점. 현재 view를 검증하고 응답 식별자를 붙인다. */
    public CompletionStage<IoniaClientTransport.Reply> execute(IoniaClientTransport.Request request) {
        Objects.requireNonNull(request, "request");
        CompletionStage<Result> result = request.view() == config.view()
                ? execute(request.command(), request.contextAt(timeSource))
                : CompletableFuture.completedFuture(resultOf(ResultStatus.BAD_REQUEST));
        return result.thenApply(value -> new IoniaClientTransport.Reply(config.nodeId(), config.view(),
                request.clientId(), request.operationId(), value));
    }

    /**
     * leader GET/SET/DELETE 실행. SET/DELETE는 commit 후 KV의 실제 결과를 반환한다.
     * 빠른 SET의 기록 ACK는 이 메서드가 아닌 recordWrite를 사용한다.
     */
    @Override
    public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(context, "context");
        CompletableFuture<Result> future = new CompletableFuture<>();
        pending.put(future, context.deadline());
        future.whenComplete((value, error) -> pending.remove(future));
        if (closed.get()) {
            future.complete(resultOf(ResultStatus.UNAVAILABLE));
        } else if (!offerOwner(() -> executeOnOwner(command, context, future))) {
            future.complete(resultOf(ResultStatus.BUSY));
        }
        return future;
    }

    private void executeOnOwner(Command command, RequestContext context, CompletableFuture<Result> future) {
        if (closed.get()) {
            future.complete(resultOf(ResultStatus.UNAVAILABLE));
        } else if (context.deadline().isExpired(timeSource)) {
            future.complete(resultOf(ResultStatus.TIMEOUT));
        } else if (!supportedContext(context)) {
            future.complete(resultOf(ResultStatus.BAD_REQUEST));
        } else if (!config.isLeader()) {
            future.complete(resultOf(ResultStatus.NOT_PRIMARY));
        } else if (command instanceof Command.Get get) {
            reads.add(new ReadWait(get, new Waiting(context, future), log.lastLogIndex()));
            drainReads();
        } else {
            try {
                log.record(context.clientId(), context.operationId(), command);
            } catch (IllegalArgumentException error) {
                future.complete(resultOf(ResultStatus.BAD_REQUEST));
                return;
            }
            RequestId id = new RequestId(context.clientId(), context.operationId());
            Result existing = appliedResults.get(id);
            if (existing != null) {
                finish(new Waiting(context, future), existing);
                return;
            }
            writes.computeIfAbsent(id, ignored -> new ArrayList<>()).add(new Waiting(context, future));
            order(command, context);
        }
    }

    private void order(Command command, RequestContext context) {
        log.order(context.clientId(), context.operationId(), command);
        matched.put(config.nodeId(), log.lastLogIndex());
        replicate();
    }

    /** 송신 노드와 view를 검사한 후 owner에서 메시지를 처리한다. 잘못된/알 수 없는 메시지는 무시한다. */
    @Override
    public void onPeerMessage(NodeId from, PeerMessage message) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(message, "message");
        if (closed.get() || !config.replicas().contains(from) || from.equals(config.nodeId())
                || !(message instanceof IoniaMessage ionia) || ionia.view() != config.view()) {
            return;
        }
        offerOwner(() -> {
            if (closed.get()) {
                return;
            }
            switch (ionia) {
                case IoniaMessage.Prepare prepare -> acceptPrepare(from, prepare);
                case IoniaMessage.PrepareAck ack -> acceptAck(from, ack);
                case IoniaMessage.Commit commit -> acceptCommit(from, commit);
            }
        });
        // 큐가 가득 차 수신 이벤트를 놓치면 다음 TICK의 재전송으로 다시 시도한다.
    }

    private void acceptPrepare(NodeId from, IoniaMessage.Prepare prepare) {
        if (config.isLeader() || !from.equals(config.leaderId())) {
            return;
        }
        try {
            log.accept(prepare);
        } catch (IllegalArgumentException error) {
            return;
        }
        log.commitThrough(Math.min(announcedCommit, log.lastLogIndex()));
        send(config.leaderId(), new IoniaMessage.PrepareAck(config.view(), log.lastLogIndex()));
        applyNext();
    }

    private void acceptAck(NodeId from, IoniaMessage.PrepareAck ack) {
        if (!config.isLeader() || ack.lastLogIndex() > log.lastLogIndex()) {
            return;
        }
        matched.merge(from, ack.lastLogIndex(), Math::max);
        List<Long> indices = matched.values().stream().sorted().toList();
        long quorumIndex = indices.get(indices.size() - config.orderingQuorumSize());
        if (quorumIndex > log.commitIndex()) {
            log.commitThrough(quorumIndex);
            broadcastCommit();
            applyNext();
        }
    }

    private void acceptCommit(NodeId from, IoniaMessage.Commit commit) {
        if (config.isLeader() || !from.equals(config.leaderId())) {
            return;
        }
        announcedCommit = Math.max(announcedCommit, commit.commitIndex());
        // Commit이 Prepare보다 먼저 도착해도 없는 항목을 적용했다고 표시하지 않는다.
        log.commitThrough(Math.min(announcedCommit, log.lastLogIndex()));
        applyNext();
    }

    private void replicate() {
        for (NodeId node : config.replicas()) {
            if (!node.equals(config.nodeId())) {
                List<IoniaMessage.LogEntry> entries = log.entriesFrom(matched.get(node) + 1);
                if (!entries.isEmpty()) {
                    send(node, new IoniaMessage.Prepare(config.view(), entries));
                }
            }
        }
    }

    private void broadcastCommit() {
        for (NodeId node : config.replicas()) {
            if (!node.equals(config.nodeId())) {
                send(node, new IoniaMessage.Commit(config.view(), log.commitIndex()));
            }
        }
    }

    private void send(NodeId to, IoniaMessage message) {
        try {
            // 로컬 전송 완료를 원격 ACK로 세지 않는다. 실패/유실은 TICK에서 미확인 구간을 재전송한다.
            transport.send(new PeerEnvelope(config.nodeId(), to, message)).exceptionally(error -> null);
        } catch (RuntimeException error) {
            // 동기 전송 거절도 상태를 되돌리지 않고 다음 TICK에서 재시도한다.
        }
    }

    private void applyNext() {
        if (closed.get() || applying || log.appliedIndex() == log.commitIndex()) {
            return;
        }
        IoniaMessage.LogEntry entry = log.entry(log.appliedIndex() + 1).orElseThrow();
        applying = true;
        // Client의 만료된 deadline으로 확정된 쓰기의 적용을 건너뛰지 않는다.
        RequestContext internal = new RequestContext(entry.clientId(), entry.operationId(),
                RoutingVersion.ZERO, OwnershipEpoch.ZERO,
                new Deadline(timeSource.monotonicNanos() + Long.MAX_VALUE));
        try {
            localKv.execute(entry.command(), internal).whenComplete((result, error) -> {
                if (!offerOwner(() -> applied(entry, result, error))) {
                    close();
                }
            });
        } catch (RuntimeException error) {
            close();
        }
    }

    private void applied(IoniaMessage.LogEntry entry, Result result, Throwable error) {
        if (closed.get()) {
            return;
        }
        boolean success = error == null && result != null
                && (result.status() == ResultStatus.OK
                    || (entry.command() instanceof Command.Delete && result.status() == ResultStatus.MISS));
        if (!success) {
            close();
            return;
        }
        applying = false;
        log.markApplied(entry.index());
        RequestId id = new RequestId(entry.clientId(), entry.operationId());
        appliedResults.put(id, result);
        List<Waiting> waiters = writes.remove(id);
        if (waiters != null) {
            waiters.forEach(waiting -> finish(waiting, result));
        }
        drainReads();
        applyNext();
    }

    private void drainReads() {
        Iterator<ReadWait> iterator = reads.iterator();
        while (iterator.hasNext()) {
            ReadWait read = iterator.next();
            if (read.waiting().future().isDone()) {
                iterator.remove();
            } else if (read.barrier() <= log.appliedIndex()) {
                iterator.remove();
                Waiting waiting = read.waiting();
                if (waiting.context().deadline().isExpired(timeSource)) {
                    finish(waiting, resultOf(ResultStatus.TIMEOUT));
                } else {
                    try {
                        localKv.execute(read.command(), waiting.context()).whenComplete((result, error) ->
                                finish(waiting, error == null && result != null
                                        ? result : resultOf(ResultStatus.INTERNAL_ERROR)));
                    } catch (RuntimeException error) {
                        finish(waiting, resultOf(ResultStatus.INTERNAL_ERROR));
                    }
                }
            }
        }
    }

    private void finish(Waiting waiting, Result result) {
        Result value = closed.get() ? resultOf(ResultStatus.UNAVAILABLE)
                : waiting.context().deadline().isExpired(timeSource) ? resultOf(ResultStatus.TIMEOUT) : result;
        waiting.future().complete(value);
    }

    /**
     * TICK에서 deadline을 검사하고 leader의 미확인 로그와 commit을 재전송한다.
     * timeout은 이미 기록한 쓰기를 삭제하지 않는다. 시간은 event 값 대신 주입한 TimeSource로 확인한다.
     */
    @Override
    public void onTimer(TimerEvent event) {
        Objects.requireNonNull(event, "event");
        if (!TICK.equals(event.name()) || closed.get()) {
            return;
        }
        pending.forEach((future, deadline) -> {
            if (deadline.isExpired(timeSource)) {
                future.complete(resultOf(ResultStatus.TIMEOUT));
            }
        });
        recordCalls.forEach((future, call) -> {
            if (call.deadline().isExpired(timeSource)) {
                future.complete(ack(call.request(), ResultStatus.TIMEOUT));
            }
        });
        offerOwner(() -> {
            if (closed.get()) {
                return;
            }
            writes.values().forEach(waiters -> waiters.removeIf(waiting -> waiting.future().isDone()));
            writes.values().removeIf(List::isEmpty);
            drainReads();
            if (config.isLeader()) {
                replicate();
                broadcastCommit();
            }
            applyNext();
        });
    }

    /** 복제 진행 정보의 불변 복사본. 원시 Log를 다른 thread에 노출하지 않는다. */
    public Progress progress() {
        return progress;
    }

    /** 공통 snapshot은 메타데이터이며 데이터 복구 snapshot이 아니다. */
    @Override
    public EngineSnapshot snapshot() {
        return new EngineSnapshot(config.nodeId(), timeSource.monotonicNanos(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }

    /** 대기 요청을 UNAVAILABLE로 완료한다. 외부 소유 KV/owner/transport는 닫지 않는다. */
    @Override
    public void close() {
        closed.set(true);
        pending.keySet().forEach(future -> future.complete(resultOf(ResultStatus.UNAVAILABLE)));
        recordCalls.forEach((future, call) -> future.complete(ack(call.request(), ResultStatus.UNAVAILABLE)));
    }

    private boolean offerOwner(Runnable task) {
        return owner.offer(() -> {
            try {
                task.run();
            } catch (RuntimeException error) {
                close();
            } finally {
                publish();
            }
        });
    }

    private void publish() {
        progress = new Progress(config.nodeId(), log.recordedCount(), log.lastLogIndex(),
                log.commitIndex(), log.appliedIndex());
    }

    private IoniaClientTransport.WriteAck ack(IoniaClientTransport.Request request, ResultStatus status) {
        return new IoniaClientTransport.WriteAck(config.nodeId(), config.view(),
                request.clientId(), request.operationId(), status);
    }

    private static boolean supportedContext(RequestContext context) {
        return RoutingVersion.ZERO.equals(context.routingVersion())
                && OwnershipEpoch.ZERO.equals(context.ownershipEpoch());
    }

    private static Result resultOf(ResultStatus status) {
        return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }
}
