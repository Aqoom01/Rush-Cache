package io.openkache.core.replication.ionia;

import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 고정 view/3노드 메모리 프로토타입의 클라이언트. 여러 thread에서 호출할 수 있으며 block하지 않는다.
 *
 * <p>SET은 세 노드 모두의 기록 ACK로 완료한다. GET은 leader에서만 읽고 DELETE는 leader의 commit/적용 결과를
 * 기다린다. 기록 ACK는 SSD 내구성이나 KV 적용 완료를 뜻하지 않는다. follower 읽기 검증, quorum fallback,
 * leader 변경은 아직 없다. 주소와 TCP는 주입한 전송 구현이 맡는다.
 *
 * <p>호출자는 대기 요청의 deadline 처리를 위해 주기적으로 onTimer를 호출해야 한다. 자체 timer thread를 만들지 않는다.
 */
public final class IoniaClient implements AutoCloseable {

    private final ClientId clientId;
    private final List<NodeId> replicas;
    private final NodeId leaderId;
    private final long view;
    private final IoniaClientTransport transport;
    private final TimeSource timeSource;
    private final Map<CompletableFuture<Result>, Deadline> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * 고정 그룹과 비차단 전송 기능을 연결한다. transport의 생명주기는 호출자가 관리한다.
     * Client는 복제 노드가 아니며 Engine이나 로컬 KV를 생성하지 않는다.
     */
    public IoniaClient(
            ClientId clientId, List<NodeId> replicas, NodeId leaderId, long view,
            IoniaClientTransport transport, TimeSource timeSource) {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.replicas = List.copyOf(Objects.requireNonNull(replicas, "replicas"));
        this.leaderId = Objects.requireNonNull(leaderId, "leaderId");
        if (this.replicas.size() != IoniaConfig.REPLICA_COUNT
                || new HashSet<>(this.replicas).size() != this.replicas.size()) {
            throw new IllegalArgumentException("replicas must contain exactly three distinct nodes");
        }
        if (!this.replicas.contains(leaderId)) {
            throw new IllegalArgumentException("leaderId must belong to replicas");
        }
        if (view < 0) {
            throw new IllegalArgumentException("view must be >= 0");
        }
        this.view = view;
        this.transport = Objects.requireNonNull(transport, "transport");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    }

    /**
     * 논리 요청을 분기한다. 같은 쓰기의 재시도에는 context의 ClientId/OperationId와 내용을 유지해야 한다.
     * 반환 stage의 timeout은 서버의 쓰기 취소나 rollback을 의미하지 않는다.
     */
    public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(context, "context");
        if (closed.get()) {
            return completed(ResultStatus.UNAVAILABLE);
        }
        if (!clientId.equals(context.clientId())) {
            return completed(ResultStatus.BAD_REQUEST);
        }
        if (context.deadline().isExpired(timeSource)) {
            return completed(ResultStatus.TIMEOUT);
        }
        CompletableFuture<Result> future = new CompletableFuture<>();
        pending.put(future, context.deadline());
        future.whenComplete((result, error) -> pending.remove(future));
        if (closed.get()) {
            future.complete(resultOf(ResultStatus.UNAVAILABLE));
            return future;
        }
        switch (command) {
            case Command.Set set -> executeSet(set, context, future);
            case Command.Get get -> executeGet(get, context, future);
            case Command.Delete delete -> executeDelete(delete, context, future);
        }
        return future;
    }

    private void executeSet(Command.Set set, RequestContext context, CompletableFuture<Result> future) {
        Set<NodeId> acknowledged = ConcurrentHashMap.newKeySet();
        // 세 비차단 전송을 시작한다. 전송 stage의 성공이 아닌 각 노드의 기록 응답을 센다.
        for (NodeId node : replicas) {
            try {
                transport.record(node, request(set, context)).whenComplete((ack, error) -> {
                    if (future.isDone()) {
                        return;
                    }
                    if (context.deadline().isExpired(timeSource)) {
                        future.complete(resultOf(ResultStatus.TIMEOUT));
                    } else if (error != null || ack == null) {
                        future.complete(resultOf(ResultStatus.UNAVAILABLE));
                    } else if (!node.equals(ack.nodeId()) || ack.view() != view
                            || !clientId.equals(ack.clientId())
                            || !context.operationId().equals(ack.operationId())) {
                        future.complete(resultOf(ResultStatus.BAD_REQUEST));
                    } else if (ack.status() != ResultStatus.OK) {
                        future.complete(resultOf(ack.status()));
                    } else {
                        acknowledged.add(node);
                        if (acknowledged.size() == replicas.size()) {
                            future.complete(resultOf(ResultStatus.OK));
                        }
                    }
                });
            } catch (RuntimeException error) {
                future.complete(resultOf(ResultStatus.UNAVAILABLE));
            }
        }
    }

    private void executeGet(Command.Get get, RequestContext context, CompletableFuture<Result> future) {
        executeAtLeader(get, context, future);
    }

    private void executeDelete(Command.Delete delete, RequestContext context, CompletableFuture<Result> future) {
        executeAtLeader(delete, context, future);
    }

    private void executeAtLeader(Command command, RequestContext context, CompletableFuture<Result> future) {
        try {
            transport.execute(leaderId, request(command, context)).whenComplete((reply, error) -> {
                if (context.deadline().isExpired(timeSource)) {
                    future.complete(resultOf(ResultStatus.TIMEOUT));
                } else if (error != null || reply == null) {
                    future.complete(resultOf(ResultStatus.UNAVAILABLE));
                } else if (!leaderId.equals(reply.nodeId()) || reply.view() != view
                        || !clientId.equals(reply.clientId())
                        || !context.operationId().equals(reply.operationId())) {
                    future.complete(resultOf(ResultStatus.BAD_REQUEST));
                } else {
                    future.complete(reply.result());
                }
            });
        } catch (RuntimeException error) {
            future.complete(resultOf(ResultStatus.UNAVAILABLE));
        }
    }

    private IoniaClientTransport.Request request(Command command, RequestContext context) {
        long remaining = Math.max(0, context.deadline().monotonicNanos() - timeSource.monotonicNanos());
        return new IoniaClientTransport.Request(view, command, clientId, context.operationId(),
                context.routingVersion(), context.ownershipEpoch(), remaining);
    }

    /** 외부 timer 구동 시 만료된 요청을 완료한다. 이미 수락된 원격 쓰기를 취소하지 않는다. */
    public void onTimer(TimerEvent event) {
        Objects.requireNonNull(event, "event");
        pending.forEach((future, deadline) -> {
            if (deadline.isExpired(timeSource)) {
                future.complete(resultOf(ResultStatus.TIMEOUT));
            }
        });
    }

    /** 대기 요청을 UNAVAILABLE로 완료한다. 외부 소유 transport는 닫지 않는다. */
    @Override
    public void close() {
        closed.set(true);
        pending.keySet().forEach(future -> future.complete(resultOf(ResultStatus.UNAVAILABLE)));
    }

    private static CompletionStage<Result> completed(ResultStatus status) {
        Result result = resultOf(status);
        return CompletableFuture.completedFuture(result);
    }

    private static Result resultOf(ResultStatus status) {
        return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }
}
