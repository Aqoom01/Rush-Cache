package io.openkache.core.replication.ionia;

import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.spi.TimeSource;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * IONIA 클라이언트용 비차단 요청/응답 경계. 주소, socket, Engine 구현을 알지 않는다.
 *
 * <p>실제 TCP 구현은 server.ionia에, 테스트용 구현은 test 소스에 둔다. stage는 전송 완료가 아닌 원격 응답으로 완료한다.
 * 여러 thread의 호출에 안전해야 하며 호출 thread를 block하지 않는다. 노드 간 순서 합의용 Transport와 별개다.
 */
public interface IoniaClientTransport {

    /** SET의 메모리 기록 ACK를 요청한다. KV 적용 완료를 뜻하지 않는다. */
    CompletionStage<WriteAck> record(NodeId to, Request request);

    /** leader GET 또는 순서 확정/적용 결과가 필요한 쓰기를 요청한다. */
    CompletionStage<Reply> execute(NodeId to, Request request);

    /**
     * timeoutNanos는 송신 시점의 상대 잔여 시간이며 0은 만료다.
     * 수신 측은 자신의 시계로 deadline을 구성한다. JVM의 절대 nanoTime을 전송하지 않는다.
     */
    record Request(
            long view, Command command, ClientId clientId, OperationId operationId,
            RoutingVersion routingVersion, OwnershipEpoch ownershipEpoch, long timeoutNanos) {
        public Request {
            if (view < 0 || timeoutNanos < 0) {
                throw new IllegalArgumentException("view and timeoutNanos must be non-negative");
            }
            Objects.requireNonNull(command, "command");
            Objects.requireNonNull(clientId, "clientId");
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(routingVersion, "routingVersion");
            Objects.requireNonNull(ownershipEpoch, "ownershipEpoch");
        }

        /** 수신 JVM 시계에 맞춘 context. wire decoder/테스트 어댑터에서 사용한다. */
        public RequestContext contextAt(TimeSource time) {
            Objects.requireNonNull(time, "time");
            return new RequestContext(clientId, operationId, routingVersion, ownershipEpoch,
                    new Deadline(time.monotonicNanos() + timeoutNanos));
        }
    }

    /** OK여도 메모리 기록만 보장하며 commit/KV 적용/SSD 내구성을 뜻하지 않는다. */
    record WriteAck(NodeId nodeId, long view, ClientId clientId, OperationId operationId, ResultStatus status) {
        public WriteAck {
            requireIdentity(nodeId, view, clientId, operationId);
            Objects.requireNonNull(status, "status");
        }
    }

    /** 실행 결과와 요청/노드 식별자. Client는 대상과 식별자가 모두 일치하는지 검사한다. */
    record Reply(NodeId nodeId, long view, ClientId clientId, OperationId operationId, Result result) {
        public Reply {
            requireIdentity(nodeId, view, clientId, operationId);
            Objects.requireNonNull(result, "result");
        }
    }

    default CompletionStage<ReadReply> followerRead(NodeId to, Request request) {
        return java.util.concurrent.CompletableFuture.failedFuture(new UnsupportedOperationException("follower read transport"));
    }
    default CompletionStage<MetaReply> metaQuery(NodeId to, Request request) {
        return java.util.concurrent.CompletableFuture.failedFuture(new UnsupportedOperationException("meta query transport"));
    }
    default CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) {
        return java.util.concurrent.CompletableFuture.failedFuture(new UnsupportedOperationException("view probe transport"));
    }
    record ReadReply(NodeId nodeId, long view, ClientId clientId, OperationId operationId, long appliedIndex, Result result) {
        public ReadReply {
            requireIdentity(nodeId, view, clientId, operationId);
            if (appliedIndex < 0) throw new IllegalArgumentException("negative applied index");
            Objects.requireNonNull(result);
        }
    }
    /** data=null means INDEX; DATA includes MISS as well as values and errors. */
    record MetaReply(NodeId nodeId, long view, ClientId clientId, OperationId operationId, long index, Result data) {
        public MetaReply {
            requireIdentity(nodeId, view, clientId, operationId);
            if (index < 0) throw new IllegalArgumentException("negative history index");
        }
    }

    private static void requireIdentity(NodeId nodeId, long view, ClientId clientId, OperationId operationId) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(operationId, "operationId");
        if (view < 0) {
            throw new IllegalArgumentException("view must be non-negative");
        }
    }
}
