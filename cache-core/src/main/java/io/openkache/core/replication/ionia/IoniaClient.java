package io.openkache.core.replication.ionia;

import io.openkache.core.api.Command;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 여러 노드에 대한 요청 전송 지시와 ACK·읽기 결과 판단을 맡을 IONIA 클라이언트.
 *
 * <p><b>현재 상태: 구조 확인용 골격.</b> {@link #execute}는 메시지를 보내지 않고 {@code NOT_IMPLEMENTED}를 반환한다.
 * 실제 요청 전송, 기록 ACK 집계, 읽기 검증, 재시도와 timeout 판단은 아직 구현하지 않았다.
 *
 * <p>Workload는 논리 {@link Command}만 만들고, Runner가 이 Client를 호출한다. Client는 각 서버의
 * {@link IoniaEngine}을 내부에 만들거나 로컬 KV를 소유하지 않는다. IP·호스트명·포트와 실제 socket은 다루지 않는다.
 *
 * <p>후속 구현에서 클라이언트용 기록·읽기 요청과 응답 타입 및 비차단 전송 경계를 연결한다. 공통 {@code Transport}의
 * 로컬 전송 완료나 노드 간 {@code PrepareAck}를 클라이언트의 기록 ACK로 대신 사용하지 않는다.
 */
public final class IoniaClient {

    private final ClientId clientId;
    private final List<NodeId> replicas;
    private final NodeId leaderId;
    private final long view;

    /**
     * 클라이언트와 고정 복제 그룹의 구성을 보관한다. Client 자신은 복제 그룹의 노드가 아니다.
     *
     * @param clientId 논리 요청을 발급하는 클라이언트 ID
     * @param replicas 서로 다른 복제 노드 세 개. 불변 복사본을 보관한다
     * @param leaderId 복제 그룹에 속한 고정 leader
     * @param view 실행 동안 고정할 IONIA 세대 번호. 0 이상
     */
    public IoniaClient(ClientId clientId, List<NodeId> replicas, NodeId leaderId, long view) {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.replicas = List.copyOf(Objects.requireNonNull(replicas, "replicas"));
        this.leaderId = Objects.requireNonNull(leaderId, "leaderId");
        if (this.replicas.size() != IoniaConfig.REPLICA_COUNT) {
            throw new IllegalArgumentException("replicas must contain exactly three nodes");
        }
        if (new HashSet<>(this.replicas).size() != this.replicas.size()) {
            throw new IllegalArgumentException("replicas must contain distinct nodes");
        }
        if (!this.replicas.contains(leaderId)) {
            throw new IllegalArgumentException("leaderId must belong to replicas");
        }
        if (view < 0) {
            throw new IllegalArgumentException("view must be >= 0: " + view);
        }
        this.view = view;
    }

    /**
     * 논리 요청 하나를 실행할 진입점. 현재는 송신이나 결과 판단 없이 {@code NOT_IMPLEMENTED}로 완료한다.
     *
     * <p>후속 구현할 흐름:
     *
     * <ul>
     *   <li>SET: 복제 그룹에 기록 요청 → 같은 view·논리 요청의 서로 다른 노드 ACK 확인
     *   <li>GET: leader 또는 follower 읽기 → pending 변경·응답 index 검증
     *   <li>DELETE: leader에서 순서 확정·적용 후 실제 삭제 결과 수신
     * </ul>
     *
     * <p>논리 요청의 식별자는 context의 {@code ClientId + OperationId}를 사용한다. 재시도에서도 유지해야 하며,
     * timeout을 미적용이나 취소 성공으로 해석하지 않는다. 호출 thread에서 응답을 기다리며 block하지 않아야 한다.
     *
     * @param command Workload 또는 테스트가 만든 논리 요청
     * @param context 논리 요청 ID와 deadline. 이 Client의 clientId를 사용해야 한다
     * @return 현재는 항상 미구현 결과
     */
    public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(context, "context");
        return CompletableFuture.completedFuture(
                Result.of(ResultStatus.NOT_IMPLEMENTED, RoutingVersion.ZERO, OwnershipEpoch.ZERO));
    }
}
