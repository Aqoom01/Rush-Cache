package io.openkache.core.replication.ionia;

import io.openkache.core.model.NodeId;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * 메모리 기반 IONIA 정상 경로 프로토타입의 노드 설정.
 *
 * <p>복제본은 서로 다른 세 노드이며, leader와 view는 실행 중 변경하지 않는다. view는 IONIA 연구의 세대 번호로,
 * 공통 {@code OwnershipEpoch}나 {@code RoutingVersion}과 별개다. 이 설정은 주소 매핑, leader 선출, 영속화 또는
 * 장애 복구를 제공하지 않는다. 불변이므로 여러 thread에서 공유할 수 있다.
 *
 * @param nodeId 이 설정을 사용하는 노드. 복제 그룹에 속해야 한다
 * @param leaderId 고정 leader. 복제 그룹에 속해야 한다
 * @param replicas 복제 그룹의 세 노드. 입력 목록의 순서를 유지한 불변 복사본을 보관한다
 * @param view 실행 동안 고정할 세대 번호. 0 이상
 */
public record IoniaConfig(NodeId nodeId, NodeId leaderId, List<NodeId> replicas, long view) {

    /** 이번 프로토타입이 지원하는 복제본 수. */
    public static final int REPLICA_COUNT = 3;

    public IoniaConfig {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(leaderId, "leaderId");
        replicas = List.copyOf(Objects.requireNonNull(replicas, "replicas"));
        if (replicas.size() != REPLICA_COUNT) {
            throw new IllegalArgumentException("replicas must contain exactly three nodes");
        }
        if (new HashSet<>(replicas).size() != replicas.size()) {
            throw new IllegalArgumentException("replicas must contain distinct nodes");
        }
        if (!replicas.contains(nodeId)) {
            throw new IllegalArgumentException("nodeId must belong to replicas");
        }
        if (!replicas.contains(leaderId)) {
            throw new IllegalArgumentException("leaderId must belong to replicas");
        }
        if (view < 0) {
            throw new IllegalArgumentException("view must be >= 0: " + view);
        }
    }

    /** 이 노드가 고정 leader이면 {@code true}. */
    public boolean isLeader() {
        return nodeId.equals(leaderId);
    }

    /**
     * 정상 쓰기 경로에 필요한 서로 다른 노드의 기록 ACK 수. 3노드에서는 leader를 포함한 세 노드 모두다.
     *
     * <p>개수만 충족하면 되는 것은 아니다. 같은 view와 논리 요청의 ACK인지, leader가 포함되는지는 Client가 검사해야
     * 한다. 메모리 기록 ACK는 재시작 후 내구성을 보장하지 않는다.
     */
    public int fastWriteQuorumSize() {
        return REPLICA_COUNT;
    }

    /** 순서 확정에 필요한 노드 수. leader 자신을 포함한 과반수이므로 3노드에서는 2다. */
    public int orderingQuorumSize() {
        return REPLICA_COUNT / 2 + 1;
    }
}
