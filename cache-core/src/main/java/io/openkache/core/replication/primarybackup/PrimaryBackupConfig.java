package io.openkache.core.replication.primarybackup;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * 고정 구성의 Primary-Backup 복제 그룹 설정.
 *
 * <p>Primary와 Backup은 실행 중 바뀌지 않는다. 승격·장애 감지·승계 순서는 이 설정에 아직 없다. 불변이므로 여러 thread에서
 * 공유할 수 있다.
 *
 * @param nodeId 이 설정을 쓰는 노드. Primary이거나 Backup 중 하나여야 한다
 * @param primaryId 고정 Primary
 * @param backupIds 고정 Backup 목록. 비어 있지 않고 서로 다르며 Primary를 포함하지 않는다. 입력 순서를 유지한 불변 복사본
 * @param epoch 실행 동안 고정할 소유권 epoch. 모든 복제 메시지에 붙는다
 */
public record PrimaryBackupConfig(NodeId nodeId, NodeId primaryId, List<NodeId> backupIds, OwnershipEpoch epoch) {

    public PrimaryBackupConfig {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(primaryId, "primaryId");
        backupIds = List.copyOf(Objects.requireNonNull(backupIds, "backupIds"));
        Objects.requireNonNull(epoch, "epoch");
        if (backupIds.isEmpty()) {
            throw new IllegalArgumentException("backupIds must not be empty");
        }
        if (new HashSet<>(backupIds).size() != backupIds.size()) {
            throw new IllegalArgumentException("backupIds must be distinct: " + backupIds);
        }
        if (backupIds.contains(primaryId)) {
            throw new IllegalArgumentException("primaryId must not be a backup: " + primaryId);
        }
        if (!nodeId.equals(primaryId) && !backupIds.contains(nodeId)) {
            throw new IllegalArgumentException("nodeId must be the primary or one of the backups: " + nodeId);
        }
    }

    /** 이 노드가 고정 Primary이면 {@code true}. */
    public boolean isPrimary() {
        return nodeId.equals(primaryId);
    }

    /** 동기 복제에서 클라이언트 OK에 필요한 ACK 수. 모든 Backup이므로 {@code backupIds.size()}다. */
    public int requiredAcks() {
        return backupIds.size();
    }
}
