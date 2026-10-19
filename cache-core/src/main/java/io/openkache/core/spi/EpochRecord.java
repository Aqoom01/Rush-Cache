package io.openkache.core.spi;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link EpochStore}에 저장하는 안전성 메타데이터.
 *
 * @param epoch 이 노드가 알고 있는 가장 높은 소유권 epoch
 * @param votedFor {@code epoch}에서 투표한 노드. 투표 기반 프로토콜이 아니거나 투표하지 않았으면 empty
 */
public record EpochRecord(OwnershipEpoch epoch, Optional<NodeId> votedFor) {

    public EpochRecord {
        Objects.requireNonNull(epoch, "epoch");
        Objects.requireNonNull(votedFor, "votedFor");
    }
}
