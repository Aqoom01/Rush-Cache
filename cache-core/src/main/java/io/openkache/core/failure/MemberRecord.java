package io.openkache.core.failure;

import io.openkache.core.model.NodeId;
import java.util.Objects;

/**
 * 한 노드에 대해 로컬 SWIM 인스턴스가 알고 있는 membership 정보.
 *
 * @param nodeId 노드 식별자
 * @param incarnation 재시작과 suspicion 반박 순서를 구분하는 SWIM incarnation. ownership epoch와 무관하다
 * @param status 현재 관측 상태
 * @param changedAtNanos 이 상태로 바뀐 로컬 monotonic 시각
 */
public record MemberRecord(NodeId nodeId, long incarnation, MemberStatus status, long changedAtNanos) {

    public MemberRecord {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(status, "status");
        if (incarnation < 0) {
            throw new IllegalArgumentException("incarnation must be >= 0: " + incarnation);
        }
    }
}
