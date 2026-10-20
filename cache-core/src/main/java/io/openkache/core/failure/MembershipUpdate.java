package io.openkache.core.failure;

import io.openkache.core.model.NodeId;
import java.util.Objects;

/**
 * probe 메시지에 piggyback해 전파하는 membership 변경.
 *
 * @param member 상태가 바뀐 노드
 * @param incarnation 변경의 SWIM incarnation
 * @param status 전파할 상태
 * @param reporter 이 변경을 만든 노드
 */
public record MembershipUpdate(NodeId member, long incarnation, MemberStatus status, NodeId reporter) {

    public MembershipUpdate {
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(reporter, "reporter");
        if (incarnation < 0) {
            throw new IllegalArgumentException("incarnation 번호 >= 0: " + incarnation);
        }
    }
}
