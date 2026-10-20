package io.openkache.core.failure;

import io.openkache.core.model.NodeId;
import java.util.Objects;

/**
 * 로컬 membership 관측이 실제로 바뀐 사건.
 *
 * @param member 상태가 바뀐 노드
 * @param previous 이전 상태
 * @param current 새 상태
 * @param incarnation 새 상태의 SWIM incarnation
 * @param observedAtNanos 상태 변경을 관측한 로컬 monotonic 시각
 */
public record MembershipEvent(
        NodeId member,
        MemberStatus previous,
        MemberStatus current,
        long incarnation,
        long observedAtNanos) {

    public MembershipEvent {
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(current, "current");
        if (previous == current) {
            throw new IllegalArgumentException("membership 이벤트는 무조건 상태가 바뀌어야한다.");
        }
        if (incarnation < 0) {
            throw new IllegalArgumentException("incarnation 번호 >= 0 : " + incarnation);
        }
    }
}
