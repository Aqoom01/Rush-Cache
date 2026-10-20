package io.openkache.core.failure;

/** membership 상태 변경을 받는 callback. detector의 owner thread에서 호출된다. */
@FunctionalInterface
public interface MembershipListener {

    void onMembershipChanged(MembershipEvent event);

    static MembershipListener noop() {
        return event -> {};
    }
}
