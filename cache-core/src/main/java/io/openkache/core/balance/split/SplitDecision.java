package io.openkache.core.balance.split;

import java.util.Objects;

/** 분할 정책의 결정. 분할하거나, 사유와 함께 보류한다. */
public sealed interface SplitDecision {

    /**
     * 해시값 {@code at}에서 나눈다. 왼쪽은 {@code [start, at)}, 오른쪽은 {@code [at, end)}다.
     *
     * @param at 오른쪽 조각의 시작 해시값(부호 없는 64비트). {@code RoutingTable.withSplit(id, at)}에 그대로 넘긴다
     */
    record Split(long at) implements SplitDecision {}

    /**
     * 분할하지 않는다.
     *
     * @param reason 보류 사유
     */
    record NoSplit(NoSplitReason reason) implements SplitDecision {
        public NoSplit {
            Objects.requireNonNull(reason, "reason");
        }
    }
}
