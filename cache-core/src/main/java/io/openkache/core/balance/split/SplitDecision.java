package io.openkache.core.balance.split;

import io.openkache.core.model.Bytes;
import java.util.Objects;

/** 분할 정책의 결정. 분할하거나, 사유와 함께 보류한다. */
public sealed interface SplitDecision {

    /**
     * {@code splitKey}에서 나눈다. 왼쪽은 {@code [start, splitKey)}, 오른쪽은 {@code [splitKey, end)}다.
     *
     * @param splitKey 오른쪽 범위의 첫 key
     */
    record Split(Bytes splitKey) implements SplitDecision {
        public Split {
            Objects.requireNonNull(splitKey, "splitKey");
        }
    }

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
