package io.openkache.core.model;

/**
 * 소유권(Primary, Tablet Owner 등)이 바뀔 때마다 증가하는 세대 번호.
 *
 * <p>값은 0 이상이고 단조 증가한다. 더 작은 epoch를 가진 요청이나 메시지는 오래된 소유자의 것으로 보고 거절한다
 * ({@code STALE_EPOCH}). wire에서는 u64로 전송하지만 Java에서는 0 이상 {@link Long#MAX_VALUE} 이하만 허용한다.
 */
public record OwnershipEpoch(long value) implements Comparable<OwnershipEpoch> {

    public static final OwnershipEpoch ZERO = new OwnershipEpoch(0);

    public OwnershipEpoch {
        Ids.requireNonNegative("OwnershipEpoch", value);
    }

    /** 다음 epoch. */
    public OwnershipEpoch next() {
        return new OwnershipEpoch(Math.addExact(value, 1));
    }

    public boolean isNewerThan(OwnershipEpoch other) {
        return value > other.value;
    }

    @Override
    public int compareTo(OwnershipEpoch other) {
        return Long.compare(value, other.value);
    }
}
