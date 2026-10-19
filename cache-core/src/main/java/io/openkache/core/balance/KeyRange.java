package io.openkache.core.balance;

import io.openkache.core.model.Bytes;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

/**
 * Tablet 하나가 맡는 key 범위 {@code [start, end)}.
 *
 * <p>key 순서는 {@link #ORDER}(부호 없는 바이트 사전순)를 따른다. {@code start}가 비어 있으면 아래로, {@code end}가
 * 비어 있으면 위로 끝이 없다.
 *
 * <p>B2 내부 최소 타입이다. B1의 Tablet 구조가 확정되면 그 타입으로 바꾼다. 불변 객체이므로 여러 thread에서 공유할 수
 * 있다.
 *
 * @param start 포함하는 시작 key. 비어 있으면 하한 없음
 * @param end 포함하지 않는 끝 key. 비어 있으면 상한 없음
 */
public record KeyRange(Optional<Bytes> start, Optional<Bytes> end) {

    /**
     * key 순서. 바이트를 앞에서부터 부호 없는 값(0-255)으로 비교하고, 한쪽이 다른 쪽의 앞부분이면 짧은 쪽이 앞선다.
     */
    public static final Comparator<Bytes> ORDER = KeyRange::compareKeys;

    private static final KeyRange ALL = new KeyRange(Optional.empty(), Optional.empty());

    public KeyRange {
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(end, "end");
        if (start.isPresent() && end.isPresent() && ORDER.compare(start.get(), end.get()) >= 0) {
            throw new IllegalArgumentException("start must be less than end: " + start.get() + " >= " + end.get());
        }
    }

    /** 위아래 끝이 없는 전체 범위. */
    public static KeyRange all() {
        return ALL;
    }

    /** {@code key}가 이 범위 안에 있는지. */
    public boolean contains(Bytes key) {
        Objects.requireNonNull(key, "key");
        boolean aboveStart = start.isEmpty() || ORDER.compare(start.get(), key) <= 0;
        boolean belowEnd = end.isEmpty() || ORDER.compare(key, end.get()) < 0;
        return aboveStart && belowEnd;
    }

    /**
     * {@code splitKey}로 나눈 왼쪽 범위 {@code [start, splitKey)}.
     *
     * @throws IllegalArgumentException {@code splitKey}가 {@code start}보다 크고 범위 안에 있지 않을 때
     */
    public KeyRange leftOf(Bytes splitKey) {
        requireSplittableAt(splitKey);
        return new KeyRange(start, Optional.of(splitKey));
    }

    /**
     * {@code splitKey}로 나눈 오른쪽 범위 {@code [splitKey, end)}.
     *
     * @throws IllegalArgumentException {@code splitKey}가 {@code start}보다 크고 범위 안에 있지 않을 때
     */
    public KeyRange rightOf(Bytes splitKey) {
        requireSplittableAt(splitKey);
        return new KeyRange(Optional.of(splitKey), end);
    }

    /** {@code start < splitKey < end}이면 {@code true}. 이때만 두 범위가 모두 비어 있지 않게 나눌 수 있다. */
    public boolean isSplittableAt(Bytes splitKey) {
        Objects.requireNonNull(splitKey, "splitKey");
        boolean aboveStart = start.isEmpty() || ORDER.compare(start.get(), splitKey) < 0;
        return aboveStart && contains(splitKey);
    }

    private void requireSplittableAt(Bytes splitKey) {
        if (!isSplittableAt(splitKey)) {
            throw new IllegalArgumentException("cannot split " + this + " at " + splitKey);
        }
    }

    /** 앞에서부터 한 바이트씩 비교하고, 처음 다른 바이트에서 순서를 정한다. 끝까지 같으면 짧은 쪽이 앞이다. */
    private static int compareKeys(Bytes a, Bytes b) {
        int common = Math.min(a.size(), b.size());

        for (int i = 0; i < common; i++) {
            // Java byte는 0x80 이상을 음수로 읽으므로 0-255로 바꿔 비교한다.
            int diff = Byte.toUnsignedInt(a.get(i)) - Byte.toUnsignedInt(b.get(i));
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(a.size(), b.size());
    }
}
