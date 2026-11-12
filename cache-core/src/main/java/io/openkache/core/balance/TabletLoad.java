package io.openkache.core.balance;

import io.openkache.core.tablet.TabletRange;
import java.util.List;
import java.util.Objects;

/**
 * Tablet 하나를 관찰 window 하나 동안 본 결과. 분할 정책의 입력이다.
 *
 * <p>{@code keys}는 해시값 오름차순(부호 없음)이고 중복이 없으며 모두 {@code range} 안에 있어야 한다.
 *
 * <p>불변 객체이므로 여러 thread에서 공유할 수 있다.
 *
 * @param range 이 Tablet의 해시 구간 (B1 {@link TabletRange})
 * @param window 논리 시간 window 번호 (0 이상). 실제 시계가 아니다
 * @param keys 이 window에 관찰한 key별 요청 수
 */
public record TabletLoad(TabletRange range, long window, List<KeyLoad> keys) {

    public TabletLoad {
        Objects.requireNonNull(range, "range");
        if (window < 0) {
            throw new IllegalArgumentException("window must be >= 0: " + window);
        }
        keys = List.copyOf(keys);
        for (int i = 0; i < keys.size(); i++) {
            long hash = keys.get(i).keyHash();
            if (!range.contains(hash)) {
                throw new IllegalArgumentException(Long.toUnsignedString(hash) + " is outside " + range);
            }
            if (i > 0 && Long.compareUnsigned(keys.get(i - 1).keyHash(), hash) >= 0) {
                throw new IllegalArgumentException("keys must be sorted by hash without duplicates at index " + i);
            }
        }
    }

    /** window 안의 전체 요청 수. */
    public long totalRequests() {
        return keys.stream().mapToLong(KeyLoad::requests).sum();
    }
}
