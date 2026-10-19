package io.openkache.core.balance;

import java.util.List;
import java.util.Objects;

/**
 * Tablet 하나를 관찰 window 하나 동안 본 결과. 분할 정책의 입력이다.
 *
 * <p>{@code keys}는 범위 안에 있는 모든 key다. 이 window에 요청을 받지 않은 key도 요청 수 0으로 포함한다. key 순서({@link
 * KeyRange#ORDER})로 정렬되어 있고 중복이 없어야 한다.
 *
 * <p>B2 내부 최소 타입이다. 불변 객체이므로 여러 thread에서 공유할 수 있다.
 *
 * @param range 이 Tablet의 key 범위
 * @param window 논리 시간 window 번호 (0 이상). 실제 시계가 아니다
 * @param keys 범위 안 key별 요청 수. key 순서로 정렬, 중복 없음
 */
public record TabletLoad(KeyRange range, long window, List<KeyLoad> keys) {

    public TabletLoad {
        Objects.requireNonNull(range, "range");
        if (window < 0) {
            throw new IllegalArgumentException("window must be >= 0: " + window);
        }
        keys = List.copyOf(keys);
        for (int i = 0; i < keys.size(); i++) {
            KeyLoad current = keys.get(i);
            if (!range.contains(current.key())) {
                throw new IllegalArgumentException(current.key() + " is outside " + range);
            }
            if (i > 0 && KeyRange.ORDER.compare(keys.get(i - 1).key(), current.key()) >= 0) {
                throw new IllegalArgumentException("keys must be sorted without duplicates at index " + i);
            }
        }
    }

    /** window 안의 전체 요청 수. */
    public long totalRequests() {
        return keys.stream().mapToLong(KeyLoad::requests).sum();
    }
}
