package io.openkache.core.balance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openkache.core.model.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tablet 관찰 결과({@link TabletLoad})가 잘못된 입력을 거절하고, 요청 수를 바르게 합산하는지 확인한다.
 *
 * <p>분할 정책은 key가 정렬되어 있고 범위 안에 있다고 믿고 동작하므로, 그 전제를 여기서 막는다.
 */
@DisplayName("TabletLoad (Tablet 관찰 결과)")
class TabletLoadTest {

    @Test
    @DisplayName("전체 요청 수는 모든 key의 읽기 + 쓰기를 더한 값이다")
    void totalRequestsSumsReadsAndWrites() {
        // given: a(3+1) + b(0+0) + c(2+4) = 10
        TabletLoad load = new TabletLoad(KeyRange.all(), 0, List.of(keyLoad("a", 3, 1), keyLoad("b", 0, 0), keyLoad("c", 2, 4)));

        // when
        long total = load.totalRequests();

        // then
        assertEquals(10, total);
    }

    @Test
    @DisplayName("key가 순서대로 정렬되어 있지 않으면 거절한다")
    void rejectsUnsortedKeys() {
        // given: b가 a보다 앞에 있음
        List<KeyLoad> unsorted = List.of(keyLoad("b", 1, 0), keyLoad("a", 1, 0));

        // when & then
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(KeyRange.all(), 0, unsorted));
    }

    @Test
    @DisplayName("같은 key가 두 번 있으면 거절한다")
    void rejectsDuplicateKeys() {
        // given
        List<KeyLoad> duplicated = List.of(keyLoad("a", 1, 0), keyLoad("a", 2, 0));

        // when & then
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(KeyRange.all(), 0, duplicated));
    }

    @Test
    @DisplayName("범위 밖의 key가 있으면 거절한다 ([\"b\", \"d\") 범위에 \"d\")")
    void rejectsKeysOutsideTheRange() {
        // given: end인 "d"는 범위에 포함되지 않는다
        KeyRange range = new KeyRange(Optional.of(Bytes.utf8("b")), Optional.of(Bytes.utf8("d")));
        List<KeyLoad> outside = List.of(keyLoad("d", 1, 0));

        // when & then
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(range, 0, outside));
    }

    @Test
    @DisplayName("요청 수와 window 번호가 음수면 거절한다")
    void rejectsNegativeCountsAndWindow() {
        // given & when & then
        assertThrows(IllegalArgumentException.class, () -> keyLoad("a", -1, 0), "읽기 수 음수");
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(KeyRange.all(), -1, List.of()), "window 음수");
    }

    @Test
    @DisplayName("만든 뒤 원본 리스트를 바꿔도 TabletLoad는 바뀌지 않는다")
    void modifyingSourceListDoesNotChangeLoad() {
        // given
        List<KeyLoad> source = new ArrayList<>(List.of(keyLoad("a", 1, 0)));
        TabletLoad load = new TabletLoad(KeyRange.all(), 0, source);

        // when: 원본 리스트에 key 추가
        source.add(keyLoad("b", 1, 0));

        // then: TabletLoad는 만들 때 복사해 두었으므로 그대로 1개
        assertEquals(1, load.keys().size());
    }

    // ---- 도우미 ----

    private static KeyLoad keyLoad(String key, long reads, long writes) {
        return new KeyLoad(Bytes.utf8(key), reads, writes);
    }
}
