package io.openkache.core.balance;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openkache.core.tablet.TabletRange;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tablet 관찰 결과({@link TabletLoad})가 잘못된 입력을 거절하고, 요청 수를 바르게 합산하는지 확인한다.
 *
 * <p>분할 정책은 key가 해시값 순서로 정렬되어 있고 구간 안에 있다고 믿고 동작하므로, 그 전제를 여기서 막는다.
 */
@DisplayName("TabletLoad (Tablet 관찰 결과)")
class TabletLoadTest {

    @Test
    @DisplayName("전체 요청 수는 모든 key의 읽기 + 쓰기를 더한 값이다")
    void totalRequestsSumsReadsAndWrites() {
        // given: (3+1) + (0+0) + (2+4) = 10
        TabletLoad load = new TabletLoad(
                TabletRange.FULL, 0, List.of(new KeyLoad(10, 3, 1), new KeyLoad(20, 0, 0), new KeyLoad(30, 2, 4)));

        // when
        long total = load.totalRequests();

        // then
        assertEquals(10, total);
    }

    @Test
    @DisplayName("해시값은 부호 없이 정렬한다: 0xFFFF_FFFF_FFFF_FFFF(-1L)이 가장 크다")
    void hashesAreOrderedAsUnsigned() {
        // given: Java long으로는 -1L이 5보다 작지만, 부호 없는 64비트로는 2^64 - 1로 가장 크다
        List<KeyLoad> unsignedOrder = List.of(new KeyLoad(5, 1, 0), new KeyLoad(-1L, 1, 0));
        List<KeyLoad> signedOrder = List.of(new KeyLoad(-1L, 1, 0), new KeyLoad(5, 1, 0));

        // when & then
        assertDoesNotThrow(() -> new TabletLoad(TabletRange.FULL, 0, unsignedOrder));
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(TabletRange.FULL, 0, signedOrder));
    }

    @Test
    @DisplayName("같은 해시값이 두 번 있으면 거절한다")
    void rejectsDuplicateHashes() {
        // given
        List<KeyLoad> duplicated = List.of(new KeyLoad(7, 1, 0), new KeyLoad(7, 2, 0));

        // when & then
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(TabletRange.FULL, 0, duplicated));
    }

    @Test
    @DisplayName("구간 밖의 해시값이 있으면 거절한다 ([100, 200) 구간에 200)")
    void rejectsHashesOutsideTheRange() {
        // given: end인 200은 구간에 포함되지 않는다
        TabletRange range = new TabletRange(100, 200);
        List<KeyLoad> outside = List.of(new KeyLoad(200, 1, 0));

        // when & then
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(range, 0, outside));
    }

    @Test
    @DisplayName("요청 수와 window 번호가 음수면 거절한다")
    void rejectsNegativeCountsAndWindow() {
        // given & when & then
        assertThrows(IllegalArgumentException.class, () -> new KeyLoad(1, -1, 0), "읽기 수 음수");
        assertThrows(IllegalArgumentException.class, () -> new TabletLoad(TabletRange.FULL, -1, List.of()), "window 음수");
    }

    @Test
    @DisplayName("만든 뒤 원본 리스트를 바꿔도 TabletLoad는 바뀌지 않는다")
    void modifyingSourceListDoesNotChangeLoad() {
        // given
        List<KeyLoad> source = new ArrayList<>(List.of(new KeyLoad(1, 1, 0)));
        TabletLoad load = new TabletLoad(TabletRange.FULL, 0, source);

        // when: 원본 리스트에 key 추가
        source.add(new KeyLoad(2, 1, 0));

        // then: TabletLoad는 만들 때 복사해 두었으므로 그대로 1개
        assertEquals(1, load.keys().size());
    }
}
