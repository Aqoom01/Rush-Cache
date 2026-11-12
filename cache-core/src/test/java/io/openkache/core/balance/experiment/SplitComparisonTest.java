package io.openkache.core.balance.experiment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.balance.experiment.Scenarios.Scenario;
import io.openkache.core.balance.split.HeatSplitPolicy;
import io.openkache.core.tablet.TabletRange;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Split 비교 실험 틀(입력 생성과 지표 계산)이 의도대로 동작하는지 확인한다. 실험 결과 자체는 검사하지 않는다. */
@DisplayName("SplitComparison (Split 비교 실험 틀)")
class SplitComparisonTest {

    @Test
    @DisplayName("key 해시값은 seed가 같으면 같고, 부호 없는 오름차순이다")
    void keyHashesAreDeterministicAndSortedUnsigned() {
        // given & when
        long[] first = Scenarios.keyHashes(1000, 7);
        long[] second = Scenarios.keyHashes(1000, 7);

        // then
        assertArrayEquals(first, second);
        for (int i = 1; i < first.length; i++) {
            assertTrue(Long.compareUnsigned(first[i - 1], first[i]) < 0, "index " + i);
        }
    }

    @Test
    @DisplayName("S2는 앞쪽 20% key가 요청의 약 80%를 받는다")
    void hotRangeScenarioPutsAboutEightyPercentOnTheFrontFifth() {
        // given
        long[] counts = Scenarios.requests(Scenario.S2_HOT_RANGE, 1000, 40_000, new Random(1));

        // when
        long front = Arrays.stream(counts, 0, 200).sum();

        // then: 40,000건이면 표준편차가 약 0.002라 ±0.01 안에 든다
        assertEquals(0.8, front / 40_000.0, 0.01);
    }

    @Test
    @DisplayName("S3는 가운데 key 하나가 요청의 약 90%를 받는다")
    void singleHotKeyScenarioPutsAboutNinetyPercentOnTheMiddleKey() {
        // given
        long[] counts = Scenarios.requests(Scenario.S3_SINGLE_HOT_KEY, 1000, 40_000, new Random(1));

        // when
        double share = counts[500] / 40_000.0;

        // then
        assertEquals(0.9, share, 0.01);
    }

    @Test
    @DisplayName("큰 쪽 비율은 at보다 작은 해시값을 왼쪽으로 세서 계산한다 (30 : 10 → 0.75)")
    void largerShareSplitsByHash() {
        // given: 해시 10, 20은 왼쪽(30건), 30은 오른쪽(10건)
        TabletLoad load = new TabletLoad(
                TabletRange.FULL, 0, List.of(new KeyLoad(10, 10, 0), new KeyLoad(20, 20, 0), new KeyLoad(30, 10, 0)));

        // when
        double share = SplitComparison.largerShare(load, 30);

        // then
        assertEquals(0.75, share, 1e-9);
    }

    @Test
    @DisplayName("같은 정책·시나리오·seed로 다시 돌리면 같은 결과가 나온다")
    void runOnceIsDeterministic() {
        // given
        HeatSplitPolicy policy = new HeatSplitPolicy();

        // when
        SplitComparison.Outcome first = SplitComparison.runOnce(policy, Scenario.S2_HOT_RANGE, 3);
        SplitComparison.Outcome second = SplitComparison.runOnce(policy, Scenario.S2_HOT_RANGE, 3);

        // then
        assertEquals(first, second);
    }
}
