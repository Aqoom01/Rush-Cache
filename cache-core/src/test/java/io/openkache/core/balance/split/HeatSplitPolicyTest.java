package io.openkache.core.balance.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.tablet.TabletRange;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Split-for-Heat가 요청 부하를 기준으로 자르고, 효과가 없으면 보류하는지 확인한다. 예시는 design.md §4.3의 표와 같다.
 *
 * <p>모든 정책에 공통인 규칙은 {@link SplitInvariantTest}가 본다.
 */
@DisplayName("HeatSplitPolicy (Split-for-Heat: 요청 부하가 반반이 되는 key에서 분할)")
class HeatSplitPolicyTest {

    private final SplitPolicy policy = new HeatSplitPolicy();

    @Test
    @DisplayName("앞쪽 20% key에 요청 80%가 몰리면 125번 key에서 잘라 2,000 : 2,000으로 나눈다")
    void hotRangeIsSplitWhereRequestsAreHalved() {
        // given: 앞쪽 200개 key 16건씩, 나머지 800개 1건씩. 전체 4,000건
        TabletLoad load = Loads.hotRange();

        // when
        SplitDecision decision = policy.decide(load);

        // then: 0-124번 key가 125 × 16 = 2,000건 → 125번이 오른쪽의 첫 key
        assertEquals(new SplitDecision.Split(Loads.key(125, 1000)), decision);
        assertEquals(2000, leftRequests(load, decision));
    }

    @Test
    @DisplayName("요청이 고르면 key 개수의 가운데(500번)에서 자른다")
    void uniformLoadIsSplitInTheMiddle() {
        // given
        TabletLoad load = Loads.uniform(1000, 2);

        // when
        SplitDecision decision = policy.decide(load);

        // then
        assertEquals(new SplitDecision.Split(Loads.key(500, 1000)), decision);
    }

    @Test
    @DisplayName("가운데 key 하나가 50%를 받으면 큰 쪽이 약 0.75라 분할한다")
    void hotKeyWithHalfTheLoadIsSplit() {
        // given: 500번 key 999건, 나머지 999개 1건씩. 전체 1,998건 중 500번이 50%
        TabletLoad load = Loads.tablet(1000, i -> i == 500 ? 999 : 1);

        // when
        SplitDecision decision = policy.decide(load);

        // then: 500번 앞에서 자르면 500 : 1,498 → 큰 쪽 0.75 ≤ 0.8
        assertEquals(new SplitDecision.Split(Loads.key(500, 1000)), decision);
    }

    @Test
    @DisplayName("가운데 key 하나가 90%를 받으면 어디서 잘라도 큰 쪽이 0.8을 넘어 SINGLE_HOT_KEY로 보류한다")
    void hotKeyWithMostOfTheLoadIsNotSplit() {
        // given: 500번 key 8,991건, 나머지 999개 1건씩. 전체 9,990건 중 500번이 90%
        TabletLoad load = Loads.tablet(1000, i -> i == 500 ? 8991 : 1);

        // when
        SplitDecision decision = policy.decide(load);

        // then: 가장 잘 잘라도 500 : 9,490 → 큰 쪽 0.95
        assertEquals(new SplitDecision.NoSplit(NoSplitReason.SINGLE_HOT_KEY), decision);
    }

    @Test
    @DisplayName("큰 쪽 비율이 정확히 θ(0.8)면 분할하고, 넘으면 보류한다")
    void thetaIsInclusive() {
        // given: key 2개. 8 : 2 → 0.8, 9 : 1 → 0.9
        TabletLoad atTheta = Loads.tablet(2, i -> i == 0 ? 8 : 2);
        TabletLoad overTheta = Loads.tablet(2, i -> i == 0 ? 9 : 1);

        // when
        SplitDecision atThetaDecision = policy.decide(atTheta);
        SplitDecision overThetaDecision = policy.decide(overTheta);

        // then
        assertEquals(new SplitDecision.Split(Loads.key(1, 2)), atThetaDecision);
        assertEquals(new SplitDecision.NoSplit(NoSplitReason.SINGLE_HOT_KEY), overThetaDecision);
    }

    @Test
    @DisplayName("θ를 0.9로 올리면 9 : 1도 분할한다")
    void higherThetaAllowsMoreSkewedSplits() {
        // given
        SplitPolicy lenient = new HeatSplitPolicy(0.9);
        TabletLoad load = Loads.tablet(2, i -> i == 0 ? 9 : 1);

        // when
        SplitDecision decision = lenient.decide(load);

        // then
        assertEquals(new SplitDecision.Split(Loads.key(1, 2)), decision);
    }

    @Test
    @DisplayName("자른 두 조각 모두에 key가 하나 이상 있다 (Baseline과 달리 빈 조각을 만들지 않음)")
    void bothHalvesHaveKeys() {
        // given: 모든 key가 구간 앞쪽 1/4에 몰려 있다. Baseline은 가운데에서 잘라 오른쪽이 빈다
        TabletRange range = new TabletRange(0, 4000);
        List<KeyLoad> keys = List.of(new KeyLoad(10, 5, 0), new KeyLoad(20, 5, 0), new KeyLoad(30, 5, 0), new KeyLoad(40, 5, 0));
        TabletLoad load = new TabletLoad(range, 0, keys);

        // when
        SplitDecision.Split split = assertInstanceOf(SplitDecision.Split.class, policy.decide(load));

        // then
        TabletRange.Split parts = range.splitAt(split.at());
        assertTrue(keys.stream().anyMatch(k -> parts.left().contains(k.keyHash())), "왼쪽에 key가 있어야 한다");
        assertTrue(keys.stream().anyMatch(k -> parts.right().contains(k.keyHash())), "오른쪽에 key가 있어야 한다");
    }

    @Test
    @DisplayName("key가 1개면 나눌 자리가 없어 NO_VALID_POINT로 보류한다")
    void singleKeyIsNotSplit() {
        // given: 구간은 넓지만 key가 하나뿐이다
        TabletLoad load = Loads.tablet(1, i -> 1000);

        // when
        SplitDecision decision = policy.decide(load);

        // then
        assertEquals(new SplitDecision.NoSplit(NoSplitReason.NO_VALID_POINT), decision);
    }

    @Test
    @DisplayName("θ는 0 초과 1 이하만 받는다")
    void rejectsThetaOutOfRange() {
        // given & when & then
        assertThrows(IllegalArgumentException.class, () -> new HeatSplitPolicy(0));
        assertThrows(IllegalArgumentException.class, () -> new HeatSplitPolicy(1.1));
    }

    @Test
    @DisplayName("variant 이름은 설계 문서와 같은 split-for-heat이다")
    void variantNameMatchesTheDesignDocument() {
        // given: Split-for-Heat 정책 (필드 policy)

        // when
        String variant = policy.variant();

        // then
        assertEquals("split-for-heat", variant);
    }

    // ---- 도우미 ----

    /** {@code decision}의 분할 지점보다 왼쪽에 있는 key의 요청 합. */
    private static long leftRequests(TabletLoad load, SplitDecision decision) {
        long at = ((SplitDecision.Split) decision).at();
        return load.keys().stream()
                .filter(k -> Long.compareUnsigned(k.keyHash(), at) < 0)
                .mapToLong(KeyLoad::requests)
                .sum();
    }
}
