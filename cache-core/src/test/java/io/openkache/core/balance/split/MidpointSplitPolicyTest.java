package io.openkache.core.balance.split;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openkache.core.balance.TabletLoad;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Baseline 정책이 "key 순서상 가운데 key"에서 자르는지 확인한다.
 *
 * <p>모든 정책에 공통인 규칙(두 쪽 모두에 key가 있는지 등)은 {@link SplitInvariantTest}가 본다. 여기서는 Baseline만의 동작,
 * 즉 "어디서 자르는가"만 본다.
 */
@DisplayName("MidpointSplitPolicy (Baseline: key 순서상 가운데에서 분할)")
class MidpointSplitPolicyTest {

    private final SplitPolicy policy = new MidpointSplitPolicy();

    @Test
    @DisplayName("key 개수가 짝수면 정확히 반씩 나눈다 (key 0-9 → 0-4 | 5-9)")
    void evenKeyCountSplitsIntoEqualHalves() {
        // given: key 10개
        TabletLoad load = Loads.uniform(10, 1);

        // when
        SplitDecision decision = policy.decide(load);

        // then: 10 / 2 = 5번 key가 오른쪽의 첫 key
        assertEquals(new SplitDecision.Split(Loads.key(5)), decision);
    }

    @Test
    @DisplayName("key 개수가 홀수면 남는 key 하나는 오른쪽에 간다 (key 0-8 → 0-3 | 4-8)")
    void oddKeyCountPutsTheExtraKeyOnTheRight() {
        // given: key 9개
        TabletLoad load = Loads.uniform(9, 1);

        // when
        SplitDecision decision = policy.decide(load);

        // then: 9 / 2 = 4 (정수 나눗셈)번 key가 오른쪽의 첫 key → 왼쪽 4개, 오른쪽 5개
        assertEquals(new SplitDecision.Split(Loads.key(4)), decision);
    }

    @Test
    @DisplayName("key가 2개면 그 사이에서 나눈다 (key 0 | 1)")
    void twoKeysSplitBetweenThem() {
        // given: key 2개 (나눌 수 있는 최소 개수)
        TabletLoad load = Loads.uniform(2, 1);

        // when
        SplitDecision decision = policy.decide(load);

        // then
        assertEquals(new SplitDecision.Split(Loads.key(1)), decision);
    }

    @Test
    @DisplayName("요청이 앞쪽에 몰려도 분할 지점은 같다 (부하를 보지 않음. Split-for-Heat와 비교되는 지점)")
    void ignoresWhereTheRequestsAre() {
        // given: key 1,000개. 하나는 고르게, 하나는 앞쪽 20%에 요청 80%
        TabletLoad uniform = Loads.uniform(1000, 4);
        TabletLoad hotRange = Loads.hotRange();

        // when
        SplitDecision uniformDecision = policy.decide(uniform);
        SplitDecision hotRangeDecision = policy.decide(hotRange);

        // then: 둘 다 key 개수의 가운데(500번)에서 자른다
        assertEquals(new SplitDecision.Split(Loads.key(500)), uniformDecision);
        assertEquals(uniformDecision, hotRangeDecision);
    }

    @Test
    @DisplayName("variant 이름은 설계 문서와 같은 midpoint-split이다")
    void variantNameMatchesTheDesignDocument() {
        // given: Baseline 정책 (필드 policy)

        // when
        String variant = policy.variant();

        // then
        assertEquals("midpoint-split", variant);
    }
}
