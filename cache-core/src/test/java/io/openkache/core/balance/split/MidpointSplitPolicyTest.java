package io.openkache.core.balance.split;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openkache.core.balance.TabletLoad;
import io.openkache.core.tablet.TabletRange;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Baseline 정책이 "해시 구간의 산술적 가운데"에서 자르는지 확인한다.
 *
 * <p>모든 정책에 공통인 규칙은 {@link SplitInvariantTest}가 본다. 여기서는 Baseline만의 동작, 즉 "어디서 자르는가"만 본다.
 */
@DisplayName("MidpointSplitPolicy (Baseline: 해시 구간의 가운데에서 분할)")
class MidpointSplitPolicyTest {

    private final SplitPolicy policy = new MidpointSplitPolicy();

    @Test
    @DisplayName("해시 공간 전체 [0, 2^64)는 2^63에서 자른다")
    void fullSpaceSplitsAtTwoToThe63() {
        // given
        TabletLoad load = new TabletLoad(TabletRange.FULL, 0, List.of());

        // when
        SplitDecision decision = policy.decide(load);

        // then: 2^63은 Java long으로 Long.MIN_VALUE와 같은 비트다
        assertEquals(new SplitDecision.Split(1L << 63), decision);
        assertEquals("9223372036854775808", Long.toUnsignedString(((SplitDecision.Split) decision).at()));
    }

    @Test
    @DisplayName("[1000, 2000)은 1500에서 자른다")
    void boundedRangeSplitsAtItsMiddle() {
        // given
        TabletLoad load = new TabletLoad(new TabletRange(1000, 2000), 0, List.of());

        // when
        SplitDecision decision = policy.decide(load);

        // then
        assertEquals(new SplitDecision.Split(1500), decision);
    }

    @Test
    @DisplayName("끝이 공간의 끝(2^64)인 [2^63, 2^64)는 2^63 + 2^62에서 자른다")
    void rangeEndingAtSpaceEndSplitsAtItsMiddle() {
        // given: end = 0은 2^64를 뜻한다
        TabletLoad load = new TabletLoad(new TabletRange(1L << 63, 0), 0, List.of());

        // when
        SplitDecision decision = policy.decide(load);

        // then
        assertEquals(new SplitDecision.Split((1L << 63) + (1L << 62)), decision);
    }

    @Test
    @DisplayName("폭이 1인 구간은 나눌 자리가 없어 NO_VALID_POINT로 보류한다")
    void widthOneRangeIsNotSplit() {
        // given: [5, 6)에는 5 하나뿐이다
        TabletLoad load = new TabletLoad(new TabletRange(5, 6), 0, List.of());

        // when
        SplitDecision decision = policy.decide(load);

        // then
        assertEquals(new SplitDecision.NoSplit(NoSplitReason.NO_VALID_POINT), decision);
    }

    @Test
    @DisplayName("요청이 한쪽에 몰려도 분할 지점은 같다 (부하를 보지 않음. Split-for-Heat와 비교되는 지점)")
    void ignoresWhereTheRequestsAre() {
        // given: key 1,000개. 하나는 고르게, 하나는 앞쪽 20%에 요청 80%
        TabletLoad uniform = Loads.uniform(1000, 4);
        TabletLoad hotRange = Loads.hotRange();

        // when
        SplitDecision uniformDecision = policy.decide(uniform);
        SplitDecision hotRangeDecision = policy.decide(hotRange);

        // then: 둘 다 2^63에서 자른다
        assertEquals(new SplitDecision.Split(1L << 63), uniformDecision);
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
