package io.openkache.core.balance.split;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.tablet.TabletRange;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 모든 분할 정책이 지켜야 하는 규칙(design.md §5 불변식)을 검사한다.
 *
 * <p>테스트 하나가 "정책 × Tablet 모양" 조합마다 한 번씩 실행된다. 결과 화면에는 {@code [split-for-heat] 앞쪽 20% key에
 * 요청 80%}처럼 표시된다.
 *
 * <p>여기서는 결정이 "올바른 모양"인지만 본다. 분할 지점이 좋은지는 정책별 테스트와 시나리오에서 본다. 정책이 보류
 * ({@code NoSplit})를 낸 조합은 분할 결과에 대한 불변식(1, 2, 6)이 해당 없으므로 "건너뜀"으로 표시된다.
 */
@DisplayName("모든 분할 정책 공통 불변식 (design.md §5)")
class SplitInvariantTest {

    /** 검사할 정책 목록. 새 정책을 만들면 여기에 한 줄 추가한다. */
    private static final List<SplitPolicy> POLICIES =
            List.of(new MidpointSplitPolicy(), new HeatSplitPolicy(), new EStoreSplitPolicy());

    private static final TabletRange QUARTER_2 = new TabletRange(1L << 62, 1L << 63);

    /** 나눌 수 있는 여러 모양의 Tablet. 이름은 결과 화면에 나온다. */
    private static final List<Named<TabletLoad>> SPLITTABLE_LOADS = List.of(
            Named.of("key 2개", Loads.uniform(2, 10)),
            Named.of("균등한 key 1,000개", Loads.uniform(1000, 2)),
            Named.of("앞쪽 20% key에 요청 80%", Loads.hotRange()),
            Named.of("가운데 key 하나에 요청 전부", Loads.singleHotKey(2000)),
            Named.of("요청 0건", Loads.uniform(10, 0)),
            Named.of("양 끝이 막힌 구간 [2^62, 2^63)", Loads.tablet(QUARTER_2, 800, i -> i % 7)));

    /** 나눌 자리가 없는 Tablet: 폭이 1인 구간. */
    private static final List<Named<TabletLoad>> UNSPLITTABLE_LOADS = List.of(
            Named.of("폭 1 구간, key 0개", new TabletLoad(new TabletRange(5, 6), 0, List.of())),
            Named.of("폭 1 구간, key 1개", new TabletLoad(new TabletRange(5, 6), 0, List.of(new KeyLoad(5, 1000, 0)))));

    static Stream<Arguments> everyPolicyWithSplittableLoad() {
        return combine(SPLITTABLE_LOADS);
    }

    static Stream<Arguments> everyPolicyWithUnsplittableLoad() {
        return combine(UNSPLITTABLE_LOADS);
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 1: 자른 위치가 구간 안이고 시작점과 달라, 두 조각 모두 비어 있지 않다")
    void splitPointIsStrictlyInsideTheRange(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다. 조합 목록은 클래스 위쪽 POLICIES, *_LOADS 참고

        // when
        long at = splitPointOf(policy, load);

        // then: B1 TabletRange.splitAt이 받아들이는 지점이어야 RoutingTable.withSplit에 넘길 수 있다
        assertTrue(load.range().contains(at), "구간 안이어야 한다");
        assertTrue(at != load.range().start(), "시작점이면 왼쪽 조각이 빈다");
        assertDoesNotThrow(() -> load.range().splitAt(at));
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 2: 두 조각이 원래 구간을 빈틈·겹침 없이 덮고, 모든 key가 정확히 한쪽에 들어간다")
    void halvesCoverTheRangeWithoutGapOrOverlap(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다. 조합 목록은 클래스 위쪽 POLICIES, *_LOADS 참고

        // when
        long at = splitPointOf(policy, load);
        TabletRange.Split parts = load.range().splitAt(at);

        // then: [start, at) + [at, end) = [start, end)
        assertEquals(load.range().start(), parts.left().start());
        assertEquals(at, parts.left().end());
        assertEquals(at, parts.right().start());
        assertEquals(load.range().end(), parts.right().end());

        for (KeyLoad key : load.keys()) {
            boolean inLeft = parts.left().contains(key.keyHash());
            boolean inRight = parts.right().contains(key.keyHash());
            assertTrue(inLeft != inRight, Long.toUnsignedString(key.keyHash()) + "가 정확히 한쪽에만 있어야 한다");
        }
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 3: 같은 입력이면 항상 같은 결정을 낸다")
    void sameInputGivesSameDecision(SplitPolicy policy, TabletLoad load) {
        // given: 내용은 같지만 따로 만든 입력
        TabletLoad sameContent = copyOf(load);

        // when
        SplitDecision first = policy.decide(load);
        SplitDecision second = policy.decide(sameContent);

        // then
        assertEquals(first, second);
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 4: 결정한 뒤에도 입력이 바뀌지 않는다")
    void decidingDoesNotChangeInput(SplitPolicy policy, TabletLoad load) {
        // given: 실행 전 상태를 따로 보관
        TabletLoad before = copyOf(load);

        // when
        policy.decide(load);

        // then
        assertEquals(before, load);
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithUnsplittableLoad")
    @DisplayName("불변식 5: 나눌 자리가 없으면 NO_VALID_POINT 사유와 함께 보류한다")
    void unsplittableRangeIsNoSplitWithReason(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다. 조합 목록은 클래스 위쪽 POLICIES, *_LOADS 참고

        // when
        SplitDecision decision = policy.decide(load);

        // then
        SplitDecision.NoSplit noSplit = assertInstanceOf(SplitDecision.NoSplit.class, decision);
        assertEquals(NoSplitReason.NO_VALID_POINT, noSplit.reason());
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 6: 두 조각 요청 수의 합이 원래 Tablet의 요청 수와 같다")
    void requestsAreConservedAcrossHalves(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다. 조합 목록은 클래스 위쪽 POLICIES, *_LOADS 참고

        // when
        TabletRange.Split parts = load.range().splitAt(splitPointOf(policy, load));
        long left = requestsIn(load, parts.left());
        long right = requestsIn(load, parts.right());

        // then
        assertEquals(load.totalRequests(), left + right);
    }

    // ---- 도우미 ----

    /** 정책이 고른 분할 지점. 정책이 보류하면 이 테스트를 건너뛴다. */
    private static long splitPointOf(SplitPolicy policy, TabletLoad load) {
        if (!(policy.decide(load) instanceof SplitDecision.Split split)) {
            return abort("보류 결정이라 분할 결과에 대한 불변식은 해당 없음");
        }
        return split.at();
    }

    private static long requestsIn(TabletLoad load, TabletRange part) {
        return load.keys().stream()
                .filter(k -> part.contains(k.keyHash()))
                .mapToLong(KeyLoad::requests)
                .sum();
    }

    /** 내용은 같지만 별개의 객체인 입력. */
    private static TabletLoad copyOf(TabletLoad load) {
        return new TabletLoad(load.range(), load.window(), new ArrayList<>(load.keys()));
    }

    /** 모든 정책과 주어진 Tablet 모양을 짝지은 조합. 정책은 variant 이름으로 표시한다. */
    private static Stream<Arguments> combine(List<Named<TabletLoad>> loads) {
        return POLICIES.stream()
                .flatMap(policy -> loads.stream().map(load -> Arguments.of(Named.of(policy.variant(), policy), load)));
    }
}
