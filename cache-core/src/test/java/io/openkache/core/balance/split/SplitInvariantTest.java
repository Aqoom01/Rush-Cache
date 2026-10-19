package io.openkache.core.balance.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.KeyRange;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.model.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 모든 분할 정책이 지켜야 하는 규칙(design.md §5 불변식)을 검사한다.
 *
 * <p>테스트 하나가 "정책 × Tablet 모양" 조합마다 한 번씩 실행된다. 예: 정책 1개 × 모양 6개 = 6번. 결과 화면에는 {@code
 * [midpoint-split] 앞쪽 20% key에 요청 80%}처럼 표시된다.
 *
 * <p>여기서는 결정이 "올바른 모양"인지만 본다. 분할 지점이 좋은지(부하가 고르게 나뉘는지)는 정책별 테스트와 시나리오에서 본다.
 *
 * <p>정책이 보류({@code NoSplit})를 낸 경우, 분할 결과에 대한 불변식(1, 2, 6)은 해당 없으므로 그 조합은 "건너뜀"으로 표시된다.
 */
@DisplayName("모든 분할 정책 공통 불변식 (design.md §5)")
class SplitInvariantTest {

    /** 검사할 정책 목록. 새 정책을 만들면 여기에 한 줄 추가한다. */
    private static final List<SplitPolicy> POLICIES = List.of(new MidpointSplitPolicy());

    private static final KeyRange KEY_100_TO_900 =
            new KeyRange(Optional.of(Loads.key(100)), Optional.of(Loads.key(900)));

    /** 나눌 수 있는(key 2개 이상) 여러 모양의 Tablet. 이름은 결과 화면에 나온다. */
    private static final List<Named<TabletLoad>> SPLITTABLE_LOADS = List.of(
            Named.of("key 2개", Loads.uniform(2, 10)),
            Named.of("균등한 key 1,000개", Loads.uniform(1000, 2)),
            Named.of("앞쪽 20% key에 요청 80%", Loads.hotRange()),
            Named.of("가운데 key 하나에 요청 전부", Loads.singleHotKey(2000)),
            Named.of("요청 0건", Loads.uniform(10, 0)),
            Named.of("양 끝이 막힌 범위 [key-0100, key-0900)", Loads.tablet(KEY_100_TO_900, 100, 800, i -> i % 7)));

    /** 나눌 수 없는(key 2개 미만) Tablet. */
    private static final List<Named<TabletLoad>> UNSPLITTABLE_LOADS = List.of(
            Named.of("key 0개", Loads.uniform(0, 0)),
            Named.of("key 1개", Loads.uniform(1, 1000)));

    static Stream<Arguments> everyPolicyWithSplittableLoad() {
        return combine(SPLITTABLE_LOADS);
    }

    static Stream<Arguments> everyPolicyWithUnsplittableLoad() {
        return combine(UNSPLITTABLE_LOADS);
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 1: 자른 위치가 범위 안에 있고, 두 쪽 모두에 key가 있다")
    void bothHalvesHaveKeys(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다.

        // when
        SplitOutcome outcome = splitWith(policy, load);

        // then
        assertTrue(load.range().isSplittableAt(outcome.splitKey()), "자른 위치가 범위 안(start 초과, end 미만)이어야 한다");
        assertTrue(outcome.leftKeys().size() > 0, "왼쪽에 key가 하나 이상 있어야 한다");
        assertTrue(outcome.rightKeys().size() > 0, "오른쪽에 key가 하나 이상 있어야 한다");
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 2: 두 쪽이 원래 범위를 빈틈·겹침 없이 덮는다")
    void halvesCoverTheRangeWithoutGapOrOverlap(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다.

        // when
        SplitOutcome outcome = splitWith(policy, load);

        // then: 범위의 경계가 이어진다. [start, splitKey) + [splitKey, end) = [start, end)
        assertEquals(load.range().start(), outcome.left().start());
        assertEquals(Optional.of(outcome.splitKey()), outcome.left().end());
        assertEquals(Optional.of(outcome.splitKey()), outcome.right().start());
        assertEquals(load.range().end(), outcome.right().end());

        // then: 모든 key가 정확히 한쪽에만 들어간다
        for (KeyLoad key : load.keys()) {
            boolean inLeft = outcome.left().contains(key.key());
            boolean inRight = outcome.right().contains(key.key());
            assertTrue(inLeft != inRight, key.key() + "가 정확히 한쪽에만 있어야 한다");
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
    @DisplayName("불변식 5: key가 2개 미만이면 NO_VALID_POINT 사유와 함께 보류한다")
    void fewerThanTwoKeysIsNoSplitWithReason(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다.

        // when
        SplitDecision decision = policy.decide(load);

        // then
        SplitDecision.NoSplit noSplit = assertInstanceOf(SplitDecision.NoSplit.class, decision);
        assertEquals(NoSplitReason.NO_VALID_POINT, noSplit.reason());
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @MethodSource("everyPolicyWithSplittableLoad")
    @DisplayName("불변식 6: 두 쪽 요청 수의 합이 원래 Tablet의 요청 수와 같다")
    void requestsAreConservedAcrossHalves(SplitPolicy policy, TabletLoad load) {
        // given: 정책(policy)과 Tablet(load)은 파라미터로 받는다.

        // when
        SplitOutcome outcome = splitWith(policy, load);

        // then
        assertEquals(load.totalRequests(), outcome.leftRequests() + outcome.rightRequests());
    }

    // ---- Helper ----

    /**
     * 분할 결과. 정책이 고른 위치와, 그 위치로 나눈 두 범위, 각 범위에 들어간 key.
     *
     * @param splitKey 오른쪽 범위의 첫 key
     * @param left 왼쪽 범위 {@code [start, splitKey)}
     * @param right 오른쪽 범위 {@code [splitKey, end)}
     * @param leftKeys 왼쪽 범위에 들어간 key
     * @param rightKeys 오른쪽 범위에 들어간 key
     */
    private record SplitOutcome(
            Bytes splitKey, KeyRange left, KeyRange right, List<KeyLoad> leftKeys, List<KeyLoad> rightKeys) {

        long leftRequests() {
            return leftKeys.stream().mapToLong(KeyLoad::requests).sum();
        }

        long rightRequests() {
            return rightKeys.stream().mapToLong(KeyLoad::requests).sum();
        }
    }

    /** 정책을 실행하고, 고른 위치로 Tablet을 나눈 결과를 돌려준다. 정책이 보류하면 이 테스트를 건너뛴다. */
    private static SplitOutcome splitWith(SplitPolicy policy, TabletLoad load) {
        if (!(policy.decide(load) instanceof SplitDecision.Split split)) {
            return abort("보류 결정이라 분할 결과에 대한 불변식은 해당 없음");
        }
        Bytes splitKey = split.splitKey();
        KeyRange left = load.range().leftOf(splitKey);
        KeyRange right = load.range().rightOf(splitKey);
        List<KeyLoad> leftKeys = load.keys().stream().filter(k -> left.contains(k.key())).toList();
        List<KeyLoad> rightKeys = load.keys().stream().filter(k -> right.contains(k.key())).toList();
        return new SplitOutcome(splitKey, left, right, leftKeys, rightKeys);
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
