package io.openkache.core.balance.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** estore-inspired가 hot key 묶음의 경계에서 hot과 cold를 나누고, hot이 없으면 보류하는지 확인한다. */
@DisplayName("EStoreSplitPolicy (hot key 묶음의 경계에서 hot/cold 분리)")
class EStoreSplitPolicyTest {

    private final SplitPolicy policy = new EStoreSplitPolicy();

    @Test
    @DisplayName("앞쪽 20% key에 요청 80%면 hot 묶음의 끝(200번 key)에서 잘라 hot 3,200 : cold 800으로 나눈다")
    void hotRangeIsSeparatedFromCold() {
        // given: 평균 4건, hot 기준 8건 이상 → 앞쪽 200개(16건)가 hot
        TabletLoad load = Loads.hotRange();

        // when
        SplitDecision decision = policy.decide(load);

        // then: 반반이 아니라 hot/cold 경계에서 자른다(split-for-heat는 125번)
        assertEquals(new SplitDecision.Split(Loads.key(200, 1000)), decision);
        assertEquals(3200, leftRequests(load, decision));
    }

    @Test
    @DisplayName("가운데 key 하나만 hot이면 그 key의 경계에서 잘라 hot key를 한쪽 끝에 둔다")
    void singleHotKeyIsCutBesideIt() {
        TabletLoad load = Loads.tablet(1000, i -> i == 500 ? 9000 : 1);

        SplitDecision decision = policy.decide(load);

        // then: 끝(501번)에서 자르면 큰 쪽 500 + 9,000 = 9,500, 시작(500번)에서 자르면 9,000 + 499 = 9,499 → 시작
        assertEquals(new SplitDecision.Split(Loads.key(500, 1000)), decision);
    }

    @Test
    @DisplayName("hot key가 없으면(균등) NO_HOT_DATA로 보류한다")
    void uniformLoadHasNoHotData() {
        assertEquals(new SplitDecision.NoSplit(NoSplitReason.NO_HOT_DATA), policy.decide(Loads.uniform(1000, 3)));
        assertEquals(new SplitDecision.NoSplit(NoSplitReason.NO_HOT_DATA), policy.decide(Loads.uniform(10, 0)));
    }

    @Test
    @DisplayName("hot 묶음이 여럿이면 요청 합이 가장 큰 묶음의 경계에서 자른다")
    void hottestRunWins() {
        // given: 100-109번(합 100 x 10)과 700-749번(합 50 x 50) 두 묶음. 뒤 묶음이 더 뜨겁다
        TabletLoad load = Loads.tablet(1000, i -> i >= 100 && i < 110 ? 100 : i >= 700 && i < 750 ? 50 : 1);

        SplitDecision decision = policy.decide(load);

        long at = ((SplitDecision.Split) decision).at();
        boolean atRunEdge = at == Loads.key(700, 1000) || at == Loads.key(750, 1000);
        assertEquals(true, atRunEdge, "뒤 묶음의 시작 또는 끝이어야 한다");
    }

    @Test
    @DisplayName("hotFactor는 1보다 커야 한다")
    void rejectsHotFactorNotAboveOne() {
        assertThrows(IllegalArgumentException.class, () -> new EStoreSplitPolicy(1.0));
    }

    private static long leftRequests(TabletLoad load, SplitDecision decision) {
        long at = ((SplitDecision.Split) decision).at();
        return load.keys().stream()
                .filter(k -> Long.compareUnsigned(k.keyHash(), at) < 0)
                .mapToLong(KeyLoad::requests)
                .sum();
    }
}
