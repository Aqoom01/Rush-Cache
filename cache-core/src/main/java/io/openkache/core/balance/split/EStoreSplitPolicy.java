package io.openkache.core.balance.split;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import java.util.List;

/**
 * E-Store에서 착안한 hot/cold 분리 분할: hot key가 모인 구간의 경계에서 잘라 hot 데이터와 cold 데이터를 다른 Tablet으로 나눈다.
 *
 * <p>참고: Taft et al., "E-Store: Fine-Grained Elastic Partitioning for Distributed Transaction Processing Systems",
 * PVLDB 8(3), 2015. E-Store는 접근 빈도 상위 tuple(hot)을 개별로, 나머지(cold)를 큰 블록으로 관리하는 두 단계 분할을 쓴다. 이
 * 정책은 그 아이디어 중 "hot을 cold에서 떼어 낸다"만 가져왔다. 아래 기준값과 절차는 논문에 없는 프로젝트 자체 결정이다
 * ({@code experiments/b2-load-balancing/references/estore.md}).
 *
 * <ul>
 *   <li>hot key: 요청 수가 key 평균의 {@code hotFactor}배 이상인 key. 기본 2.0
 *   <li>해시값 순서로 이어진 hot key 묶음(run) 중 요청 합이 가장 큰 것을 고르고, 그 시작 또는 끝에서 자른다. 두 자리 중 큰 쪽
 *       비율이 작은 쪽을 고르고, 같으면 끝을 고른다
 *   <li>hot key가 없거나 모든 key가 hot이면 {@link NoSplitReason#NO_HOT_DATA}
 * </ul>
 *
 * <p>한 번에 한 번만 자르는 {@link SplitPolicy} 계약이라 hot key 하나를 완전히 떼어 내려면 두 번 잘라야 한다. 그래서 한 번의 결정으로는
 * {@code split-for-heat}보다 큰 쪽 비율이 클 수 있다. 반복 분할에서의 효과는 Benchmark C로 본다.
 */
public final class EStoreSplitPolicy implements SplitPolicy {

    public static final double DEFAULT_HOT_FACTOR = 2.0;

    private final double hotFactor;

    public EStoreSplitPolicy() {
        this(DEFAULT_HOT_FACTOR);
    }

    /** @param hotFactor hot key 판정 배수. 1 초과 */
    public EStoreSplitPolicy(double hotFactor) {
        if (!(hotFactor > 1)) {
            throw new IllegalArgumentException("hotFactor must be > 1: " + hotFactor);
        }
        this.hotFactor = hotFactor;
    }

    @Override
    public String variant() {
        return "estore-inspired";
    }

    @Override
    public SplitDecision decide(TabletLoad load) {
        List<KeyLoad> keys = load.keys();
        int n = keys.size();
        if (n < 2) {
            return new SplitDecision.NoSplit(NoSplitReason.NO_VALID_POINT);
        }
        long total = load.totalRequests();
        if (total == 0) {
            return new SplitDecision.NoSplit(NoSplitReason.NO_HOT_DATA);
        }

        // 가장 뜨거운 hot run [bestStart, bestEnd). 평균 비교는 곱셈으로 해 나눗셈 오차를 피한다.
        int bestStart = -1;
        int bestEnd = -1;
        long bestSum = -1;
        int i = 0;
        while (i < n) {
            if (!isHot(keys.get(i), total, n)) {
                i++;
                continue;
            }
            int start = i;
            long sum = 0;
            while (i < n && isHot(keys.get(i), total, n)) {
                sum += keys.get(i).requests();
                i++;
            }
            if (sum > bestSum) {
                bestStart = start;
                bestEnd = i;
                bestSum = sum;
            }
        }
        if (bestStart < 0 || (bestStart == 0 && bestEnd == n)) {
            return new SplitDecision.NoSplit(NoSplitReason.NO_HOT_DATA);
        }

        // 자를 후보: run의 끝(오른쪽 첫 cold key)과 시작(run의 첫 key). 0번 key에서는 자를 수 없다.
        int cut = -1;
        long cutLarger = Long.MAX_VALUE;
        for (int candidate : new int[] {bestEnd, bestStart}) {
            if (candidate <= 0 || candidate >= n) {
                continue;
            }
            long left = 0;
            for (int k = 0; k < candidate; k++) {
                left += keys.get(k).requests();
            }
            long larger = Math.max(left, total - left);
            if (larger < cutLarger) {
                cut = candidate;
                cutLarger = larger;
            }
        }
        return new SplitDecision.Split(keys.get(cut).keyHash());
    }

    private boolean isHot(KeyLoad key, long total, int n) {
        return key.requests() * (double) n >= hotFactor * total;
    }
}
