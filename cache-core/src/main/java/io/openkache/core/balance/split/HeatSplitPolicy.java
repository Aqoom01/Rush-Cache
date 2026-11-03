package io.openkache.core.balance.split;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import java.util.List;

/**
 * Split-for-Heat: 관찰한 요청 부하가 양쪽으로 가장 고르게 나뉘는 key에서 나누고, 나눠도 효과가 없으면 보류한다.
 *
 * <p>DynamoDB 논문(USENIX ATC 2022) §4.4와 AWS 블로그 "Scaling DynamoDB Part 2"의 아이디어를 따른 프로젝트 구현이다.
 * 기준값과 계산 방식은 자료에 공개되지 않아 design.md §4.3에서 정했다.
 *
 * <ul>
 *   <li>분할 지점: key를 해시값 순서로 놓고, 왼쪽 요청 합이 전체의 절반에 가장 가까워지는 key가 오른쪽의 첫 key가 된다.
 *       같은 값이면 key 개수의 가운데에 가까운 쪽을 고른다.
 *   <li>보류: 그렇게 나눠도 큰 쪽 요청 비율이 {@code theta}를 넘으면 {@link NoSplitReason#SINGLE_HOT_KEY}.
 * </ul>
 */
public final class HeatSplitPolicy implements SplitPolicy {

    /** 효과 판단 기준 θ의 기본값. 가운데 key 하나가 요청의 60%를 넘게 받으면 보류하는 것과 같다(design.md §4.3). */
    public static final double DEFAULT_THETA = 0.8;

    private final double theta;

    public HeatSplitPolicy() {
        this(DEFAULT_THETA);
    }

    /** @param theta 분할 후 큰 쪽 요청 비율의 허용 상한. 0 초과 1 이하 */
    public HeatSplitPolicy(double theta) {
        if (!(theta > 0 && theta <= 1)) {
            throw new IllegalArgumentException("theta must be in (0, 1]: " + theta);
        }
        this.theta = theta;
    }

    @Override
    public String variant() {
        return "split-for-heat";
    }

    @Override
    public SplitDecision decide(TabletLoad load) {
        List<KeyLoad> keys = load.keys();
        int n = keys.size();
        if (n < 2) {
            return new SplitDecision.NoSplit(NoSplitReason.NO_VALID_POINT);
        }

        long total = load.totalRequests();
        int best = -1;
        long bestLarger = Long.MAX_VALUE;
        long left = 0;
        // k번째 key가 오른쪽의 첫 key일 때, 두 쪽 중 큰 쪽 요청 수가 가장 작은 k를 찾는다.
        for (int k = 1; k < n; k++) {
            left += keys.get(k - 1).requests();
            long larger = Math.max(left, total - left);
            boolean closerToMiddle = Math.abs(k - n / 2) < Math.abs(best - n / 2);
            if (larger < bestLarger || (larger == bestLarger && closerToMiddle)) {
                best = k;
                bestLarger = larger;
            }
        }

        if (bestLarger > theta * total) {
            return new SplitDecision.NoSplit(NoSplitReason.SINGLE_HOT_KEY);
        }
        return new SplitDecision.Split(keys.get(best).keyHash());
    }
}
