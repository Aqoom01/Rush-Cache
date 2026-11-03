package io.openkache.core.balance.split;

import io.openkache.core.balance.TabletLoad;
import io.openkache.core.tablet.TabletRange;

/**
 * Baseline: 해시 구간을 산술적 가운데에서 나눈다. key 분포와 요청 부하는 보지 않는다.
 *
 * <p>DynamoDB 논문(USENIX ATC 2022) §4.4가 부하 기반 분할의 비교 대상으로 든 "key 범위를 가운데에서 나누는 방식"에
 * 해당한다. 구간 폭이 1이면 나눌 자리가 없어 보류한다.
 */
public final class MidpointSplitPolicy implements SplitPolicy {

    @Override
    public String variant() {
        return "midpoint-split";
    }

    @Override
    public SplitDecision decide(TabletLoad load) {
        TabletRange range = load.range();
        long half = halfWidth(range);
        if (half == 0) {
            return new SplitDecision.NoSplit(NoSplitReason.NO_VALID_POINT);
        }
        return new SplitDecision.Split(range.start() + half);
    }

    /** 구간 폭의 절반(부호 없음). 전체 공간은 폭이 2^64라 long으로 표현할 수 없어 따로 2^63을 돌려준다. */
    private static long halfWidth(TabletRange range) {
        if (range.equals(TabletRange.FULL)) {
            return 1L << 63;
        }
        // end == 0(공간의 끝)이어도 0 - start가 부호 없는 폭 2^64 - start가 된다.
        return Long.divideUnsigned(range.end() - range.start(), 2);
    }
}
