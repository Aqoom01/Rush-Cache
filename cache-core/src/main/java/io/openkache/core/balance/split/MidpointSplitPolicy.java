package io.openkache.core.balance.split;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import java.util.List;

/**
 * Baseline: 범위 안 key를 순서대로 놓았을 때 가운데 key에서 나눈다. 요청 부하는 보지 않는다.
 *
 * <p>DynamoDB 논문(USENIX ATC 2022) §4.4가 부하 기반 분할의 비교 대상으로 든 "key 범위를 가운데에서 나누는 방식"에
 * 해당한다. 바이트열 key 공간의 산술적 중간이 아니라 존재하는 key의 순서상 가운데를 쓴다. 범위 끝이 무한대일 때도 정의되고,
 * key가 범위에 고르게 퍼져 있으면 둘은 거의 같다.
 *
 * <p>key가 n개면 {@code n / 2}번째(0부터) key가 오른쪽 범위의 첫 key가 된다. 왼쪽에 {@code n / 2}개, 오른쪽에 나머지가
 * 간다. 효과가 없을 것 같은 분할도 보류하지 않는다.
 */
public final class MidpointSplitPolicy implements SplitPolicy {

    @Override
    public String variant() {
        return "midpoint-split";
    }

    @Override
    public SplitDecision decide(TabletLoad load) {
        List<KeyLoad> keys = load.keys();
        if (keys.size() < 2) {
            return new SplitDecision.NoSplit(NoSplitReason.NO_VALID_POINT);
        }
        return new SplitDecision.Split(keys.get(keys.size() / 2).key());
    }
}
