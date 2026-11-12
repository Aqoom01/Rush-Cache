package io.openkache.core.balance.split;

import io.openkache.core.balance.TabletLoad;

/**
 * Tablet을 어디서 나눌지, 나누지 않을지 정하는 정책.
 *
 * <p>분할을 시작할지(부하 기준, 지속 기간, 유예)는 모든 정책에 공통인 단계가 판단하고, 이 정책은 그 단계를 통과한 Tablet에
 * 대해서만 호출된다. 정책은 계획만 만들고 데이터를 옮기지 않는다.
 *
 * <p>구현은 다음을 지켜야 한다 (design.md §5).
 *
 * <ul>
 *   <li>{@code Split}의 {@code at}은 구간 안에 있고 시작점과 다르다. 그래서 두 조각 모두 비어 있지 않다.
 *   <li>같은 입력에는 같은 결정을 낸다. 난수와 실제 시각을 쓰지 않는다.
 *   <li>입력을 변경하지 않는다.
 * </ul>
 *
 * <p>상태를 갖지 않으므로 여러 thread에서 동시에 호출해도 안전해야 한다.
 */
public interface SplitPolicy {

    /** 실험 결과의 variant 값으로 쓰는 이름. 예: {@code midpoint-split}. */
    String variant();

    /** {@code load}를 보고 분할 여부와 지점을 정한다. */
    SplitDecision decide(TabletLoad load);
}
