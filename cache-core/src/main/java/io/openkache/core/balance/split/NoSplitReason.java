package io.openkache.core.balance.split;

/**
 * 분할을 보류한 사유.
 *
 * <p>design.md §4의 분할 시작 조건·유예 사유는 그 판단(SplitTrigger)을 구현할 때 추가한다.
 */
public enum NoSplitReason {
    /** 나눌 자리가 없다. 구간 폭이 1이거나, 부하 기준 정책에서 key가 둘 미만이다. */
    NO_VALID_POINT,

    /** 부하가 key 하나에 몰려, 어디서 나눠도 그 key가 있는 쪽이 효과 판단 기준(θ)을 넘는다. */
    SINGLE_HOT_KEY
}
