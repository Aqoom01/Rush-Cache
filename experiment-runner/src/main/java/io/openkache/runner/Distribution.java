package io.openkache.runner;

/**
 * 요청의 key 선택 분포.
 *
 * <p>Zipfian은 명세서의 "선택" 항목이라 아직 구현하지 않았다. 설정에 {@code zipfian}을 쓰면 알 수 없는 값으로 거절되어
 * {@link RunnerConfig}가 기동 전에 {@link ConfigException}을 던진다.
 */
public enum Distribution {
    /** {@code key.count}개의 key에서 균등하게 고른다. */
    UNIFORM,
    /** {@code hot.key.fraction} 확률로 고정된 하나의 key(index 0)를 고르고, 그 외에는 균등하게 고른다. */
    FIXED_HOT_KEY,
    /**
     * key를 해시값({@code KeyHash}) 오름차순으로 놓고 앞쪽 {@code hot.range.key.fraction}의 key에 요청의
     * {@code hot.range.request.fraction}을, 나머지 key에 나머지를 각각 균등하게 보낸다. 해시 구간 하나에 부하가 몰린
     * Tablet을 만든다(B2 split-comparison의 S2와 같은 모양). {@code hot.range.shift.per.window}를 주면 window마다 그 비율만큼
     * 구간이 해시 순서로 이동한다(끝에서 처음으로 이어진다).
     */
    HOT_RANGE,
    /**
     * Zipf 분포(지수 {@code zipf.exponent}, 기본 1.0). 순위 1인 key가 가장 많이 받는다. 순위와 key 번호의 대응은 seed로 섞어 해시
     * 순서와 무관하다.
     */
    ZIPF
}
