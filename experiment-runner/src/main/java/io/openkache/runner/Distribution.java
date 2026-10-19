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
    FIXED_HOT_KEY
}
