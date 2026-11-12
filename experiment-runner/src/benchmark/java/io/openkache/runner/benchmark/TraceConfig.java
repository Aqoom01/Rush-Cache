package io.openkache.runner.benchmark;

import java.util.Objects;

/**
 * 논리 요청 생성 설정. 불변이며 thread 간 공유 가능하다.
 * key/value 크기는 byte, 비율은 정수 percent, hotProbability는 0..1이다.
 * 실제 송신·동시성·시간·복제 알고리즘은 포함하지 않는다.
 */
public record TraceConfig(
        long seed, long keyCount, int keyBytes, int valueBytes,
        int getPercent, int setPercent, int deletePercent,
        int streamCount, Distribution distribution, double hotProbability) {

    /** v1에서 구현한 분포만 수용한다. Zipf/moving-hot은 후속 범위다. */
    public enum Distribution { UNIFORM, FIXED_HOT }

    /** 큰 실수 입력으로 인한 단일 요청의 과도한 할당을 막는 v1 byte 상한. */
    public static final int MAX_BYTES = 1_048_576;

    public TraceConfig {
        if (keyCount <= 0) { throw new IllegalArgumentException("keyCount must be positive"); }
        if (keyBytes < KeyValueCodec.MIN_KEY_BYTES || keyBytes > MAX_BYTES) {
            throw new IllegalArgumentException("keyBytes must be 16..1048576");
        }
        if (valueBytes < KeyValueCodec.MIN_VALUE_BYTES || valueBytes > MAX_BYTES) {
            throw new IllegalArgumentException("valueBytes must be 44..1048576");
        }
        if (getPercent < 0 || getPercent > 100 || setPercent < 0 || setPercent > 100
                || deletePercent < 0 || deletePercent > 100
                || getPercent + setPercent + deletePercent != 100) {
            throw new IllegalArgumentException("GET/SET/DELETE percentages must sum to 100");
        }
        if (streamCount <= 0) { throw new IllegalArgumentException("streamCount must be positive"); }
        Objects.requireNonNull(distribution, "distribution");
        if (!Double.isFinite(hotProbability) || hotProbability < 0 || hotProbability > 1) {
            throw new IllegalArgumentException("hotProbability must be finite and in 0..1");
        }
        if (distribution == Distribution.UNIFORM && hotProbability != 0) {
            throw new IllegalArgumentException("UNIFORM requires hotProbability=0");
        }
        // -0.0 and +0.0 have the same semantics and canonical encoding.
        hotProbability = hotProbability == 0 ? 0.0 : hotProbability;
    }
}

