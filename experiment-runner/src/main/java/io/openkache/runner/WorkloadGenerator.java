package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * {@link RunnerConfig}로부터 논리적 요청({@link Command})을 하나씩 만든다.
 *
 * <p>요청을 미리 전부 만들어 메모리에 쌓지 않고 {@link #next()}를 호출할 때마다 하나씩 만든다. 같은 {@link RunnerConfig}와
 * 같은 {@link Random} 시드로 만들면 같은 요청열이 나온다(연산 종류·key 선택만 재현된다. value 내용은 매 SET마다 새로
 * 채우지만 크기는 {@code valueSize}로 고정이다).
 *
 * <p>thread 하나가 소유하며 공유하지 않는다. {@link Random}은 thread-safe하지 않다.
 */
final class WorkloadGenerator {

    private final RunnerConfig config;
    private final Random random;
    private final byte[] valueScratch;

    WorkloadGenerator(RunnerConfig config, Random random) {
        this.config = config;
        this.random = random;
        this.valueScratch = new byte[config.valueSize()];
    }

    /** 다음 논리적 요청. */
    Command next() {
        int roll = random.nextInt(100);
        Bytes key = key(pickKeyIndex());
        if (roll < config.mixGetPercent()) {
            return new Command.Get(key);
        } else if (roll < config.mixGetPercent() + config.mixSetPercent()) {
            random.nextBytes(valueScratch);
            return new Command.Set(key, Bytes.copyOf(valueScratch));
        } else {
            return new Command.Delete(key);
        }
    }

    private int pickKeyIndex() {
        return switch (config.distribution()) {
            case UNIFORM -> random.nextInt(config.keyCount());
            case FIXED_HOT_KEY ->
                    random.nextDouble() < config.hotKeyFraction() ? 0 : random.nextInt(config.keyCount());
        };
    }

    private Bytes key(int index) {
        String raw = "key-" + index;
        byte[] bytes = new byte[config.keyLength()];
        byte[] rawBytes = raw.getBytes(StandardCharsets.UTF_8);
        int copyLength = Math.min(rawBytes.length, bytes.length);
        System.arraycopy(rawBytes, 0, bytes, 0, copyLength);
        return Bytes.copyOf(bytes);
    }
}
