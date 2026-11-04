package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
    /** {@link Distribution#HOT_RANGE}에서만 쓴다: key 번호를 해시값 오름차순으로 놓은 것과 앞쪽 뜨거운 key 수. */
    private final int[] hashOrder;
    private final int hotKeys;

    WorkloadGenerator(RunnerConfig config, Random random) {
        this.config = config;
        this.random = random;
        this.valueScratch = new byte[config.valueSize()];
        this.hashOrder = config.distribution() == Distribution.HOT_RANGE ? hashOrder(config) : new int[0];
        this.hotKeys = (int) (config.keyCount() * config.hotRangeKeyFraction());
    }

    /** key 번호 {@code 0..keyCount-1}을 {@link KeyHash} 부호 없는 오름차순으로 정렬한 배열. */
    static int[] hashOrder(RunnerConfig config) {
        int n = config.keyCount();
        long[] packed = new long[n];
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            packed[i] = KeyHash.of(key(i, config.keyLength()));
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> Long.compareUnsigned(packed[a], packed[b]));
        int[] result = new int[n];
        for (int i = 0; i < n; i++) {
            result[i] = order[i];
        }
        return result;
    }

    /** 다음 논리적 요청. */
    Command next() {
        int roll = random.nextInt(100);
        Bytes key = key(pickKeyIndex(), config.keyLength());
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
            case HOT_RANGE ->
                    random.nextDouble() < config.hotRangeRequestFraction()
                            ? hashOrder[random.nextInt(hotKeys)]
                            : hashOrder[hotKeys + random.nextInt(config.keyCount() - hotKeys)];
        };
    }

    /** key 번호 {@code index}의 key: {@code "key-<index>"}를 {@code keyLength} byte로 자르거나 0으로 채운다. */
    static Bytes key(int index, int keyLength) {
        String raw = "key-" + index;
        byte[] bytes = new byte[keyLength];
        byte[] rawBytes = raw.getBytes(StandardCharsets.UTF_8);
        int copyLength = Math.min(rawBytes.length, bytes.length);
        System.arraycopy(rawBytes, 0, bytes, 0, copyLength);
        return Bytes.copyOf(bytes);
    }
}
