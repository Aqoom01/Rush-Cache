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
 * <p>window를 받으면 window에 따라 바뀌는 부하(이동하는 hot range, burst)를 만든다. window가 없는 실행은 window 0이다.
 *
 * <p>thread 하나가 소유하며 공유하지 않는다. {@link Random}은 thread-safe하지 않다.
 */
final class WorkloadGenerator {

    private final RunnerConfig config;
    private final KeySpace keys;
    private final Random random;
    private final byte[] valueScratch;
    private final int hotKeys;
    private final int hotStart;
    /** burst window면 burst 구간의 시작(해시 순서 번호), 아니면 -1. */
    private final int burstStart;
    private final int burstWidth;

    WorkloadGenerator(RunnerConfig config, Random random) {
        this(config, KeySpace.of(config), random, 0);
    }

    WorkloadGenerator(RunnerConfig config, KeySpace keys, Random random, int window) {
        this.config = config;
        this.keys = keys;
        this.random = random;
        this.valueScratch = new byte[config.valueSize()];
        int n = config.keyCount();
        this.hotKeys = (int) (n * config.hotRangeKeyFraction());
        this.hotStart = (int) (Math.round(window * config.hotRangeShiftPerWindow() * n) % n);
        this.burstWidth = Math.max(1, (int) (n * config.burstWidth()));
        this.burstStart = isBurst(config, window)
                ? (int) Math.floorMod(mix(config.seed() * 0x9E3779B97F4A7C15L + window), (long) (n - burstWidth + 1))
                : -1;
    }

    /** {@code window}가 burst window인가: {@code burst.every.windows}마다 마지막 window. */
    static boolean isBurst(RunnerConfig config, int window) {
        int every = config.burstEveryWindows();
        return every > 0 && window % every == every - 1;
    }

    /** window {@code window}에 보낼 요청 수. burst window는 기본 수의 (1 + burst.factor)배다. */
    static long requestsIn(RunnerConfig config, long base, int window) {
        return isBurst(config, window) ? Math.round(base * (1 + config.burstFactor())) : base;
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
        int n = config.keyCount();
        if (burstStart >= 0 && random.nextDouble() < config.burstFactor() / (1 + config.burstFactor())) {
            return keys.hashOrder[burstStart + random.nextInt(burstWidth)];
        }
        return switch (config.distribution()) {
            case UNIFORM -> random.nextInt(n);
            case FIXED_HOT_KEY -> random.nextDouble() < config.hotKeyFraction() ? 0 : random.nextInt(n);
            case HOT_RANGE -> random.nextDouble() < config.hotRangeRequestFraction()
                    ? keys.hashOrder[(hotStart + random.nextInt(hotKeys)) % n]
                    : keys.hashOrder[(hotStart + hotKeys + random.nextInt(n - hotKeys)) % n];
            case ZIPF -> keys.zipfKey(random.nextDouble());
        };
    }

    /** key 번호 {@code 0..keyCount-1}을 해시 오름차순으로 정렬한 배열. {@link KeySpace#hashOrder(RunnerConfig)}와 같다. */
    static int[] hashOrder(RunnerConfig config) {
        return KeySpace.hashOrder(config);
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

    /** SplitMix64 마무리 단계. */
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
