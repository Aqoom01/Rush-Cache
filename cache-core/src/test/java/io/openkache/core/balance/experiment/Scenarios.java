package io.openkache.core.balance.experiment;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.TabletRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Split 비교 실험의 입력: seed별 key 해시값과 시나리오별 요청 분포. design.md §6. */
final class Scenarios {

    private Scenarios() {}

    /** 요청 패턴. 아래 설명의 "앞쪽"은 해시값이 작은 쪽이다. */
    enum Scenario {
        /** S1: 모든 key에 고르게. */
        S1_UNIFORM,
        /** S2: 요청의 80%가 앞쪽 20% key에 고르게, 나머지 20%가 나머지 key에 고르게. */
        S2_HOT_RANGE,
        /** S3: 요청의 90%가 가운데(n/2번째) key 하나에, 나머지 10%가 다른 key에 고르게. */
        S3_SINGLE_HOT_KEY
    }

    /** seed별 16바이트 난수 key {@code n}개의 해시값. 부호 없는 오름차순이며 중복이 없다. */
    static long[] keyHashes(int n, long seed) {
        Random random = new Random(seed);
        byte[] key = new byte[16];
        long[] hashes = new long[n];
        for (int i = 0; i < n; i++) {
            random.nextBytes(key);
            hashes[i] = KeyHash.of(Bytes.copyOf(key));
        }
        // 부호 비트를 뒤집으면 부호 있는 정렬이 부호 없는 순서와 같아진다.
        for (int i = 0; i < n; i++) {
            hashes[i] ^= Long.MIN_VALUE;
        }
        Arrays.sort(hashes);
        for (int i = 0; i < n; i++) {
            hashes[i] ^= Long.MIN_VALUE;
            if (i > 0 && hashes[i] == hashes[i - 1]) {
                throw new IllegalStateException("hash collision at seed " + seed);
            }
        }
        return hashes;
    }

    /** window 하나 동안 key별 요청 수. 배열 위치는 {@link #keyHashes}의 위치와 같다. */
    static long[] requests(Scenario scenario, int n, int total, Random random) {
        long[] counts = new long[n];
        for (int r = 0; r < total; r++) {
            counts[pick(scenario, n, random)]++;
        }
        return counts;
    }

    static TabletLoad load(long[] hashes, long[] counts, long window) {
        List<KeyLoad> keys = new ArrayList<>(hashes.length);
        for (int i = 0; i < hashes.length; i++) {
            keys.add(new KeyLoad(hashes[i], counts[i], 0));
        }
        return new TabletLoad(TabletRange.FULL, window, keys);
    }

    private static int pick(Scenario scenario, int n, Random random) {
        return switch (scenario) {
            case S1_UNIFORM -> random.nextInt(n);
            case S2_HOT_RANGE -> {
                int hot = n / 5;
                yield random.nextDouble() < 0.8 ? random.nextInt(hot) : hot + random.nextInt(n - hot);
            }
            case S3_SINGLE_HOT_KEY -> {
                int hot = n / 2;
                if (random.nextDouble() < 0.9) {
                    yield hot;
                }
                int other = random.nextInt(n - 1);
                yield other >= hot ? other + 1 : other;
            }
        };
    }
}
