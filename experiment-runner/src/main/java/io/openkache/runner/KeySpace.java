package io.openkache.runner;

import io.openkache.core.tablet.KeyHash;
import java.util.Arrays;
import java.util.Random;

/**
 * 실행 하나에서 모든 {@link WorkloadGenerator}가 함께 읽는 key 공간 정보. 한 번만 계산하고 바꾸지 않으므로 여러 thread에서 읽을 수
 * 있다.
 *
 * <ul>
 *   <li>{@link #hashOrder}: key 번호를 {@link KeyHash} 부호 없는 오름차순으로 놓은 것(HOT_RANGE, burst 구간)
 *   <li>Zipf 누적 분포와 순위 → key 번호 대응(ZIPF). 순위는 {@code seed}로 섞어 해시 순서와 무관하다
 * </ul>
 */
final class KeySpace {

    final int[] hashOrder;
    final double[] zipfCdf;
    final int[] keyOfRank;

    private KeySpace(int[] hashOrder, double[] zipfCdf, int[] keyOfRank) {
        this.hashOrder = hashOrder;
        this.zipfCdf = zipfCdf;
        this.keyOfRank = keyOfRank;
    }

    static KeySpace of(RunnerConfig config) {
        int n = config.keyCount();
        boolean needsOrder = config.distribution() == Distribution.HOT_RANGE || config.burstEveryWindows() > 0;
        int[] order = needsOrder ? hashOrder(config) : new int[0];
        if (config.distribution() != Distribution.ZIPF) {
            return new KeySpace(order, new double[0], new int[0]);
        }
        double[] cdf = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            sum += 1.0 / Math.pow(i + 1, config.zipfExponent());
            cdf[i] = sum;
        }
        for (int i = 0; i < n; i++) {
            cdf[i] /= sum;
        }
        int[] keyOfRank = new int[n];
        for (int i = 0; i < n; i++) {
            keyOfRank[i] = i;
        }
        Random shuffle = new Random(config.seed() * 31 + 17);
        for (int i = n - 1; i > 0; i--) {
            int j = shuffle.nextInt(i + 1);
            int t = keyOfRank[i];
            keyOfRank[i] = keyOfRank[j];
            keyOfRank[j] = t;
        }
        return new KeySpace(order, cdf, keyOfRank);
    }

    /** key 번호 {@code 0..keyCount-1}을 {@link KeyHash} 부호 없는 오름차순으로 정렬한 배열. */
    static int[] hashOrder(RunnerConfig config) {
        int n = config.keyCount();
        long[] hashes = new long[n];
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            hashes[i] = KeyHash.of(WorkloadGenerator.key(i, config.keyLength()));
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> Long.compareUnsigned(hashes[a], hashes[b]));
        int[] result = new int[n];
        for (int i = 0; i < n; i++) {
            result[i] = order[i];
        }
        return result;
    }

    /** Zipf 순위 하나를 뽑아 key 번호로 바꾼다. */
    int zipfKey(double u) {
        int lo = 0;
        int hi = zipfCdf.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (zipfCdf[mid] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return keyOfRank[lo];
    }
}
