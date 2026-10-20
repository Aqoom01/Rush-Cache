package io.openkache.core.tablet.experiment;

import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * 실험 입력 생성. seed가 같으면 같은 key와 같은 요청열이 나온다.
 *
 * <p>key는 길이 16바이트의 난수이고, 실험은 key 자체가 아니라 {@link KeyHash}로 바꾼 해시값만 쓴다. 요청은 Zipf 분포로 key마다 요청
 * 수를 센 배열이다(요청열을 하나씩 보관하지 않는다).
 */
final class Workload {

    private Workload() {}

    /** 길이 16바이트 key {@code count}개의 해시값. */
    static long[] keyHashes(int count, long seed) {
        SplittableRandom rnd = new SplittableRandom(seed);
        long[] hashes = new long[count];
        byte[] key = new byte[16];
        for (int i = 0; i < count; i++) {
            rnd.nextBytes(key);
            hashes[i] = KeyHash.of(Bytes.copyOf(key));
        }
        return hashes;
    }

    /**
     * key 인덱스별 요청 수. 순위 {@code r}인 key의 확률은 {@code 1 / r^theta}에 비례하고, 어느 key가 몇 순위인지는 seed로 섞는다.
     *
     * @param keys key 개수
     * @param requests 총 요청 수
     * @param theta 쏠림 정도. 0이면 균등, 0.99는 흔히 쓰는 쏠림, 1.2는 극단
     */
    static int[] zipfRequests(int keys, int requests, double theta, long seed) {
        double[] cdf = new double[keys];
        double sum = 0;
        for (int i = 0; i < keys; i++) {
            sum += 1.0 / Math.pow(i + 1, theta);
            cdf[i] = sum;
        }
        for (int i = 0; i < keys; i++) {
            cdf[i] /= sum;
        }
        int[] rankToKey = new int[keys];
        for (int i = 0; i < keys; i++) {
            rankToKey[i] = i;
        }
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = keys - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int tmp = rankToKey[i];
            rankToKey[i] = rankToKey[j];
            rankToKey[j] = tmp;
        }
        int[] count = new int[keys];
        for (int r = 0; r < requests; r++) {
            int idx = Arrays.binarySearch(cdf, rnd.nextDouble());
            if (idx < 0) {
                idx = -idx - 1;
            }
            if (idx >= keys) {
                idx = keys - 1;
            }
            count[rankToKey[idx]]++;
        }
        return count;
    }
}
