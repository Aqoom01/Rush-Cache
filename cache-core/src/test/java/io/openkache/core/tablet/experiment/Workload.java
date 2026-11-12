package io.openkache.core.tablet.experiment;

import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.KeyHash;
import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * 실험 입력 생성. seed가 같으면 같은 key와 같은 요청열이 나온다.
 *
 * <p>key는 길이 16바이트의 난수이고, 실험은 key 자체가 아니라 {@link KeyHash}로 바꾼 해시값만 쓴다. 요청은 Zipf 분포로 key마다 요청
 * 수를 센 배열이다(요청열을 하나씩 보관하지 않는다). 태그 묶음 시나리오(S7)만 실제 key 바이트열을 만들고 variant의
 * {@link DistributionScheme#keyHash}로 해시한다.
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

    /** 태그(hash tag) 묶음이 있는 key 집합. {@code groupOf[i]}는 key i의 묶음 번호(0..groups-1). */
    record Tagged(Bytes[] keys, int[] groupOf, int groups) {}

    /**
     * 묶음 {@code groups}개에 key {@code count}개를 나눠 담은 key 집합. 묶음 g의 key는 {@code "{g<g>}:<i>"} 꼴이라 tag가 {@code g<g>}다.
     * 묶음별 key 수는 {@link #zipfRequests}와 같은 Zipf({@code thetaGroups})로 정한다. 0이면 거의 균등, 0.99면 큰 묶음 몇 개가 key의
     * 상당 부분을 차지한다(테넌트·사용자별 데이터 크기 편차).
     */
    static Tagged taggedKeys(int groups, int count, double thetaGroups, long seed) {
        int[] sizes = zipfRequests(groups, count, thetaGroups, seed);
        Bytes[] keys = new Bytes[count];
        int[] groupOf = new int[count];
        int k = 0;
        for (int g = 0; g < groups; g++) {
            String tag = "{g" + g + "}:";
            for (int i = 0; i < sizes[g]; i++) {
                keys[k] = Bytes.utf8(tag + i);
                groupOf[k] = g;
                k++;
            }
        }
        return new Tagged(keys, groupOf, groups);
    }

    /**
     * 순차 key {@code count}개. 앞 8바이트는 big-endian 번호(seed로 정한 시작값부터 1씩 증가), 뒤 8바이트는 0~63 범위의 난수(중괄호가 안
     * 들어가므로 hash tag로 오인되지 않는다). 시간·자동 증가 ID처럼 "들어오는 순서대로 커지는" key다. 배열 순서가 곧 key 순서다.
     */
    static Bytes[] sequentialKeys(int count, long seed) {
        SplittableRandom rnd = new SplittableRandom(seed);
        long from = rnd.nextLong() >>> 2; // 끝까지 더해도 넘치지 않게
        Bytes[] keys = new Bytes[count];
        for (int i = 0; i < count; i++) {
            byte[] k = new byte[16];
            long v = from + i;
            for (int b = 7; b >= 0; b--) {
                k[b] = (byte) v;
                v >>>= 8;
            }
            for (int b = 8; b < 16; b++) {
                k[b] = (byte) rnd.nextInt(64);
            }
            keys[i] = Bytes.copyOf(k);
        }
        return keys;
    }

    /** variant의 {@link DistributionScheme#keyHash}로 구한 해시값. tag를 보는 variant와 key 전체를 보는 variant가 여기서 갈린다. */
    static long[] hashesOf(DistributionScheme scheme, Bytes[] keys) {
        long[] hashes = new long[keys.length];
        for (int i = 0; i < keys.length; i++) {
            hashes[i] = scheme.keyHash(keys[i]);
        }
        return hashes;
    }
}
