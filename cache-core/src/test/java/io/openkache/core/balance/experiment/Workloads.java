package io.openkache.core.balance.experiment;

import java.util.Random;

/**
 * Benchmark A-C의 key 단위 부하. key는 해시값 오름차순 번호(0 = 해시값이 가장 작은 key)로 다루고, window마다 key별 요청 수를 만든다.
 *
 * <p>같은 (시나리오, seed, window)면 같은 요청 수를 낸다. 정책은 입력에만 영향을 받고 입력을 바꾸지 않는다. 기준값(hot 비율, 이동
 * 속도, burst 크기)은 프로젝트 자체 결정이며 design.md §9.4에 적는다.
 */
final class Workloads {

    private Workloads() {}

    enum Pattern {
        /** 모든 key에 고르게. */
        UNIFORM,
        /** 해시값이 작은 쪽 20% key에 요청 80%(S2와 같은 모양). */
        HOT_RANGE,
        /** 가운데 key 하나가 요청의 {@code singleHotShare}를 받고 나머지는 고르게. */
        SINGLE_HOT_KEY,
        /** Zipf(s = 1.0). 순위는 seed로 섞어 해시 순서와 무관하다. */
        ZIPF,
        /** 20% 구간에 80%. 구간 시작이 window마다 key 수의 5%씩 오른쪽으로 이동하고 끝에 닿으면 처음으로 돌아간다. */
        MOVING_HOTSPOT,
        /** 균등 기본 부하에 5 window마다 기본의 3배 요청이 임의의 5% 구간에 몰린다(window 4, 9, 14, ...). */
        BURSTY
    }

    /**
     * @param keys key 수
     * @param requests window당 기본 요청 수
     * @param singleHotShare {@link Pattern#SINGLE_HOT_KEY}에서 hot key의 몫
     */
    record Spec(Pattern pattern, int keys, int requests, double singleHotShare) {}

    /** window {@code window}의 key별 요청 수. 배열 위치 = key 번호(해시 오름차순). */
    static long[] requests(Spec spec, long seed, int window) {
        int n = spec.keys();
        long[] counts = new long[n];
        Random random = new Random(seed * 1_000_003L + spec.pattern().ordinal() * 7_919L + window * 104_729L);
        switch (spec.pattern()) {
            case UNIFORM -> spread(counts, random, spec.requests(), 0, n);
            case HOT_RANGE -> hotRange(counts, random, spec.requests(), 0);
            case MOVING_HOTSPOT -> hotRange(counts, random, spec.requests(), (int) ((long) window * n / 20 % n));
            case SINGLE_HOT_KEY -> {
                long hot = Math.round(spec.requests() * spec.singleHotShare());
                counts[n / 2] += hot;
                spread(counts, random, (int) (spec.requests() - hot), 0, n);
            }
            case ZIPF -> zipf(counts, random, spec.requests(), new Random(seed * 31 + 17));
            case BURSTY -> {
                spread(counts, random, spec.requests(), 0, n);
                if (window % 5 == 4) {
                    int width = Math.max(1, n / 20);
                    int start = random.nextInt(n - width + 1);
                    spread(counts, random, spec.requests() * 3, start, start + width);
                }
            }
        }
        return counts;
    }

    /** {@code [from, to)} 구간 key에 요청 {@code total}건을 균등하게 뿌린다. */
    private static void spread(long[] counts, Random random, int total, int from, int to) {
        for (int r = 0; r < total; r++) {
            counts[from + random.nextInt(to - from)]++;
        }
    }

    /** {@code start}부터 key 수의 20%(끝에서 처음으로 이어짐)에 80%, 나머지 key에 20%. */
    private static void hotRange(long[] counts, Random random, int total, int start) {
        int n = counts.length;
        int hot = Math.max(1, n / 5);
        for (int r = 0; r < total; r++) {
            if (random.nextDouble() < 0.8) {
                counts[(start + random.nextInt(hot)) % n]++;
            } else {
                counts[(start + hot + random.nextInt(n - hot)) % n]++;
            }
        }
    }

    /** Zipf(1.0). 순위 → key 번호 대응은 {@code ranks}(seed 고정)로 섞는다. */
    private static void zipf(long[] counts, Random random, int total, Random ranks) {
        int n = counts.length;
        double[] cdf = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            sum += 1.0 / (i + 1);
            cdf[i] = sum;
        }
        int[] keyOfRank = new int[n];
        for (int i = 0; i < n; i++) {
            keyOfRank[i] = i;
        }
        for (int i = n - 1; i > 0; i--) {
            int j = ranks.nextInt(i + 1);
            int t = keyOfRank[i];
            keyOfRank[i] = keyOfRank[j];
            keyOfRank[j] = t;
        }
        for (int r = 0; r < total; r++) {
            double u = random.nextDouble() * sum;
            int lo = 0;
            int hi = n - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cdf[mid] < u) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            counts[keyOfRank[lo]]++;
        }
    }
}
