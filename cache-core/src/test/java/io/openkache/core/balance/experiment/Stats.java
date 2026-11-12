package io.openkache.core.balance.experiment;

import java.util.Arrays;
import java.util.Locale;

/** Benchmark 집계: 평균·최소·최대·표본 표준편차와 seed별 짝 개선율의 평균·95% 신뢰구간(t 분포). NaN은 "정의되지 않음"이라 뺀다. */
final class Stats {

    /** 자유도 1-30의 양측 95% t 값. 그보다 크면 1.96을 쓴다. */
    private static final double[] T95 = {
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
        2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
        2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042
    };

    private Stats() {}

    record Summary(int n, double mean, double min, double max, double std) {
        String format() {
            if (n == 0) {
                return "-";
            }
            return String.format(Locale.ROOT, "%.3f (%.3f-%.3f, sd %.3f)", mean, min, max, std);
        }
    }

    /** 개선율(%) 요약. {@code n}은 짝이 맞는 seed 수. */
    record Improvement(int n, double mean, double ciLow, double ciHigh) {
        String format() {
            if (n == 0) {
                return "NA";
            }
            if (n == 1) {
                return String.format(Locale.ROOT, "%+.1f%% (n=1)", mean);
            }
            return String.format(Locale.ROOT, "%+.1f%% [%+.1f, %+.1f]", mean, ciLow, ciHigh);
        }
    }

    static Summary summarize(double[] values) {
        double[] v = Arrays.stream(values).filter(x -> !Double.isNaN(x)).toArray();
        if (v.length == 0) {
            return new Summary(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }
        double mean = Arrays.stream(v).average().orElseThrow();
        double var = v.length < 2 ? 0 : Arrays.stream(v).map(x -> (x - mean) * (x - mean)).sum() / (v.length - 1);
        return new Summary(v.length, mean, Arrays.stream(v).min().orElseThrow(), Arrays.stream(v).max().orElseThrow(), Math.sqrt(var));
    }

    /**
     * seed별로 짝지은 개선율. 낮을수록 좋은 지표는 (Baseline − 후보) / Baseline × 100, 높을수록 좋은 지표는 (후보 − Baseline) /
     * Baseline × 100. Baseline이 0이거나 둘 중 하나가 정의되지 않은 seed는 뺀다.
     */
    static Improvement improvement(double[] baseline, double[] candidate, boolean lowerIsBetter) {
        double[] pct = new double[baseline.length];
        int n = 0;
        for (int i = 0; i < baseline.length; i++) {
            double b = baseline[i];
            double c = candidate[i];
            if (Double.isNaN(b) || Double.isNaN(c) || b == 0) {
                continue;
            }
            pct[n++] = (lowerIsBetter ? b - c : c - b) / Math.abs(b) * 100;
        }
        if (n == 0) {
            return new Improvement(0, Double.NaN, Double.NaN, Double.NaN);
        }
        Summary s = summarize(Arrays.copyOf(pct, n));
        double half = n < 2 ? 0 : t95(n - 1) * s.std() / Math.sqrt(n);
        return new Improvement(n, s.mean(), s.mean() - half, s.mean() + half);
    }

    private static double t95(int df) {
        return df <= T95.length ? T95[df - 1] : 1.96;
    }
}
