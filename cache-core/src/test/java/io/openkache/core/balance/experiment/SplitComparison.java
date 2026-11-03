package io.openkache.core.balance.experiment;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.balance.experiment.Scenarios.Scenario;
import io.openkache.core.balance.split.HeatSplitPolicy;
import io.openkache.core.balance.split.MidpointSplitPolicy;
import io.openkache.core.balance.split.NoSplitReason;
import io.openkache.core.balance.split.SplitDecision;
import io.openkache.core.balance.split.SplitPolicy;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Split 정책 비교 실험 러너. 서버·네트워크 없이 in-process로 돌고 결과를 long 형식 CSV로 쓴다.
 *
 * <p>JUnit 테스트가 아니라 {@code main}이다. {@code ./gradlew test} 때마다 결과 파일을 쓰면 안 되기 때문이다. 실행 방법과 지표
 * 정의는 {@code experiments/b2-load-balancing/split-comparison/README.md}, 설계는 {@code design.md} §6.
 *
 * <pre>
 * ./gradlew :cache-core:testClasses
 * java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
 *      io.openkache.core.balance.experiment.SplitComparison \
 *      experiments/b2-load-balancing/split-comparison/summary.csv
 * </pre>
 */
public final class SplitComparison {

    static final int KEYS = 1000;
    /** window당 요청 수. 분할 시작 조건 T(1,000)의 4배라 공통 단계(SplitTrigger)는 항상 통과하므로 이 실험에서는 생략한다. */
    static final int REQUESTS = 4000;
    static final double[] THETAS = {0.6, 0.7, 0.8, 0.9};
    static final int SEEDS = 10;

    private SplitComparison() {}

    /**
     * 한 번의 실행 결과. 분할하지 않았으면 분할 관련 값은 {@code NaN}이다.
     *
     * @param decidedLargerShare 결정에 쓴 window(0)에서 큰 쪽 요청 비율
     * @param nextLargerShare 다음 window(1)에서 큰 쪽 요청 비율. 효과는 이 값으로 판단한다
     * @param hottestKeyShare 다음 window에서 가장 많이 받은 key 하나의 요청 비율
     */
    record Outcome(SplitDecision decision, double decidedLargerShare, double nextLargerShare, double hottestKeyShare) {

        boolean split() {
            return decision instanceof SplitDecision.Split;
        }

        boolean ineffective(double theta) {
            return split() && nextLargerShare > theta;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("usage: SplitComparison <summary.csv>");
            System.exit(2);
        }
        Path csv = Path.of(args[0]);
        Files.createDirectories(csv.toAbsolutePath().getParent());
        List<String> summary = new ArrayList<>();
        summary.add("| θ | variant | scenario | 분할 | SINGLE_HOT_KEY | 다음 window 큰 쪽 비율 평균 (최소-최대) | 효과 없는 분할 |");
        summary.add("|---|---|---|---|---|---|---|");
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(csv))) {
            out.println("variant,scenario,theta,seed,metric,value");
            for (double theta : THETAS) {
                for (SplitPolicy policy : List.of(new MidpointSplitPolicy(), new HeatSplitPolicy(theta))) {
                    for (Scenario scenario : Scenario.values()) {
                        List<Outcome> outcomes = new ArrayList<>();
                        for (int seed = 1; seed <= SEEDS; seed++) {
                            Outcome o = runOnce(policy, scenario, seed);
                            outcomes.add(o);
                            write(out, policy, scenario, theta, seed, o);
                        }
                        summary.add(summaryRow(policy, scenario, theta, outcomes));
                    }
                }
            }
        }
        summary.forEach(System.out::println);
        System.err.println("wrote " + csv);
    }

    /** 같은 seed면 같은 key와 같은 요청열을 쓴다. 정책과 θ는 결과에 영향을 주지만 입력에는 주지 않는다. */
    static Outcome runOnce(SplitPolicy policy, Scenario scenario, long seed) {
        long[] hashes = Scenarios.keyHashes(KEYS, seed);
        Random requests = new Random(seed * 1_000_003L + scenario.ordinal());
        TabletLoad decideOn = Scenarios.load(hashes, Scenarios.requests(scenario, KEYS, REQUESTS, requests), 0);
        TabletLoad next = Scenarios.load(hashes, Scenarios.requests(scenario, KEYS, REQUESTS, requests), 1);

        SplitDecision decision = policy.decide(decideOn);
        double hottest = hottestKeyShare(next);
        if (decision instanceof SplitDecision.Split split) {
            return new Outcome(decision, largerShare(decideOn, split.at()), largerShare(next, split.at()), hottest);
        }
        return new Outcome(decision, Double.NaN, Double.NaN, hottest);
    }

    /** {@code at}에서 나눴을 때 큰 쪽 요청 수 / 전체. */
    static double largerShare(TabletLoad load, long at) {
        long left = 0;
        for (KeyLoad key : load.keys()) {
            if (Long.compareUnsigned(key.keyHash(), at) < 0) {
                left += key.requests();
            }
        }
        long total = load.totalRequests();
        return (double) Math.max(left, total - left) / total;
    }

    static double hottestKeyShare(TabletLoad load) {
        long max = load.keys().stream().mapToLong(KeyLoad::requests).max().orElse(0);
        return (double) max / load.totalRequests();
    }

    private static void write(PrintWriter out, SplitPolicy policy, Scenario scenario, double theta, int seed, Outcome o) {
        String prefix = String.format(Locale.ROOT, "%s,%s,%.1f,%d,", policy.variant(), scenario, theta, seed);
        boolean singleHotKey = o.decision() instanceof SplitDecision.NoSplit n && n.reason() == NoSplitReason.SINGLE_HOT_KEY;
        out.println(prefix + "split," + (o.split() ? 1 : 0));
        out.println(prefix + "single_hot_key," + (singleHotKey ? 1 : 0));
        out.println(prefix + "decided_larger_share," + format(o.decidedLargerShare()));
        out.println(prefix + "next_larger_share," + format(o.nextLargerShare()));
        out.println(prefix + "ineffective_split," + (o.split() ? (o.ineffective(theta) ? "1" : "0") : "NA"));
        out.println(prefix + "hottest_key_share," + format(o.hottestKeyShare()));
    }

    private static String summaryRow(SplitPolicy policy, Scenario scenario, double theta, List<Outcome> outcomes) {
        long splits = outcomes.stream().filter(Outcome::split).count();
        long singleHotKey = outcomes.stream()
                .filter(o -> o.decision() instanceof SplitDecision.NoSplit n && n.reason() == NoSplitReason.SINGLE_HOT_KEY)
                .count();
        long ineffective = outcomes.stream().filter(o -> o.ineffective(theta)).count();
        double[] shares = outcomes.stream().filter(Outcome::split).mapToDouble(Outcome::nextLargerShare).toArray();
        String shareText = shares.length == 0
                ? "-"
                : String.format(Locale.ROOT, "%.3f (%.3f-%.3f)",
                        java.util.Arrays.stream(shares).average().orElseThrow(),
                        java.util.Arrays.stream(shares).min().orElseThrow(),
                        java.util.Arrays.stream(shares).max().orElseThrow());
        return String.format(Locale.ROOT, "| %.1f | %s | %s | %d/%d | %d/%d | %s | %d/%d |",
                theta, policy.variant(), scenario, splits, outcomes.size(), singleHotKey, outcomes.size(),
                shareText, ineffective, splits);
    }

    private static String format(double value) {
        return Double.isNaN(value) ? "NA" : String.format(Locale.ROOT, "%.4f", value);
    }
}
