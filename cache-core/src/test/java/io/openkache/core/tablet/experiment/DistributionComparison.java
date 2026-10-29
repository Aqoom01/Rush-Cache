package io.openkache.core.tablet.experiment;

import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 분배 방식 비교 실험 러너. 서버·네트워크 없이 in-process로 돌고 결과를 long 형식 CSV로 쓴다.
 *
 * <p>JUnit 테스트가 아니라 {@code main}이다. {@code ./gradlew test} 때마다 결과 파일을 쓰면 안 되기 때문이다. 실행 방법과 시나리오
 * 정의는 {@code experiments/b1-tablet-distribution/distribution-comparison/README.md}, 설계는 {@code design.md} §6.
 *
 * <pre>
 * ./gradlew :cache-core:testClasses
 * java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
 *      io.openkache.core.tablet.experiment.DistributionComparison \
 *      experiments/b1-tablet-distribution/distribution-comparison/summary.csv
 * </pre>
 */
public final class DistributionComparison {

    static final int[] GRANULARITIES = {256, 1024, 4096, 16384};
    static final int[] KEY_COUNTS = {10_000, 100_000, 1_000_000};
    static final double[] THETAS = {0.9, 0.99, 1.2};
    static final int SEEDS = 5;
    static final int NODES = Variants.BASE_NODES;
    static final int REQUESTS = 1_000_000;
    static final int MAX_NODES_S4 = 16;
    /** S7 태그 묶음 수. 묶음당 평균 key 수는 100만 / 묶음 수. */
    static final int[] GROUP_COUNTS = {100, 1_000, 10_000, 100_000};
    /** S7 묶음 크기의 쏠림. 0이면 묶음 크기가 거의 같고, 0.99면 큰 묶음 몇 개가 key의 상당 부분을 차지한다. */
    static final double[] GROUP_THETAS = {0.0, 0.99};
    /** S7 요청 쏠림(key 기준). S5의 중간값. */
    static final double S7_REQUEST_THETA = 0.99;

    private final PrintWriter out;

    private DistributionComparison(PrintWriter out) {
        this.out = out;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("usage: DistributionComparison <summary.csv>");
            System.exit(2);
        }
        Path csv = Path.of(args[0]);
        Files.createDirectories(csv.toAbsolutePath().getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(csv))) {
            w.println("variant,scenario,G,nodes,keys,theta,groups,seed,metric,value");
            DistributionComparison run = new DistributionComparison(w);
            for (Variants.Variant v : Variants.ALL) {
                long t0 = System.nanoTime();
                run.s1InitialBalance(v);
                run.s2s3AddRemove(v);
                run.s4Churn(v);
                run.s5Skew(v);
                run.s7TagGroups(v);
                run.aux(v);
                System.err.printf(Locale.ROOT, "%s done in %.1fs%n", v.name(), (System.nanoTime() - t0) / 1e9);
            }
        }
        System.err.println("wrote " + csv);
    }

    // ---- S1: 초기 균형. 단위 수 균형(결정적)과 key 수 균형(통계적)을 따로 본다 ----

    void s1InitialBalance(Variants.Variant v) {
        for (int g : GRANULARITIES) {
            for (int keys : KEY_COUNTS) {
                for (int seed = 1; seed <= SEEDS; seed++) {
                    long[] hashes = Workload.keyHashes(keys, seed);
                    RoutingTable t = v.forGranularity().apply(g).initial(nodes(NODES));
                    Row row = new Row(v.name(), "S1", g, NODES, keys, Double.NaN, seed);
                    write(row, "unit_max_mean", Metrics.unitMaxMean(t, nodes(NODES)));
                    write(row, "key_max_mean", Metrics.keyMaxMean(t, hashes, nodes(NODES)));
                }
            }
        }
    }

    // ---- S2/S3: 노드 하나 추가·제거. 이동 비율이 이론 최소 1/(n+1), 1/n 에 붙는가 ----

    void s2s3AddRemove(Variants.Variant v) {
        int keys = 1_000_000;
        for (int g : GRANULARITIES) {
            for (int seed = 1; seed <= SEEDS; seed++) {
                long[] hashes = Workload.keyHashes(keys, seed);
                DistributionScheme s = v.forGranularity().apply(g);
                RoutingTable t0 = s.initial(nodes(NODES));

                NodeId added = new NodeId(NODES + 1);
                Plan addPlan = s.onNodeAdded(t0, added);
                RoutingTable t1 = addPlan.applyAll();
                Row add = new Row(v.name(), "S2", g, NODES, keys, Double.NaN, seed);
                write(add, "moved_key_ratio", Metrics.movedRatio(t0, t1, hashes));
                write(add, "moved_unit_ratio", (double) addPlan.moves().size() / t0.size());
                write(add, "ideal_ratio", 1.0 / (NODES + 1));
                write(add, "old_to_old_moves", countOldToOld(addPlan, added));
                write(add, "unit_max_mean_after", Metrics.unitMaxMean(t1, nodes(NODES + 1)));
                write(add, "key_max_mean_after", Metrics.keyMaxMean(t1, hashes, nodes(NODES + 1)));

                NodeId removed = new NodeId(NODES);
                Plan removePlan = s.onNodeRemoved(t0, removed);
                RoutingTable t2 = removePlan.applyAll();
                List<NodeId> remaining = new ArrayList<>(nodes(NODES));
                remaining.remove(removed);
                Row rem = new Row(v.name(), "S3", g, NODES, keys, Double.NaN, seed);
                write(rem, "moved_key_ratio", Metrics.movedRatio(t0, t2, hashes));
                write(rem, "moved_unit_ratio", (double) removePlan.moves().size() / t0.size());
                write(rem, "ideal_ratio", 1.0 / NODES);
                write(rem, "unit_max_mean_after", Metrics.unitMaxMean(t2, remaining));
                write(rem, "key_max_mean_after", Metrics.keyMaxMean(t2, hashes, remaining));
            }
        }
    }

    // ---- S4: 1 → 16 → 8 로 한 대씩 증감. 누적 이동이 이론 합과 같은가, 매 단계 균형이 유지되는가 ----

    void s4Churn(Variants.Variant v) {
        int g = 4096;
        int keys = 1_000_000;
        for (int seed = 1; seed <= SEEDS; seed++) {
            long[] hashes = Workload.keyHashes(keys, seed);
            DistributionScheme s = v.forGranularity().apply(g);
            List<NodeId> members = new ArrayList<>(nodes(1));
            RoutingTable t = s.initial(members);
            double cumulativeKeys = 0;
            double cumulativeUnits = 0;
            double ideal = 0;
            double worstImbalance = 1.0;
            int steps = 0;
            for (int n = 2; n <= MAX_NODES_S4; n++) {
                NodeId node = new NodeId(n);
                Plan p = s.onNodeAdded(t, node);
                RoutingTable next = p.applyAll();
                cumulativeKeys += Metrics.movedRatio(t, next, hashes);
                cumulativeUnits += (double) p.moves().size() / t.size();
                ideal += 1.0 / n;
                members.add(node);
                t = next;
                worstImbalance = Math.max(worstImbalance, Metrics.unitMaxMean(t, members));
                steps++;
            }
            for (int n = MAX_NODES_S4; n > NODES; n--) {
                NodeId node = new NodeId(n);
                Plan p = s.onNodeRemoved(t, node);
                RoutingTable next = p.applyAll();
                cumulativeKeys += Metrics.movedRatio(t, next, hashes);
                cumulativeUnits += (double) p.moves().size() / t.size();
                ideal += 1.0 / n;
                members.remove(node);
                t = next;
                worstImbalance = Math.max(worstImbalance, Metrics.unitMaxMean(t, members));
                steps++;
            }
            Row row = new Row(v.name(), "S4", g, NODES, keys, Double.NaN, seed);
            write(row, "steps", steps);
            write(row, "cumulative_moved_key_ratio", cumulativeKeys);
            write(row, "cumulative_moved_unit_ratio", cumulativeUnits);
            write(row, "ideal_cumulative_ratio", ideal);
            write(row, "worst_unit_max_mean", worstImbalance);
            write(row, "final_key_max_mean", Metrics.keyMaxMean(t, hashes, members));
        }
    }

    // ---- S5: 요청 쏠림. 그대로 뒀을 때와, 단위를 옮겨서 도달할 수 있는 한계 ----

    void s5Skew(Variants.Variant v) {
        int keys = 1_000_000;
        for (int g : GRANULARITIES) {
            for (double theta : THETAS) {
                for (int seed = 1; seed <= SEEDS; seed++) {
                    long[] hashes = Workload.keyHashes(keys, seed);
                    int[] requests = Workload.zipfRequests(keys, REQUESTS, theta, seed);
                    DistributionScheme s = v.forGranularity().apply(g);
                    RoutingTable t = s.initial(nodes(NODES));
                    long[] unitLoads = Metrics.unitLoads(t, hashes, requests);
                    Row row = new Row(v.name(), "S5", g, NODES, keys, theta, seed);
                    write(row, "request_max_mean", Metrics.requestMaxMean(t, hashes, requests, nodes(NODES)));
                    write(row, "greedy_bound", s.supportsMove() ? Metrics.greedyBound(unitLoads, NODES) : Double.NaN);
                    write(row, "hottest_unit_share", Metrics.hottestUnitShare(unitLoads));
                    write(row, "hottest_key_share", Metrics.hottestKeyShare(requests));
                }
            }
        }
    }

    // ---- S7: 태그 묶음. 같은 tag의 key를 한 노드에 모으는 대가로 균형과 쏠림 한계가 얼마나 나빠지는가 ----

    void s7TagGroups(Variants.Variant v) {
        int g = 4096;
        int keys = 1_000_000;
        for (int groups : GROUP_COUNTS) {
            for (double thetaG : GROUP_THETAS) {
                for (int seed = 1; seed <= SEEDS; seed++) {
                    Workload.Tagged w = Workload.taggedKeys(groups, keys, thetaG, seed);
                    int[] requests = Workload.zipfRequests(keys, REQUESTS, S7_REQUEST_THETA, seed);
                    DistributionScheme s = v.forGranularity().apply(g);
                    long[] hashes = Workload.hashesOf(s, w.keys());
                    RoutingTable t = s.initial(nodes(NODES));
                    long[] unitLoads = Metrics.unitLoads(t, hashes, requests);
                    Row row = new Row(v.name(), "S7", g, NODES, keys, thetaG, groups, seed);
                    write(row, "key_max_mean", Metrics.keyMaxMean(t, hashes, nodes(NODES)));
                    write(row, "largest_group_share", Metrics.largestGroupShare(w.groupOf(), groups));
                    write(row, "groups_split_ratio", Metrics.groupsSplitRatio(t, hashes, w.groupOf(), groups));
                    write(row, "same_node_pair_ratio", Metrics.sameNodePairRatio(t, hashes, w.groupOf(), groups));
                    write(row, "request_max_mean", Metrics.requestMaxMean(t, hashes, requests, nodes(NODES)));
                    write(row, "greedy_bound", s.supportsMove() ? Metrics.greedyBound(unitLoads, NODES) : Double.NaN);
                    write(row, "hottest_unit_share", Metrics.hottestUnitShare(unitLoads));
                    RoutingTable after = s.onNodeAdded(t, new NodeId(NODES + 1)).applyAll();
                    write(row, "moved_key_ratio_add", Metrics.movedRatio(t, after, hashes));
                }
            }
        }
    }

    // ---- 보조: 표 크기와 조회 비용 ----

    void aux(Variants.Variant v) {
        long[] hashes = Workload.keyHashes(1_000_000, 1);
        for (int g : GRANULARITIES) {
            DistributionScheme s = v.forGranularity().apply(g);
            RoutingTable t = s.initial(nodes(NODES));
            Row row = new Row(v.name(), "AUX", g, NODES, hashes.length, Double.NaN, 0);
            write(row, "metadata_bytes_initial", s.metadataBytes(t));
            RoutingTable after = s.onNodeAdded(t, new NodeId(NODES + 1)).applyAll();
            write(row, "metadata_bytes_after_add", s.metadataBytes(after));
            double best = Double.MAX_VALUE;
            for (int rep = 0; rep < 3; rep++) {
                best = Math.min(best, Metrics.lookupNanos(t, hashes));
            }
            write(row, "lookup_ns_min_of_3", best);
        }
    }

    private static double countOldToOld(Plan plan, NodeId added) {
        int c = 0;
        for (Change ch : plan.moves()) {
            if (!ch.to().equals(added)) {
                c++;
            }
        }
        return c;
    }

    static List<NodeId> nodes(int n) {
        List<NodeId> l = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            l.add(new NodeId(i));
        }
        return l;
    }

    /** 결과 한 줄의 조건. {@code groups}는 S7만 쓰고 나머지는 0({@code -}로 기록). */
    private record Row(String variant, String scenario, int g, int nodes, int keys, double theta, int groups, int seed) {
        Row(String variant, String scenario, int g, int nodes, int keys, double theta, int seed) {
            this(variant, scenario, g, nodes, keys, theta, 0, seed);
        }
    }

    private void write(Row r, String metric, double value) {
        out.printf(Locale.ROOT, "%s,%s,%d,%d,%d,%s,%s,%d,%s,%s%n", r.variant(), r.scenario(), r.g(), r.nodes(), r.keys(),
                Double.isNaN(r.theta()) ? "-" : Double.toString(r.theta()), r.groups() == 0 ? "-" : Integer.toString(r.groups()),
                r.seed(), metric, Double.isNaN(value) ? "NA" : String.format(Locale.ROOT, "%.6f", value));
    }
}
