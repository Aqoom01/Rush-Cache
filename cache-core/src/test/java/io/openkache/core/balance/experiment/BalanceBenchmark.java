package io.openkache.core.balance.experiment;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.balance.experiment.Scenarios.Scenario;
import io.openkache.core.balance.experiment.Workloads.Pattern;
import io.openkache.core.balance.experiment.Workloads.Spec;
import io.openkache.core.balance.placement.CostAwareRebalancing;
import io.openkache.core.balance.placement.EStoreInspiredPlacement;
import io.openkache.core.balance.placement.GreedyLoadPlacement;
import io.openkache.core.balance.placement.HysteresisRebalancing;
import io.openkache.core.balance.placement.PlacementDecision;
import io.openkache.core.balance.placement.PlacementInput;
import io.openkache.core.balance.placement.PlacementPolicy;
import io.openkache.core.balance.placement.PowerOfTwoChoicesPlacement;
import io.openkache.core.balance.placement.SlicerInspiredPlacement;
import io.openkache.core.balance.placement.WeightedRendezvousPlacement;
import io.openkache.core.balance.split.EStoreSplitPolicy;
import io.openkache.core.balance.split.HeatSplitPolicy;
import io.openkache.core.balance.split.MidpointSplitPolicy;
import io.openkache.core.balance.split.SplitDecision;
import io.openkache.core.balance.split.SplitPolicy;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.function.IntFunction;

/**
 * B2 정책 벤치마크 A(분할)·B(배치)·C(분할 + 배치). 서버·네트워크·실제 데이터 이동 없이 in-process로 돌고, 결과는 계획 기반 계산값이다
 * (QPS·IOPS가 아니다). 설계는 {@code experiments/b2-load-balancing/design.md} §9.
 *
 * <pre>
 * ./gradlew :cache-core:testClasses
 * java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
 *      io.openkache.core.balance.experiment.BalanceBenchmark experiments/b2-load-balancing [A|B|C ...]
 * </pre>
 */
public final class BalanceBenchmark {

    static final int SEEDS = 10;
    static final double THETA = 0.8;

    // Benchmark B·C 공통
    static final int KEYS = 20_000;
    static final int REQUESTS = 40_000;
    static final int WINDOWS = 20;
    static final double TARGET_UTILIZATION = 0.7;
    static final double NODE_CAPACITY = REQUESTS / (4 * TARGET_UTILIZATION);
    static final long VALUE_BYTES = 1024;
    static final double SINGLE_HOT_SHARE_B = 0.3;
    static final double IMBALANCED_RATIO = 1.2;
    static final int EVENT_WINDOW = 10;

    // Benchmark C 분할 시작 조건(design.md §4.1과 같은 구조, 값은 이 실험의 노드 용량에 맞춤)
    static final double SPLIT_THRESHOLD = NODE_CAPACITY / 4;
    static final int SPLIT_SUSTAIN = 3;
    static final int SPLIT_COOLDOWN = 3;
    static final int MAX_TABLETS = 256;
    /** 결정 시간은 JIT 영향을 줄이려고 앞 window를 버린다. */
    static final int TIMING_SKIP_WINDOWS = 3;

    private BalanceBenchmark() {}

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("usage: BalanceBenchmark <experiments/b2-load-balancing> [A|B|C ...]");
            System.exit(2);
        }
        Path root = Path.of(args[0]);
        List<String> which = args.length > 1 ? List.of(args).subList(1, args.length) : List.of("A", "B", "C");
        if (which.contains("A")) {
            benchmarkA().write(root.resolve("split-comparison"), "benchmark-a");
        }
        if (which.contains("B")) {
            benchmarkB().write(root.resolve("placement-comparison"), "benchmark-b");
        }
        if (which.contains("C")) {
            benchmarkC().write(root.resolve("end-to-end"), "benchmark-c");
        }
    }

    // ---------------------------------------------------------------- Benchmark A

    static final List<String> A_SCENARIOS =
            List.of("S1_UNIFORM", "S2_HOT_RANGE", "S3_SINGLE_HOT_KEY", "ZIPF", "MOVING_HOTSPOT", "BURSTY");

    static Results benchmarkA() {
        List<SplitPolicy> policies = List.of(new MidpointSplitPolicy(), new HeatSplitPolicy(THETA), new EStoreSplitPolicy());
        Results results = new Results("A", "midpoint-split", List.of(
                new Metric("split", "분할", false, false),
                new Metric("hold", "보류", false, false),
                new Metric("ineffective_split", "효과 없는 분할(다음 window 큰 쪽 > θ)", true, false),
                new Metric("next_larger_share", "다음 window 큰 쪽 비율(분할한 경우만)", true, true),
                new Metric("decided_larger_share", "결정 window 큰 쪽 비율(분할한 경우만)", true, false),
                new Metric("decision_micros", "결정 시간 μs(참고)", true, false)));
        for (String scenario : A_SCENARIOS) {
            for (int seed = 1; seed <= SEEDS; seed++) {
                TabletLoad[] windows = splitWindows(scenario, seed);
                for (SplitPolicy policy : policies) {
                    SplitDecision decision = policy.decide(windows[0]);
                    String v = policy.variant();
                    results.put(scenario, v, seed, "decision_micros", medianDecisionMicros(policy, windows[0]));
                    if (decision instanceof SplitDecision.Split split) {
                        double next = SplitComparison.largerShare(windows[1], split.at());
                        results.put(scenario, v, seed, "split", 1);
                        results.put(scenario, v, seed, "hold", 0);
                        results.put(scenario, v, seed, "decided_larger_share", SplitComparison.largerShare(windows[0], split.at()));
                        results.put(scenario, v, seed, "next_larger_share", next);
                        results.put(scenario, v, seed, "ineffective_split", next > THETA ? 1 : 0);
                    } else {
                        SplitDecision.NoSplit hold = (SplitDecision.NoSplit) decision;
                        results.put(scenario, v, seed, "split", 0);
                        results.put(scenario, v, seed, "hold", 1);
                        results.put(scenario, v, seed, "hold_" + hold.reason(), 1);
                        results.put(scenario, v, seed, "decided_larger_share", Double.NaN);
                        results.put(scenario, v, seed, "next_larger_share", Double.NaN);
                        results.put(scenario, v, seed, "ineffective_split", Double.NaN);
                    }
                }
            }
        }
        return results;
    }

    /**
     * 결정 window와 측정 window의 Tablet 하나(해시 공간 전체). S1-S3는 {@link SplitComparison#runOnce}와 같은 입력을 만든다(회귀 비교).
     * BURSTY는 burst window(4)로 결정하고 다음 window(5)로 잰다.
     */
    static TabletLoad[] splitWindows(String scenario, long seed) {
        long[] hashes = Scenarios.keyHashes(SplitComparison.KEYS, seed);
        if (scenario.startsWith("S")) {
            Scenario s = Scenario.valueOf(scenario);
            Random requests = new Random(seed * 1_000_003L + s.ordinal());
            return new TabletLoad[] {
                Scenarios.load(hashes, Scenarios.requests(s, SplitComparison.KEYS, SplitComparison.REQUESTS, requests), 0),
                Scenarios.load(hashes, Scenarios.requests(s, SplitComparison.KEYS, SplitComparison.REQUESTS, requests), 1)
            };
        }
        Spec spec = new Spec(Pattern.valueOf(scenario), SplitComparison.KEYS, SplitComparison.REQUESTS, 0.9);
        int decide = scenario.equals("BURSTY") ? 4 : 0;
        return new TabletLoad[] {
            Scenarios.load(hashes, Workloads.requests(spec, seed, decide), decide),
            Scenarios.load(hashes, Workloads.requests(spec, seed, decide + 1), decide + 1)
        };
    }

    private static double medianDecisionMicros(SplitPolicy policy, TabletLoad load) {
        for (int i = 0; i < 20; i++) {
            policy.decide(load);
        }
        long[] nanos = new long[31];
        for (int i = 0; i < nanos.length; i++) {
            long start = System.nanoTime();
            policy.decide(load);
            nanos[i] = System.nanoTime() - start;
        }
        Arrays.sort(nanos);
        return nanos[nanos.length / 2] / 1_000.0;
    }

    // ---------------------------------------------------------------- Benchmark B·C 공통 시뮬레이션

    /** 시나리오: 부하 모양과 window별 노드 용량. */
    record ScenarioBC(String name, Spec spec, IntFunction<Map<NodeId, Double>> capacityAt) {}

    static List<ScenarioBC> bcScenarios() {
        Map<NodeId, Double> four = capacity(1, 2, 3, 4);
        Map<NodeId, Double> five = capacity(1, 2, 3, 4, 5);
        Map<NodeId, Double> hetero = new LinkedHashMap<>();
        double unit = NODE_CAPACITY * 4 / 8; // 용량 합은 같게, 1:1:2:4
        hetero.put(new NodeId(1), unit);
        hetero.put(new NodeId(2), unit);
        hetero.put(new NodeId(3), unit * 2);
        hetero.put(new NodeId(4), unit * 4);
        return List.of(
                new ScenarioBC("UNIFORM", spec(Pattern.UNIFORM), w -> four),
                new ScenarioBC("HOT_RANGE", spec(Pattern.HOT_RANGE), w -> four),
                new ScenarioBC("SINGLE_HOT_KEY", spec(Pattern.SINGLE_HOT_KEY), w -> four),
                new ScenarioBC("ZIPF", spec(Pattern.ZIPF), w -> four),
                new ScenarioBC("MOVING_HOTSPOT", spec(Pattern.MOVING_HOTSPOT), w -> four),
                new ScenarioBC("BURSTY", spec(Pattern.BURSTY), w -> four),
                new ScenarioBC("HETEROGENEOUS", spec(Pattern.ZIPF), w -> hetero),
                new ScenarioBC("SCALE_OUT", spec(Pattern.ZIPF), w -> w < EVENT_WINDOW ? four : five),
                new ScenarioBC("SCALE_IN", spec(Pattern.ZIPF), w -> w < EVENT_WINDOW ? five : four));
    }

    private static Spec spec(Pattern pattern) {
        return new Spec(pattern, KEYS, REQUESTS, SINGLE_HOT_SHARE_B);
    }

    private static Map<NodeId, Double> capacity(long... ids) {
        Map<NodeId, Double> map = new LinkedHashMap<>();
        for (long id : ids) {
            map.put(new NodeId(id), NODE_CAPACITY);
        }
        return map;
    }

    /** 한 실행의 결과. 이용률 지표는 window 1..WINDOWS(결정 다음 window)의 값이다. */
    record RunMetrics(
            double meanMaxUtilization,
            double meanImbalance,
            double maxImbalance,
            int overloadedNodeWindows,
            int imbalancedWindows,
            int movedTablets,
            double movedMegabytes,
            double decisionMicros,
            int splits,
            int ineffectiveSplits,
            int finalTablets) {}

    /** key 해시와 window별 key 요청 수. 같은 (시나리오, seed)의 모든 정책이 같은 입력을 쓴다. */
    record Trace(long[] hashes, long[][] counts) {}

    static Trace trace(ScenarioBC scenario, long seed) {
        long[] hashes = Scenarios.keyHashes(KEYS, seed);
        long[][] counts = new long[WINDOWS + 1][];
        for (int w = 0; w <= WINDOWS; w++) {
            counts[w] = Workloads.requests(scenario.spec(), seed, w);
        }
        return new Trace(hashes, counts);
    }

    /**
     * window 0..WINDOWS-1마다 (분할 →) 배치를 결정·적용하고 다음 window의 부하로 잰다.
     *
     * @param split null이면 분할하지 않는다(Benchmark B)
     */
    static RunMetrics simulate(ScenarioBC scenario, Trace trace, SplitPolicy split, PlacementPolicy placement, int tabletsPerNode) {
        List<NodeId> initialNodes = List.copyOf(scenario.capacityAt().apply(0).keySet());
        RoutingTable table = new HashRangeScheme(tabletsPerNode).initial(initialNodes);
        Map<TabletId, Long> lastMoved = new HashMap<>();
        Map<TabletId, Integer> over = new HashMap<>();
        Map<TabletId, Integer> cooldownUntil = new HashMap<>();
        List<TabletId[]> pendingSplits = new ArrayList<>();
        List<Double> decisionMicros = new ArrayList<>();
        double sumMax = 0;
        double sumImbalance = 0;
        double maxImbalance = 0;
        int overloaded = 0;
        int imbalanced = 0;
        int moved = 0;
        long movedBytes = 0;
        int splits = 0;
        int ineffective = 0;

        for (int w = 0; w < WINDOWS; w++) {
            // 결정 시간은 정책의 decide 호출만 잰다(입력 구성은 넣지 않는다).
            long decideNanos = 0;
            if (split != null) {
                Map<TabletId, Long> load = tabletLoads(table, trace.hashes(), trace.counts()[w]);
                for (Tablet tablet : List.copyOf(table.tablets())) {
                    long l = load.get(tablet.id());
                    over.merge(tablet.id(), l > SPLIT_THRESHOLD ? 1 : 0, (a, b) -> b == 0 ? 0 : a + 1);
                    boolean eligible = over.get(tablet.id()) >= SPLIT_SUSTAIN
                            && w >= cooldownUntil.getOrDefault(tablet.id(), 0)
                            && table.size() < MAX_TABLETS;
                    if (!eligible) {
                        continue;
                    }
                    TabletLoad tabletLoad = keyLoads(tablet, trace.hashes(), trace.counts()[w], w);
                    long t0 = System.nanoTime();
                    SplitDecision splitDecision = split.decide(tabletLoad);
                    decideNanos += System.nanoTime() - t0;
                    if (splitDecision instanceof SplitDecision.Split s) {
                        TabletId right = new TabletId(table.nextTabletId());
                        table = table.withSplit(tablet.id(), s.at());
                        splits++;
                        pendingSplits.add(new TabletId[] {tablet.id(), right});
                        for (TabletId id : new TabletId[] {tablet.id(), right}) {
                            cooldownUntil.put(id, w + SPLIT_COOLDOWN);
                            over.put(id, 0);
                        }
                    }
                }
            }
            Map<NodeId, Double> capacity = scenario.capacityAt().apply(w);
            PlacementInput input = new PlacementInput(
                    table, tabletLoads(table, trace.hashes(), trace.counts()[w]), tabletBytes(table, trace.hashes()),
                    capacity, w, lastMoved);
            long t0 = System.nanoTime();
            PlacementDecision decision = placement.decide(input);
            decideNanos += System.nanoTime() - t0;
            if (w >= TIMING_SKIP_WINDOWS) {
                decisionMicros.add(decideNanos / 1_000.0);
            }
            for (Change change : decision.moves()) {
                movedBytes += input.bytes(change.unit());
                lastMoved.put(change.unit(), (long) w);
            }
            moved += decision.moves().size();
            table = decision.toPlan(table).applyAll();

            // 다음 window의 부하로 잰다.
            Map<TabletId, Long> next = tabletLoads(table, trace.hashes(), trace.counts()[w + 1]);
            Map<NodeId, Double> nextCapacity = scenario.capacityAt().apply(w + 1);
            double[] util = utilization(table, next, nextCapacity);
            double total = next.values().stream().mapToLong(Long::longValue).sum();
            double average = total / nextCapacity.values().stream().mapToDouble(Double::doubleValue).sum();
            double max = Arrays.stream(util).max().orElse(0);
            double ratio = average == 0 ? 1 : max / average;
            sumMax += max;
            sumImbalance += ratio;
            maxImbalance = Math.max(maxImbalance, ratio);
            overloaded += (int) Arrays.stream(util).filter(u -> u > 1.0).count();
            imbalanced += ratio > IMBALANCED_RATIO ? 1 : 0;
            for (TabletId[] pair : pendingSplits) {
                if (table.contains(pair[0]) && table.contains(pair[1])) {
                    long a = next.get(pair[0]);
                    long b = next.get(pair[1]);
                    if (a + b > 0 && Math.max(a, b) > THETA * (a + b)) {
                        ineffective++;
                    }
                }
            }
            pendingSplits.clear();
        }
        double[] micros = decisionMicros.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        return new RunMetrics(
                sumMax / WINDOWS, sumImbalance / WINDOWS, maxImbalance, overloaded, imbalanced, moved,
                movedBytes / 1_000_000.0, micros.length == 0 ? Double.NaN : micros[micros.length / 2],
                splits, ineffective, table.size());
    }

    /** 허용 노드별 이용률. 허용되지 않은 노드에 남은 Tablet의 부하는 넣지 않는다(모든 정책이 비우므로 생기지 않는다). */
    private static double[] utilization(RoutingTable table, Map<TabletId, Long> load, Map<NodeId, Double> capacity) {
        Map<NodeId, Long> byNode = new HashMap<>();
        for (Tablet t : table.tablets()) {
            byNode.merge(t.owner(), load.get(t.id()), Long::sum);
        }
        return capacity.entrySet().stream().mapToDouble(e -> byNode.getOrDefault(e.getKey(), 0L) / e.getValue()).toArray();
    }

    static Map<TabletId, Long> tabletLoads(RoutingTable table, long[] hashes, long[] counts) {
        Map<TabletId, Long> load = new HashMap<>();
        table.tablets().forEach(t -> load.put(t.id(), 0L));
        for (int i = 0; i < hashes.length; i++) {
            if (counts[i] != 0) {
                load.merge(table.lookup(hashes[i]).id(), counts[i], Long::sum);
            }
        }
        return load;
    }

    private static Map<TabletId, Long> tabletBytes(RoutingTable table, long[] hashes) {
        Map<TabletId, Long> bytes = new HashMap<>();
        table.tablets().forEach(t -> bytes.put(t.id(), 0L));
        for (long hash : hashes) {
            bytes.merge(table.lookup(hash).id(), VALUE_BYTES, Long::sum);
        }
        return bytes;
    }

    private static TabletLoad keyLoads(Tablet tablet, long[] hashes, long[] counts, int window) {
        List<KeyLoad> keys = new ArrayList<>();
        for (int i = 0; i < hashes.length; i++) {
            if (tablet.range().contains(hashes[i])) {
                keys.add(new KeyLoad(hashes[i], counts[i], 0));
            }
        }
        return new TabletLoad(tablet.range(), window, keys);
    }

    // ---------------------------------------------------------------- Benchmark B

    /** 참고용: 빠지는 노드의 Tablet만 옮기고 재배치하지 않는다. Baseline이 아니다. */
    static final class NoRebalance implements PlacementPolicy {
        @Override
        public String variant() {
            return "no-rebalance(참고)";
        }

        @Override
        public PlacementDecision decide(PlacementInput input) {
            Map<NodeId, Long> load = new HashMap<>();
            input.nodes().forEach(n -> load.put(n, 0L));
            List<Tablet> orphans = new ArrayList<>();
            for (Tablet t : input.tablets()) {
                if (load.containsKey(t.owner())) {
                    load.merge(t.owner(), input.load(t.id()), Long::sum);
                } else {
                    orphans.add(t);
                }
            }
            List<Change> moves = new ArrayList<>();
            for (Tablet t : orphans) {
                NodeId best = input.nodes().stream()
                        .min((a, b) -> Double.compare(
                                (load.get(a) + input.load(t.id())) / input.nodeCapacity().get(a),
                                (load.get(b) + input.load(t.id())) / input.nodeCapacity().get(b)))
                        .orElseThrow();
                load.merge(best, input.load(t.id()), Long::sum);
                moves.add(new Change(t.id(), t.owner(), best));
            }
            return new PlacementDecision(moves);
        }
    }

    static final List<Metric> BC_METRICS = List.of(
            new Metric("mean_max_utilization", "평균 최대 이용률", true, true),
            new Metric("mean_imbalance", "평균 최대/평균", true, true),
            new Metric("max_imbalance", "최악 최대/평균", true, true),
            new Metric("overloaded_node_windows", "과부하 노드·window(이용률>1)", true, true),
            new Metric("imbalanced_windows", "불균형 window(>1.2)", true, true),
            new Metric("moved_tablets", "이동 Tablet", true, true),
            new Metric("moved_megabytes", "이동 MB", true, true),
            new Metric("decision_micros", "결정 시간 μs(참고)", true, false));

    static Results benchmarkB() {
        List<PlacementPolicy> policies = List.of(
                new GreedyLoadPlacement(), new SlicerInspiredPlacement(), new EStoreInspiredPlacement(),
                new PowerOfTwoChoicesPlacement(), new WeightedRendezvousPlacement(), new CostAwareRebalancing(),
                new HysteresisRebalancing(), new NoRebalance());
        Results results = new Results("B", "greedy-load-placement", BC_METRICS);
        for (ScenarioBC scenario : bcScenarios()) {
            for (int seed = 1; seed <= SEEDS; seed++) {
                Trace trace = trace(scenario, seed);
                for (PlacementPolicy policy : policies) {
                    record(results, scenario.name(), policy.variant(), seed, simulate(scenario, trace, null, policy, 16));
                }
            }
        }
        return results;
    }

    // ---------------------------------------------------------------- Benchmark C

    record Combo(String name, SplitPolicy split, PlacementPolicy placement) {}

    static Results benchmarkC() {
        List<Combo> combos = List.of(
                new Combo("midpoint-split + greedy-load-placement", new MidpointSplitPolicy(), new GreedyLoadPlacement()),
                new Combo("split-for-heat + greedy-load-placement", new HeatSplitPolicy(THETA), new GreedyLoadPlacement()),
                new Combo("midpoint-split + slicer-inspired", new MidpointSplitPolicy(), new SlicerInspiredPlacement()),
                new Combo("split-for-heat + slicer-inspired", new HeatSplitPolicy(THETA), new SlicerInspiredPlacement()),
                new Combo("estore-inspired + estore-inspired", new EStoreSplitPolicy(), new EStoreInspiredPlacement()),
                new Combo("split-for-heat + cost-aware-rebalancing", new HeatSplitPolicy(THETA), new CostAwareRebalancing()),
                new Combo("split-for-heat + hysteresis-rebalancing", new HeatSplitPolicy(THETA), new HysteresisRebalancing()));
        List<Metric> metrics = new ArrayList<>(BC_METRICS);
        metrics.add(new Metric("splits", "분할 수", true, false));
        metrics.add(new Metric("ineffective_splits", "효과 없는 분할", true, false));
        metrics.add(new Metric("final_tablets", "최종 Tablet 수", true, false));
        Results results = new Results("C", combos.getFirst().name(), metrics);
        for (ScenarioBC scenario : bcScenarios()) {
            for (int seed = 1; seed <= SEEDS; seed++) {
                Trace trace = trace(scenario, seed);
                for (Combo combo : combos) {
                    RunMetrics m = simulate(scenario, trace, combo.split(), combo.placement(), 2);
                    record(results, scenario.name(), combo.name(), seed, m);
                    results.put(scenario.name(), combo.name(), seed, "splits", m.splits());
                    results.put(scenario.name(), combo.name(), seed, "ineffective_splits", m.ineffectiveSplits());
                    results.put(scenario.name(), combo.name(), seed, "final_tablets", m.finalTablets());
                }
            }
        }
        return results;
    }

    private static void record(Results results, String scenario, String variant, int seed, RunMetrics m) {
        results.put(scenario, variant, seed, "mean_max_utilization", m.meanMaxUtilization());
        results.put(scenario, variant, seed, "mean_imbalance", m.meanImbalance());
        results.put(scenario, variant, seed, "max_imbalance", m.maxImbalance());
        results.put(scenario, variant, seed, "overloaded_node_windows", m.overloadedNodeWindows());
        results.put(scenario, variant, seed, "imbalanced_windows", m.imbalancedWindows());
        results.put(scenario, variant, seed, "moved_tablets", m.movedTablets());
        results.put(scenario, variant, seed, "moved_megabytes", m.movedMegabytes());
        results.put(scenario, variant, seed, "decision_micros", m.decisionMicros());
    }

    // ---------------------------------------------------------------- 결과 기록

    /**
     * @param improvement 이 지표로 Baseline 대비 개선율을 계산하는가
     */
    record Metric(String name, String label, boolean lowerIsBetter, boolean improvement) {}

    /** (시나리오, variant, 지표) → seed별 값. 입력 순서를 유지한다. */
    static final class Results {
        final String benchmark;
        final String baseline;
        final List<Metric> metrics;
        final Map<String, Map<String, Map<String, double[]>>> values = new LinkedHashMap<>();

        Results(String benchmark, String baseline, List<Metric> metrics) {
            this.benchmark = benchmark;
            this.baseline = baseline;
            this.metrics = metrics;
        }

        void put(String scenario, String variant, int seed, String metric, double value) {
            double[] row = values.computeIfAbsent(scenario, s -> new LinkedHashMap<>())
                    .computeIfAbsent(variant, v -> new LinkedHashMap<>())
                    .computeIfAbsent(metric, m -> {
                        double[] empty = new double[SEEDS];
                        Arrays.fill(empty, Double.NaN);
                        return empty;
                    });
            row[seed - 1] = value;
        }

        double[] get(String scenario, String variant, String metric) {
            double[] row = values.get(scenario).get(variant).get(metric);
            if (row != null) {
                return row;
            }
            double[] empty = new double[SEEDS];
            Arrays.fill(empty, Double.NaN);
            return empty;
        }

        void write(Path dir, String name) throws IOException {
            Files.createDirectories(dir);
            try (PrintWriter csv = new PrintWriter(Files.newBufferedWriter(dir.resolve(name + ".csv")))) {
                csv.println("benchmark,scenario,variant,seed,metric,value");
                values.forEach((scenario, byVariant) -> byVariant.forEach((variant, byMetric) -> byMetric.forEach((metric, row) -> {
                    for (int i = 0; i < row.length; i++) {
                        csv.printf(Locale.ROOT, "%s,%s,\"%s\",%d,%s,%s%n", benchmark, scenario, variant, i + 1, metric, fmt(row[i]));
                    }
                })));
            }
            List<String> json = new ArrayList<>();
            List<String> md = new ArrayList<>();
            md.add("# Benchmark " + benchmark + " 요약 (자동 생성)");
            md.add("");
            md.add("Baseline: `" + baseline + "`. 값은 seed " + SEEDS + "개의 평균 (최소-최대, sd). 개선율은 seed별로 짝지은 값의 평균"
                    + " [95% 신뢰구간], 낮을수록 좋은 지표는 (Baseline − 후보) / Baseline × 100. NA는 정의되지 않음(Baseline 0, 보류 등).");
            for (String scenario : values.keySet()) {
                md.add("");
                md.add("## " + scenario);
                md.add("");
                StringBuilder header = new StringBuilder("| variant |");
                StringBuilder line = new StringBuilder("|---|");
                for (Metric m : metrics) {
                    header.append(' ').append(m.label()).append(" |");
                    line.append("---|");
                }
                md.add(header.toString());
                md.add(line.toString());
                for (String variant : values.get(scenario).keySet()) {
                    StringBuilder row = new StringBuilder("| `" + variant + "` |");
                    for (Metric m : metrics) {
                        Stats.Summary s = Stats.summarize(get(scenario, variant, m.name()));
                        String cell = isCount(m) ? countCell(get(scenario, variant, m.name())) : s.format();
                        if (m.improvement() && !variant.equals(baseline)) {
                            Stats.Improvement imp = Stats.improvement(
                                    get(scenario, baseline, m.name()), get(scenario, variant, m.name()), m.lowerIsBetter());
                            cell += "<br>" + imp.format();
                            json.add(jsonRow(scenario, variant, m, s, imp));
                        } else {
                            json.add(jsonRow(scenario, variant, m, s, null));
                        }
                        row.append(' ').append(cell).append(" |");
                    }
                    md.add(row.toString());
                }
                List<String> holds = holdReasons(scenario);
                if (!holds.isEmpty()) {
                    md.add("");
                    md.addAll(holds);
                }
            }
            Files.write(dir.resolve(name + ".json"), List.of("[", String.join(",\n", json), "]"));
            Files.write(dir.resolve(name + ".md"), md);
            System.err.println("wrote " + dir.resolve(name) + ".{csv,json,md}");
        }

        private boolean isCount(Metric m) {
            return benchmark.equals("A") && (m.name().equals("split") || m.name().equals("hold") || m.name().equals("ineffective_split"));
        }

        /** 0/1 지표는 "합/정의된 seed 수"로 쓴다. */
        private static String countCell(double[] row) {
            long defined = Arrays.stream(row).filter(x -> !Double.isNaN(x)).count();
            long sum = (long) Arrays.stream(row).filter(x -> !Double.isNaN(x)).sum();
            return defined == 0 ? "-" : sum + "/" + defined;
        }

        private List<String> holdReasons(String scenario) {
            List<String> lines = new ArrayList<>();
            values.get(scenario).forEach((variant, byMetric) -> byMetric.forEach((metric, row) -> {
                if (metric.startsWith("hold_")) {
                    long n = Arrays.stream(row).filter(x -> !Double.isNaN(x)).count();
                    lines.add("- `" + variant + "` 보류 사유 " + metric.substring(5) + ": " + n + "/" + SEEDS);
                }
            }));
            return lines;
        }

        private String jsonRow(String scenario, String variant, Metric m, Stats.Summary s, Stats.Improvement imp) {
            return String.format(Locale.ROOT,
                    "  {\"benchmark\": \"%s\", \"scenario\": \"%s\", \"variant\": \"%s\", \"metric\": \"%s\", \"n\": %d,"
                            + " \"mean\": %s, \"min\": %s, \"max\": %s, \"std\": %s, \"baseline\": \"%s\","
                            + " \"improvement_pct\": %s, \"improvement_ci95\": %s, \"paired_n\": %s}",
                    benchmark, scenario, variant, m.name(), s.n(), num(s.mean()), num(s.min()), num(s.max()), num(s.std()),
                    baseline, imp == null ? "null" : num(imp.mean()),
                    imp == null || imp.n() < 2 ? "null" : "[" + num(imp.ciLow()) + ", " + num(imp.ciHigh()) + "]",
                    imp == null ? "null" : String.valueOf(imp.n()));
        }

        private static String num(double v) {
            return Double.isNaN(v) ? "null" : String.format(Locale.ROOT, "%.6f", v);
        }

        private static String fmt(double v) {
            return Double.isNaN(v) ? "NA" : String.format(Locale.ROOT, "%.6f", v);
        }
    }
}
