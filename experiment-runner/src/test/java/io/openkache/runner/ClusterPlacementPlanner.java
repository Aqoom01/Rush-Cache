package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.balance.placement.CostAwareRebalancing;
import io.openkache.core.balance.placement.EStoreInspiredPlacement;
import io.openkache.core.balance.placement.GreedyLoadPlacement;
import io.openkache.core.balance.placement.HysteresisRebalancing;
import io.openkache.core.balance.placement.PlacementDecision;
import io.openkache.core.balance.placement.PlacementInput;
import io.openkache.core.balance.placement.PlacementPolicy;
import io.openkache.core.balance.placement.SlicerInspiredPlacement;
import io.openkache.core.balance.placement.WeightedRendezvousPlacement;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;

/**
 * B2 EC2 배치 실험(cluster-placement)의 계획 생성기. 공통 Runner 코드가 아니라 B2 실험용 도구다(test 소스에 둔다).
 *
 * <p>Runner가 window마다 보낼 요청열을 똑같이 재현해(thread {@code i}, window {@code w}의 seed = {@code seed + i + w ×
 * 1,000,003}) Tablet별 부하를 세고, window {@code w}의 부하로 배치 정책을 실행해 window {@code w + 1}의 주인을 정한다. 결과를
 * Runner의 {@code schedule.file}과 config로 쓴다. 실제 서버는 이 계획을 재생만 한다(오프라인 계획 재생).
 *
 * <p>사용법: {@code ClusterPlacementPlanner <출력 디렉터리(experiments/b2-load-balancing/cluster-placement)>}
 */
final class ClusterPlacementPlanner {

    static final int KEYS = 500_000;
    static final int WINDOWS = 20;
    static final long WINDOW_REQUESTS = 20_000;
    static final int CONCURRENCY = 32;
    static final int TABLETS_PER_NODE = 16;
    static final double TARGET_UTILIZATION = 0.7;
    static final int SCALE_EVENT_WINDOW = 10;
    static final int[] SEEDS = {1, 2};

    /** 시나리오: Runner 부하 설정과 노드 수(처음 → scale 후). */
    record Scenario(String name, Map<String, String> workload, int initialNodes, int finalNodes) {}

    record Bundle(String name, List<Scenario> scenarios, List<String> policies) {}

    static List<Bundle> bundles() {
        Map<String, String> hotRange = Map.of("distribution", "HOT_RANGE");
        Map<String, String> zipf = Map.of("distribution", "ZIPF");
        Map<String, String> moving = Map.of("distribution", "HOT_RANGE", "hot.range.shift.per.window", "0.05");
        Map<String, String> bursty = Map.of("distribution", "UNIFORM", "burst.every.windows", "5", "burst.factor", "3", "burst.width", "0.05");
        return List.of(
                new Bundle("bundle1", List.of(new Scenario("HOT_RANGE", hotRange, 4, 4), new Scenario("ZIPF", zipf, 4, 4)),
                        List.of("greedy-load-placement", "slicer-inspired", "estore-inspired", "cost-aware-rebalancing",
                                "hysteresis-rebalancing", "no-rebalance")),
                new Bundle("bundle2", List.of(new Scenario("MOVING_HOTSPOT", moving, 4, 4), new Scenario("BURSTY", bursty, 4, 4)),
                        List.of("greedy-load-placement", "slicer-inspired", "weighted-rendezvous", "hysteresis-rebalancing")),
                new Bundle("bundle3", List.of(new Scenario("SCALE_OUT", zipf, 3, 4)),
                        List.of("greedy-load-placement", "slicer-inspired", "weighted-rendezvous", "cost-aware-rebalancing")));
    }

    static PlacementPolicy policy(String name) {
        return switch (name) {
            case "greedy-load-placement" -> new GreedyLoadPlacement();
            case "slicer-inspired" -> new SlicerInspiredPlacement();
            case "estore-inspired" -> new EStoreInspiredPlacement();
            case "weighted-rendezvous" -> new WeightedRendezvousPlacement();
            case "cost-aware-rebalancing" -> new CostAwareRebalancing();
            case "hysteresis-rebalancing" -> new HysteresisRebalancing();
            case "no-rebalance" -> new PlacementPolicy() {
                @Override
                public String variant() {
                    return "no-rebalance";
                }

                @Override
                public PlacementDecision decide(PlacementInput input) {
                    return new PlacementDecision(List.of());
                }
            };
            default -> throw new IllegalArgumentException(name);
        };
    }

    private ClusterPlacementPlanner() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("usage: ClusterPlacementPlanner <experiments/b2-load-balancing/cluster-placement>");
            System.exit(2);
        }
        Path out = Path.of(args[0]);
        for (Bundle bundle : bundles()) {
            List<String> plan = new ArrayList<>();
            plan.add("# " + bundle.name() + " 계획 요약 (ClusterPlacementPlanner 자동 생성)");
            plan.add("");
            plan.add("계획 단계의 예상값이다(Runner와 같은 요청열로 센 Tablet 부하 기준). 실측은 EC2 결과로 본다.");
            plan.add("");
            plan.add("| scenario | policy | seed | 이동 Tablet 합 | 예상 평균 최대/평균 |");
            plan.add("|---|---|---|---|---|");
            for (Scenario scenario : bundle.scenarios()) {
                for (int seed : SEEDS) {
                    RunnerConfig config = config(scenario, "probe", seed, Path.of("unused"), Path.of("."));
                    long[][] counts = windowCounts(config);
                    for (String policyName : bundle.policies()) {
                        Path dir = out.resolve(bundle.name()).resolve(scenario.name());
                        Files.createDirectories(dir);
                        String base = policyName + "-s" + seed;
                        Planned planned = plan(scenario, config, counts, policy(policyName));
                        Files.write(dir.resolve(base + ".schedule"), planned.schedule(), StandardCharsets.UTF_8);
                        Files.write(dir.resolve(base + ".properties"),
                                properties(scenario, policyName, seed, dir.resolve(base + ".schedule")), StandardCharsets.UTF_8);
                        plan.add(String.format(Locale.ROOT, "| %s | %s | %d | %d | %.3f |",
                                scenario.name(), policyName, seed, planned.moves(), planned.meanImbalance()));
                    }
                }
            }
            Files.write(out.resolve(bundle.name()).resolve("plan.md"), plan, StandardCharsets.UTF_8);
            System.err.println("wrote " + out.resolve(bundle.name()));
        }
    }

    record Planned(List<String> schedule, int moves, double meanImbalance) {}

    /** window 0..W-1의 라우팅 계획. window w의 부하로 정책을 돌려 window w+1의 주인을 정한다. */
    static Planned plan(Scenario scenario, RunnerConfig config, long[][] counts, PlacementPolicy policy) {
        long[] hashes = new long[KEYS];
        for (int i = 0; i < KEYS; i++) {
            hashes[i] = KeyHash.of(WorkloadGenerator.key(i, config.keyLength()));
        }
        RoutingTable table = new HashRangeScheme(TABLETS_PER_NODE).initial(nodes(scenario.initialNodes()));
        List<Tablet> tablets = table.tablets();
        List<String> lines = new ArrayList<>();
        lines.add("# " + scenario.name() + " " + policy.variant() + " (ClusterPlacementPlanner 자동 생성. 직접 고치지 않는다)");
        lines.add("tablets " + tablets.size());
        for (Tablet t : tablets) {
            lines.add("tablet " + Long.toUnsignedString(t.range().start()) + " " + (t.owner().value() - 1));
        }
        Map<TabletId, Long> lastMoved = new HashMap<>();
        Map<TabletId, Long> bytes = tabletValues(table, hashes, null, config.valueSize());
        int moves = 0;
        double sumImbalance = 0;
        lines.add("window 0");
        for (int w = 0; w < WINDOWS; w++) {
            Map<TabletId, Long> load = tabletValues(table, hashes, counts[w], 0);
            Map<NodeId, Double> capacity = capacity(scenario, w + 1);
            sumImbalance += imbalance(table, load, capacity(scenario, w));
            if (w == WINDOWS - 1) {
                break;
            }
            PlacementDecision decision = policy.decide(new PlacementInput(table, load, bytes, capacity, w, lastMoved));
            table = decision.toPlan(table).applyAll();
            lines.add("window " + (w + 1));
            for (Change change : decision.moves()) {
                lastMoved.put(change.unit(), (long) w);
                lines.add("move " + indexOf(tablets, change.unit()) + " " + (change.to().value() - 1));
            }
            moves += decision.moves().size();
        }
        return new Planned(lines, moves, sumImbalance / WINDOWS);
    }

    /** Runner와 같은 방식으로 window별 key 요청 수를 센다. */
    static long[][] windowCounts(RunnerConfig config) {
        KeySpace keys = KeySpace.of(config);
        long[][] counts = new long[WINDOWS][KEYS];
        Map<Bytes, Integer> index = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            index.put(WorkloadGenerator.key(i, config.keyLength()), i);
        }
        for (int w = 0; w < WINDOWS; w++) {
            long requests = WorkloadGenerator.requestsIn(config, WINDOW_REQUESTS, w);
            for (int t = 0; t < CONCURRENCY; t++) {
                long n = requests / CONCURRENCY + (t < requests % CONCURRENCY ? 1 : 0);
                Supplier<Command> source = new WorkloadGenerator(
                        config, keys, new Random(config.seed() + w * ExperimentRunner.WINDOW_SEED_STRIDE + t), w)::next;
                for (long r = 0; r < n; r++) {
                    Command c = source.get();
                    Bytes key = c instanceof Command.Get g ? g.key() : c instanceof Command.Set s ? s.key() : ((Command.Delete) c).key();
                    counts[w][index.get(key)]++;
                }
            }
        }
        return counts;
    }

    static RunnerConfig config(Scenario scenario, String policy, int seed, Path schedule, Path resultsDir) {
        Properties p = new Properties();
        for (String line : properties(scenario, policy, seed, schedule)) {
            int eq = line.indexOf('=');
            if (eq > 0 && !line.startsWith("#") && !line.startsWith("schedule.file") && !line.startsWith("window.requests")
                    && !line.startsWith("prefill") && !line.startsWith("metrics.port") && !line.startsWith("refill.on.miss")) {
                p.setProperty(line.substring(0, eq), line.substring(eq + 1));
            }
        }
        p.setProperty("results.dir", resultsDir.toString());
        return RunnerConfig.from(p);
    }

    static List<String> properties(Scenario scenario, String policy, int seed, Path schedule) {
        List<String> lines = new ArrayList<>();
        lines.add("# B2 cluster-placement: " + scenario.name() + " / " + policy + " / seed " + seed
                + ". ClusterPlacementPlanner가 만든 파일이다(experiments/b2-load-balancing/cluster-placement/README.md).");
        lines.add("target.host=127.0.0.1");
        lines.add("target.port=7000");
        StringBuilder hosts = new StringBuilder();
        for (int i = 0; i < scenario.finalNodes(); i++) {
            hosts.append(i == 0 ? "" : ",").append("127.0.0.1:").append(7000 + i * 10);
        }
        lines.add("target.hosts=" + hosts);
        lines.add("study=b2-cluster-placement");
        lines.add("variant=" + policy);
        lines.add("scenario=" + scenario.name() + "-s" + seed);
        lines.add("seed=" + seed);
        lines.add("key.count=" + KEYS);
        lines.add("key.length=16");
        lines.add("value.size=1024");
        lines.add("mix.get=90");
        lines.add("mix.set=10");
        lines.add("mix.delete=0");
        new LinkedHashMap<>(scenario.workload()).forEach((k, v) -> lines.add(k + "=" + v));
        lines.add("concurrency=" + CONCURRENCY);
        lines.add("warmup.requests=20000");
        lines.add("measured.requests=" + WINDOW_REQUESTS * WINDOWS);
        lines.add("request.timeout.ms=2000");
        lines.add("repeat.count=1");
        lines.add("run.timeout.ms=240000");
        lines.add("execution.timeout.ms=240000");
        lines.add("results.dir=experiments/b2-load-balancing/cluster-placement/results");
        lines.add("schedule.file=" + schedule.toString().replace('\\', '/'));
        lines.add("window.requests=" + WINDOW_REQUESTS);
        lines.add("prefill=true");
        lines.add("metrics.port=9091");
        lines.add("refill.on.miss=true");
        lines.add("server.workers.hint=2");
        return lines;
    }

    private static List<NodeId> nodes(int n) {
        List<NodeId> nodes = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            nodes.add(new NodeId(i));
        }
        return nodes;
    }

    /** window {@code w}에 쓸 수 있는 노드와 용량(상대값: 처음 노드 수 기준 평균 이용률 0.7). */
    private static Map<NodeId, Double> capacity(Scenario scenario, int w) {
        int n = w < SCALE_EVENT_WINDOW ? scenario.initialNodes() : scenario.finalNodes();
        double perNode = WINDOW_REQUESTS / (scenario.initialNodes() * TARGET_UTILIZATION);
        Map<NodeId, Double> capacity = new LinkedHashMap<>();
        nodes(n).forEach(node -> capacity.put(node, perNode));
        return capacity;
    }

    /** Tablet별 합: {@code counts}가 null이면 key마다 {@code perKey}(byte), 아니면 key별 요청 수. */
    private static Map<TabletId, Long> tabletValues(RoutingTable table, long[] hashes, long[] counts, long perKey) {
        Map<TabletId, Long> sums = new HashMap<>();
        table.tablets().forEach(t -> sums.put(t.id(), 0L));
        for (int i = 0; i < hashes.length; i++) {
            long v = counts == null ? perKey : counts[i];
            if (v != 0) {
                sums.merge(table.lookup(hashes[i]).id(), v, Long::sum);
            }
        }
        return sums;
    }

    private static double imbalance(RoutingTable table, Map<TabletId, Long> load, Map<NodeId, Double> capacity) {
        Map<NodeId, Long> byNode = new HashMap<>();
        for (Tablet t : table.tablets()) {
            byNode.merge(t.owner(), load.get(t.id()), Long::sum);
        }
        double total = load.values().stream().mapToLong(Long::longValue).sum();
        double average = total / capacity.values().stream().mapToDouble(Double::doubleValue).sum();
        double max = capacity.entrySet().stream().mapToDouble(e -> byNode.getOrDefault(e.getKey(), 0L) / e.getValue()).max().orElse(0);
        return average == 0 ? 1 : max / average;
    }

    private static int indexOf(List<Tablet> tablets, TabletId id) {
        for (int i = 0; i < tablets.size(); i++) {
            if (tablets.get(i).id().equals(id)) {
                return i;
            }
        }
        throw new IllegalStateException("unknown tablet " + id);
    }
}
