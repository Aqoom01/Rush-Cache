package io.openkache.core.migration.experiment;

import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.migration.MigrationScheduler.JobView;
import io.openkache.core.migration.MigrationScheduler.Settings;
import io.openkache.core.migration.MigrationScheduler.Status;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;

/** 선행 핵심 알고리즘의 endpoint-only 연구 구현. 원문·변형 표는 scheduler-comparison/README.md. */
final class SchedulerAlgorithms implements MigrationScheduler.Policy {
    static final List<String> METHODS = List.of("fifo", "mongodb", "tidb-token", "tidb-feedback",
            "iter-gwin", "single-mcs", "slamig", "vmbuddies", "fpta");
    record Task(int id, Change change, long bytes, double dirty, long deadline) {}
    final String method;
    final List<Task> tasks;
    final IntToLongFunction remaining;
    final IntToDoubleFunction dirty;
    final List<Integer> plan;
    final Map<NodeId, Double> tokens = new HashMap<>(), rates = new HashMap<>();
    long calls, operations;
    private Map<Integer, Long> cachedFlow;
    private List<Integer> cachedIds = List.of();

    SchedulerAlgorithms(String method, List<Task> tasks, Settings settings, IntToLongFunction remaining, IntToDoubleFunction dirty) {
        if (!METHODS.contains(method)) throw new IllegalArgumentException("unknown scheduler method");
        this.method = method; this.tasks = List.copyOf(tasks); this.remaining = remaining; this.dirty = dirty;
        for (int i = 0; i < tasks.size(); i++) if (tasks.get(i).id() != i) throw new IllegalArgumentException("task IDs must match order");
        this.plan = switch (method) {
            case "iter-gwin" -> graphPlan(tasks, false, settings.nodeBytesPerTick());
            case "single-mcs" -> graphPlan(tasks, true, settings.nodeBytesPerTick());
            case "slamig" -> deadlinePlan(tasks, settings.nodeBytesPerTick());
            default -> tasks.stream().map(Task::id).toList();
        };
        for (Task task : tasks) for (NodeId node : endpoints(task.change())) { tokens.put(node, 1.); rates.put(node, 1. / 12); }
    }

    @Override public List<Integer> order(long tick, List<JobView> jobs, Settings settings) {
        calls++;
        Set<Integer> queued = new HashSet<>(); Set<NodeId> busy = new HashSet<>(); Map<NodeId, Integer> pending = new HashMap<>();
        for (JobView job : jobs) {
            if (job.status() == Status.QUEUED) queued.add(job.index());
            if (job.status() == Status.RUNNING) for (NodeId node : endpoints(job.change())) { busy.add(node); pending.merge(node, 1, Integer::sum); }
        }
        List<Integer> ordered = plan.stream().filter(queued::contains).toList();
        if (method.equals("tidb-token") || method.equals("tidb-feedback")) {
            if (tick > 0) for (NodeId node : tokens.keySet()) {
                if (method.equals("tidb-feedback") && tick % 4 == 0) {
                    // PD v2 방향을 옮긴 명시적 AIMD 변형. TiKV 원 구현의 상수/공식을 재현한 것은 아니다.
                    double rate = rates.get(node);
                    rates.put(node, pending.getOrDefault(node, 0) >= 2 ? Math.max(1. / 64, rate * .5) : Math.min(.5, rate + 1. / 32));
                }
                tokens.put(node, Math.min(4., tokens.get(node) + rates.get(node)));
            }
            Map<NodeId, Double> available = new HashMap<>(tokens);
            List<Integer> result = new ArrayList<>();
            int slots = settings.maxConcurrentJobs() - (int) jobs.stream().filter(j -> j.status() == Status.RUNNING).count();
            for (int id : ordered) {
                if (result.size() >= slots) break;
                Task task = tasks.get(id);
                if (endpoints(task.change()).stream().allMatch(n -> available.get(n) >= 1.)) {
                    result.add(id); endpoints(task.change()).forEach(n -> available.merge(n, -1., Double::sum));
                }
            }
            return result;
        }
        if (List.of("mongodb", "iter-gwin", "single-mcs", "slamig").contains(method)) {
            List<Integer> result = new ArrayList<>();
            for (int id : ordered) {
                var e = endpoints(tasks.get(id).change());
                if (e.stream().noneMatch(busy::contains)) { result.add(id); busy.addAll(e); }
            }
            return result;
        }
        return ordered;
    }

    @Override public void started(long tick, JobView job) {
        if (method.startsWith("tidb-")) for (NodeId node : endpoints(job.change())) {
            if (tokens.get(node) < 1. - 1e-9) throw new IllegalStateException("token underflow");
            tokens.put(node, tokens.get(node) - 1.);
        }
    }

    @Override public Map<Integer, Long> allocate(long tick, List<JobView> active, Settings settings) {
        if (active.isEmpty()) return Map.of();
        if (method.equals("fpta")) {
            var ids = active.stream().map(JobView::index).toList();
            if (!ids.equals(cachedIds)) { cachedIds = ids; cachedFlow = fpta(active.stream().map(j -> tasks.get(j.index())).toList(), settings, .2); operations++; }
            return cachedFlow;
        }
        if (!method.equals("vmbuddies")) return null;
        var ids = active.stream().map(JobView::index).toList();
        if (ids.equals(cachedIds) && tick % 4 != 0) return cachedFlow;
        cachedIds = ids;
        // Eq.(9)/(10)의 공통 완료시간을 이분 탐색한다. 원문의 quasi-secant 대신 결정적 수치 해법.
        double low = 0, high = 1;
        for (int i = 0; i < 60 && !fits(active, ratesFor(active, high, settings), settings); i++) high *= 2;
        for (int i = 0; i < 32; i++) {
            double target = (low + high) / 2;
            if (fits(active, ratesFor(active, target, settings), settings)) high = target; else low = target;
        }
        operations += 32L * active.size();
        Map<Integer, Double> requested = ratesFor(active, high, settings);
        Map<Integer, Long> result = new LinkedHashMap<>();
        active.forEach(j -> result.put(j.index(), Math.max(0, (long) Math.floor(requested.get(j.index())))));
        cachedFlow = result;
        return result;
    }

    private Map<Integer, Double> ratesFor(List<JobView> active, double target, Settings settings) {
        Map<Integer, Double> result = new LinkedHashMap<>();
        for (JobView job : active) {
            double lo = .000001, hi = Math.min(settings.nodeBytesPerTick(), settings.clusterBytesPerTick());
            for (int i = 0; i < 28; i++) {
                double rate = (lo + hi) / 2;
                if (copyTime(Math.max(152, remaining.applyAsLong(job.index())), dirty.applyAsDouble(job.index()), rate) > target) lo = rate; else hi = rate;
            }
            // target보다 느릴 수밖에 없는 개별 endpoint도 feasibility에 드러낸다.
            if (copyTime(Math.max(152, remaining.applyAsLong(job.index())), dirty.applyAsDouble(job.index()), hi) > target + 1e-7) hi = settings.nodeBytesPerTick() + settings.clusterBytesPerTick();
            result.put(job.index(), hi);
        }
        return result;
    }
    static double copyTime(long bytes, double dirty, double rate) {
        // VMbuddies Eq.(3)~(5), WWS 상한 적용. 10회의 고정 horizon이며 실제 종료 조건과 구별한다.
        double volume = bytes, time = 0;
        for (int i = 0; i <= 10; i++) { double dt = volume / rate; time += dt; volume = Math.min(bytes, dirty * dt); }
        return time;
    }
    private static boolean fits(List<JobView> active, Map<Integer, Double> allocation, Settings settings) {
        double total = 0; Map<NodeId, Double> nodes = new HashMap<>();
        for (JobView j : active) {
            double n = allocation.get(j.index()); total += n;
            for (NodeId node : endpoints(j.change())) nodes.merge(node, n, Double::sum);
        }
        return total <= settings.clusterBytesPerTick() && nodes.values().stream().allMatch(n -> n <= settings.nodeBytesPerTick());
    }

    static List<NodeId> endpoints(Change change) { return List.of(change.from(), change.to()); }
    static boolean conflict(Change a, Change b) { return endpoints(a).stream().anyMatch(endpoints(b)::contains); }
    static long[] graph(List<Change> changes) {
        if (changes.size() > 60) throw new IllegalArgumentException("research graph limited to 60 vertices");
        long[] graph = new long[changes.size()];
        for (int i = 0; i < graph.length; i++) for (int j = 0; j < i; j++) if (conflict(changes.get(i), changes.get(j))) { graph[i] |= 1L << j; graph[j] |= 1L << i; }
        return graph;
    }
    static List<Long> cliques(long[] graph, boolean complement) {
        return cliques(graph, complement, (1L << graph.length) - 1);
    }
    private static List<Long> cliques(long[] graph, boolean complement, long mask) {
        long[] neighbors = graph.clone();
        if (complement) for (int i = 0; i < graph.length; i++) neighbors[i] = mask & ~graph[i] & ~(1L << i);
        List<Long> result = new ArrayList<>(); bron(0, mask, 0, neighbors, result); return result;
    }
    private static void bron(long selected, long candidates, long excluded, long[] graph, List<Long> result) {
        if ((candidates | excluded) == 0) { if (selected != 0) result.add(selected); return; }
        int pivot = Long.numberOfTrailingZeros(candidates | excluded);
        long choices = candidates & ~graph[pivot];
        while (choices != 0) {
            long bit = Long.lowestOneBit(choices); int v = Long.numberOfTrailingZeros(bit);
            bron(selected | bit, candidates & graph[v], excluded & graph[v], graph, result);
            candidates &= ~bit; excluded |= bit; choices &= ~bit;
        }
    }
    static List<Long> cliquePartition(long[] graph) {
        List<Long> all = new ArrayList<>(cliques(graph, false)), result = new ArrayList<>();
        long mask = (1L << graph.length) - 1;
        while (mask != 0) {
            long remaining = mask;
            long best = all.stream().mapToLong(n -> n & remaining).boxed()
                    .max(Comparator.<Long>comparingInt(Long::bitCount).thenComparingLong(n -> -n)).orElseThrow();
            if (best == 0) throw new IllegalStateException("clique cover lost vertex");
            result.add(best); mask &= ~best;
        }
        return result;
    }
    private static double weight(Task t, long node) {
        if (t.deadline() >= 10000) return 1.;
        double slack = t.deadline() - copyTime(t.bytes(), t.dirty(), node);
        return slack > 1 ? 10. / slack : slack < -1 ? 100 * -slack : 100.;
    }
    static List<Integer> graphPlan(List<Task> tasks, boolean single, long node) {
        long[] g = graph(tasks.stream().map(Task::change).toList());
        List<Long> cover = single ? cliquePartition(g) : List.of();
        long remaining = (1L << tasks.size()) - 1;
        List<Integer> order = new ArrayList<>();
        while (remaining != 0) {
            long candidates = remaining, chosen = 0;
            if (single) {
                for (long clique : cover) {
                    long eligible = clique & remaining;
                    for (int v = 0; v < g.length; v++) if ((g[v] & chosen) != 0) eligible &= ~(1L << v);
                    int best = best(eligible, tasks, g, remaining & ~clique, node);
                    if (best >= 0) { chosen |= 1L << best; order.add(best); }
                }
            } else while (candidates != 0) {
                int best = best(candidates, tasks, g, candidates, node);
                chosen |= 1L << best; order.add(best); candidates &= ~g[best] & ~(1L << best);
            }
            if (chosen == 0) throw new IllegalStateException("empty independent group");
            remaining &= ~chosen;
        }
        return List.copyOf(order);
    }
    private static int best(long mask, List<Task> tasks, long[] g, long degreeMask, long node) {
        int best = -1; double score = -1;
        for (int i = 0; i < tasks.size(); i++) if ((mask & (1L << i)) != 0) {
            double value = weight(tasks.get(i), node) / (Long.bitCount(g[i] & degreeMask) + 1.);
            if (value > score) { score = value; best = i; }
        }
        return best;
    }
    static List<Integer> deadlinePlan(List<Task> tasks, long node) {
        long[] g = graph(tasks.stream().map(Task::change).toList());
        var partitions = cliquePartition(g); List<List<Integer>> queues = new ArrayList<>();
        // SLAMIG cost αT + β(T-D) + γI: α=β=1, γ=0 명시. topology impact 항은 비활성화한다.
        Comparator<Integer> byCost = Comparator.<Integer>comparingDouble(i -> 2 * copyTime(tasks.get(i).bytes(), tasks.get(i).dirty(), node) - tasks.get(i).deadline()).thenComparingInt(i -> i);
        for (long part : partitions) {
            List<Integer> q = new ArrayList<>(); for (int i = 0; i < tasks.size(); i++) if ((part & (1L << i)) != 0) q.add(i);
            q.sort(byCost); queues.add(q);
        }
        List<List<Integer>> groups = new ArrayList<>();
        while (queues.stream().anyMatch(q -> !q.isEmpty())) for (var q : queues) if (!q.isEmpty()) {
            int id = q.removeFirst();
            var group = groups.stream().filter(s -> s.stream().noneMatch(j -> (g[id] & (1L << j)) != 0)).findFirst();
            if (group.isPresent()) group.get().add(id); else { var next = new ArrayList<Integer>(); next.add(id); groups.add(next); }
        }
        groups.sort(Comparator.comparingDouble(s -> s.stream().mapToDouble(i -> 2 * copyTime(tasks.get(i).bytes(), tasks.get(i).dirty(), node) - tasks.get(i).deadline()).sum()));
        return groups.stream().flatMap(List::stream).toList();
    }

    /** Wang et al. Algorithm 1: 단일 경로(전체+양 endpoint) primal-dual flow, ε=.2. */
    static Map<Integer, Long> fpta(List<Task> tasks, Settings settings, double epsilon) {
        Map<NodeId, Integer> indices = new LinkedHashMap<>();
        for (Task task : tasks) for (NodeId n : endpoints(task.change())) indices.computeIfAbsent(n, ignored -> indices.size() + 1);
        int n = indices.size() + 1;
        double delta = (1 + epsilon) / Math.pow((1 + epsilon) * (n + 1), 1 / epsilon);
        double scale = Math.log((1 + epsilon) / delta) / Math.log(1 + epsilon);
        double[] lengths = new double[n], capacity = new double[n], flow = new double[tasks.size()];
        java.util.Arrays.fill(lengths, delta); java.util.Arrays.fill(capacity, settings.nodeBytesPerTick()); capacity[0] = settings.clusterBytesPerTick();
        int[][] paths = new int[tasks.size()][];
        for (int i = 0; i < tasks.size(); i++) paths[i] = new int[] {0, indices.get(tasks.get(i).change().from()), indices.get(tasks.get(i).change().to())};
        for (int round = 1; round <= Math.ceil(scale); round++) {
            double threshold = Math.min(1, delta * Math.pow(1 + epsilon, round));
            for (int j = 0; j < paths.length; j++) {
                int[] path = paths[j];
                while (java.util.Arrays.stream(path).mapToDouble(e -> lengths[e]).sum() < threshold) {
                    double amount = java.util.Arrays.stream(path).mapToDouble(e -> capacity[e]).min().orElseThrow();
                    flow[j] += amount;
                    for (int e : path) lengths[e] *= 1 + epsilon * amount / capacity[e];
                }
            }
        }
        double[] used = new double[n];
        for (int j = 0; j < paths.length; j++) { flow[j] /= scale; for (int e : paths[j]) used[e] += flow[j]; }
        double projection = 1;
        for (int e = 0; e < n; e++) projection = Math.max(projection, used[e] / capacity[e]);
        Map<Integer, Long> result = new LinkedHashMap<>();
        for (int j = 0; j < paths.length; j++) result.put(tasks.get(j).id(), (long) Math.floor(flow[j] / projection));
        return result;
    }

    /** CAMIG Eq.(17)~(21). 목적지 후보 중 같은 목표 개선을 주는 pair를 선택하는 제한된 selector. */
    static List<Change> camig(List<List<Change>> candidates, List<Long> bytes, long nodeBudget) {
        List<Change> pairs = candidates.stream().flatMap(List::stream).distinct().toList();
        // pair는 unit을 제외하고 묶는다.
        List<Change> unique = new ArrayList<>();
        for (Change c : pairs) if (unique.stream().noneMatch(p -> p.from().equals(c.from()) && p.to().equals(c.to()))) unique.add(c);
        long[] graph = graph(unique); var cliques = cliques(graph, false); var sets = cliques(graph, true);
        List<Integer> selectedPairs = new ArrayList<>(); Set<Integer> selectedJobs = new HashSet<>();
        List<Change> result = new ArrayList<>(java.util.Collections.nCopies(candidates.size(), null));
        while (selectedJobs.size() < candidates.size()) {
            double minIndependent = Double.POSITIVE_INFINITY;
            for (int v = 0; v < unique.size(); v++) {
                double value = membership(sets, v, selectedPairs);
                if (value > 0) minIndependent = Math.min(minIndependent, value);
            }
            double zeroPenalty = Double.isFinite(minIndependent) ? 1 / minIndependent + 1 : 2;
            Change best = null; double min = Double.POSITIVE_INFINITY; int bestJob = -1, bestPair = -1;
            for (int job = 0; job < candidates.size(); job++) if (!selectedJobs.contains(job)) for (Change c : candidates.get(job)) {
                int v = 0; while (!unique.get(v).from().equals(c.from()) || !unique.get(v).to().equals(c.to())) v++;
                int index = v;
                double migc;
                if (selectedPairs.isEmpty()) {
                    int maxClique = cliques.stream().filter(s -> (s & (1L << index)) != 0).mapToInt(Long::bitCount).max().orElseThrow();
                    int maxSet = sets.stream().filter(s -> (s & (1L << index)) != 0).mapToInt(Long::bitCount).max().orElseThrow();
                    migc = maxClique / (double) maxSet;
                } else {
                    double independent = membership(sets, index, selectedPairs);
                    migc = membership(cliques, index, selectedPairs) + (independent > 0 ? 1 / independent : zeroPenalty);
                }
                double overhead = bytes.get(job) / (double) nodeBudget;
                double score = overhead * (1 + migc);
                if (score < min) { min = score; best = c; bestJob = job; bestPair = index; }
            }
            result.set(bestJob, best); selectedJobs.add(bestJob);
            if (!selectedPairs.contains(bestPair)) selectedPairs.add(bestPair);
            // 선택된 pair와 남은 후보 pair만 유지한다. 작은 연구 graph에서는 집합을 다시 열거한다.
            long available = 0;
            for (int p = 0; p < unique.size(); p++) {
                Change pair = unique.get(p);
                boolean pending = false;
                for (int j = 0; j < candidates.size(); j++) if (!selectedJobs.contains(j)
                        && candidates.get(j).stream().anyMatch(c -> c.from().equals(pair.from()) && c.to().equals(pair.to()))) pending = true;
                if (pending || selectedPairs.contains(p)) available |= 1L << p;
            }
            cliques = cliques(graph, false, available); sets = cliques(graph, true, available);
        }
        return List.copyOf(result);
    }
    private static double membership(List<Long> sets, int v, List<Integer> selected) {
        if (selected.isEmpty()) return 0;
        var relevant = sets.stream().filter(s -> (s & (1L << v)) != 0).toList();
        long count = 0; for (long set : relevant) for (int id : selected) if ((set & (1L << id)) != 0) count++;
        return count / (double) (relevant.size() * selected.size());
    }
}
