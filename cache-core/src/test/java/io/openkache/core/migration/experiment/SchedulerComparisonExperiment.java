package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** 선행 Scheduler의 동일 입력 비교. CAMIG 후보 선택은 별도 비교군이다. */
final class SchedulerComparisonExperiment {
    static final String GENERATOR = "b3-scheduler-trace-1";
    static final int MAX_TICKS = 6000;
    static final List<String> SCENARIOS = List.of("heterogeneous", "conflict", "shared-source", "shared-destination", "dirty-skew", "deadline", "candidate-plan");
    record Condition(String scenario, String profile, List<Integer> keys, List<Double> rho, List<Long> deadlines,
                     List<List<Change>> candidates, long nodeBudget) {
        Map<String, Object> config() {
            return Map.of("scenario", scenario, "profile", profile, "keys", keys, "rho", rho, "deadlines", deadlines,
                    "nodeBudget", nodeBudget, "clusterBudget", nodeBudget * 4, "concurrency", 4, "endpointLimit", 4, "maxTicks", MAX_TICKS);
        }
    }
    static Condition condition(String scenario, String profile) {
        if (!SCENARIOS.contains(scenario) || !List.of("full", "quick").contains(profile)) throw new IllegalArgumentException("invalid scheduler condition");
        int base = profile.equals("full") ? 512 : 64;
        long node = profile.equals("full") ? 4096 : 1024;
        List<Integer> keys = new ArrayList<>(); List<Double> rho = new ArrayList<>(); List<Long> deadlines = new ArrayList<>();
        List<List<Change>> candidates = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            int size = base * new int[] {8, 4, 2, 1}[i % 4]; keys.add(size);
            double load = switch (scenario) {
                case "heterogeneous", "conflict" -> .1;
                case "dirty-skew" -> new double[] {1.2, .05, .5, .0}[i % 4];
                case "deadline" -> .1;
                default -> .5;
            };
            rho.add(load);
            long deadline = scenario.equals("deadline") ? 10 + (long) Math.ceil(size * 152. / node) * (i % 4 == 3 ? 2 : 5) : 10000;
            deadlines.add(deadline);
            long source = scenario.equals("shared-source") ? 1 : scenario.equals("heterogeneous") || scenario.equals("shared-destination") ? i + 1 : i % 4 + 1;
            long dest = scenario.equals("shared-destination") || scenario.equals("candidate-plan") ? 100
                    : scenario.equals("heterogeneous") || scenario.equals("shared-source") ? 100 + i : 100 + (i / 4 + i) % 4;
            if (scenario.equals("conflict")) {
                int[][] crossed = {{1,100},{1,101},{2,100},{3,102},{4,103},{2,102},{3,103},{4,100},{1,102},{2,103},{3,100},{4,101}};
                source = crossed[i][0]; dest = crossed[i][1];
            }
            Change first = new Change(new TabletId(i), new NodeId(source), new NodeId(dest));
            candidates.add(scenario.equals("candidate-plan") ? List.of(first,
                    new Change(new TabletId(i), new NodeId(source), new NodeId(101 + i % 3))) : List.of(first));
        }
        return new Condition(scenario, profile, List.copyOf(keys), List.copyOf(rho), List.copyOf(deadlines), List.copyOf(candidates), node);
    }
    static List<String> methods(String scenario) { return scenario.equals("candidate-plan") ? List.of("candidate-first", "candidate-balanced", "camig-selector") : SchedulerAlgorithms.METHODS; }
    static List<Change> planFor(Condition c, String method) {
        if (method.equals("camig-selector")) return SchedulerAlgorithms.camig(c.candidates(), c.keys().stream().map(k -> k * 152L).toList(), c.nodeBudget());
        if (method.equals("candidate-balanced")) {
            Map<NodeId, Long> load = new java.util.HashMap<>(); List<Change> result = new ArrayList<>();
            for (int i = 0; i < c.candidates().size(); i++) {
                Change best = c.candidates().get(i).stream().min(java.util.Comparator.comparingLong(p -> load.getOrDefault(p.to(), 0L))).orElseThrow();
                result.add(best); load.merge(best.to(), c.keys().get(i) * 152L, Long::sum);
            }
            return List.copyOf(result);
        }
        return c.candidates().stream().map(List::getFirst).toList();
    }
    static List<Map<String, Object>> changes(List<Change> plan) {
        return plan.stream().map(c -> Map.<String, Object>of("tablet", c.unit().value(), "source", c.from().value(), "destination", c.to().value())).toList();
    }
    static String workloadHash(Condition c, long seed) {
        return MovementExperiment.hash(Map.of("generator", GENERATOR, "seed", seed, "keys", c.keys(), "rho", c.rho(), "referenceBudget", c.nodeBudget(), "maxTicks", MAX_TICKS));
    }
    static Map<String, Object> input(Condition c, long seed) {
        return Map.of("condition", c.config(), "candidates", c.candidates().stream().map(SchedulerComparisonExperiment::changes).toList(), "workloadHash", workloadHash(c, seed));
    }
    static Map<String, Object> run(Condition c, String method, String variant, long seed) {
        long planningStart = System.nanoTime();
        List<Change> plan = planFor(c, method);
        long initialPlannerNanos = System.nanoTime() - planningStart;
        String executionMethod = c.scenario().equals("candidate-plan") ? "mongodb" : method;
        var f = new MultiMigrationFixture(plan, c.keys(), seed, variant, (int) c.nodeBudget(), c.nodeBudget() / 4);
        var settings = new MigrationScheduler.Settings(4, 4, c.nodeBudget() * 4, c.nodeBudget(), MAX_TICKS);
        double[] dirty = new double[plan.size()]; long[] remaining = new long[plan.size()];
        var tasks = new ArrayList<SchedulerAlgorithms.Task>();
        var requests = new ArrayList<MovementExperiment.Case>();
        for (int i = 0; i < plan.size(); i++) {
            // 초기 쓰기율은 입력 사전 관측값으로 가정하고, 실행 뒤 성공 write의 EWMA로 갱신한다.
            dirty[i] = variant.equals("pre-copy") ? c.rho().get(i) * c.nodeBudget() : 0;
            remaining[i] = c.keys().get(i) * 152L;
            tasks.add(new SchedulerAlgorithms.Task(i, plan.get(i), remaining[i], dirty[i], c.deadlines().get(i)));
            requests.add(new MovementExperiment.Case("scheduler", "M5", 12, 4, 4, "independent", c.keys().get(i),
                    c.nodeBudget() * 4, c.nodeBudget(), c.nodeBudget(), (int) c.nodeBudget(), c.nodeBudget() / 4, c.rho().get(i), variant));
        }
        long constructorStart = System.nanoTime();
        var algorithm = new SchedulerAlgorithms(executionMethod, tasks, settings, i -> remaining[i], i -> dirty[i]);
        initialPlannerNanos += System.nanoTime() - constructorStart;
        var timed = new TimedPolicy(algorithm);
        var scheduler = new MigrationScheduler(plan, settings, f::handle, timed);
        List<Map<String, Object>> budgets = new ArrayList<>(), events = new ArrayList<>();
        String[] state = new String[plan.size()]; java.util.Arrays.fill(state, "PENDING");
        int[] rounds = new int[plan.size()]; long[] phaseBytes = new long[plan.size()], phaseTick = new long[plan.size()];
        String[] modelPhase = new String[plan.size()]; int[] modelRound = new int[plan.size()];
        int tick = 0;
        for (; tick < MAX_TICKS && !scheduler.finished(); tick++) {
            for (int i = 0; i < plan.size(); i++) {
                var h = f.handles.get(i);
                if (f.routing.get().get(h.change.unit()).owner().equals(h.change.from()) && !h.sourceWritable()) h.blockedTicks++;
                long before = h.accepted;
                MovementExperiment.requests(h, requests.get(i), seed, i, tick);
                // 조건부 성공 요청률의 보수적 byte 추정. 실제 dirty 중복 제거율의 완전한 관측은 아니다.
                if (variant.equals("pre-copy")) dirty[i] = .875 * dirty[i] + .125 * (h.accepted - before) * 152.;
                if (!h.state().equals(modelPhase[i]) || h.rounds() != modelRound[i]) {
                    long elapsed = Math.max(1, tick - phaseTick[i]);
                    phaseBytes[i] = h.sentBytes(); phaseTick[i] = tick;
                    remaining[i] = Math.max(152, (long) Math.min(c.keys().get(i) * 152., dirty[i] * elapsed));
                    modelPhase[i] = h.state(); modelRound[i] = h.rounds();
                }
                if (h.post != null) remaining[i] = Math.max(152, h.post.remainingKeys() * 152L);
                else if (h.pre == null || h.state().equals("COPYING")) remaining[i] = Math.max(152, c.keys().get(i) * 152L - h.sentBytes());
                else remaining[i] = Math.max(152, remaining[i] - Math.max(0, h.sentBytes() - phaseBytes[i]));
                phaseBytes[i] = h.sentBytes();
            }
            var observation = scheduler.step(); MovementExperiment.checkBudgets(observation, requests.getFirst());
            List<Map<String, Object>> grants = new ArrayList<>();
            for (var j : observation.jobs()) if (observation.allocated().containsKey(j.index())) grants.add(Map.of("job", j.index(),
                    "allocatedBytes", observation.allocated().get(j.index()), "consumedBytes", observation.consumed().get(j.index())));
            budgets.add(Map.of("tick", tick, "grants", grants));
            for (var job : observation.jobs()) {
                var h = f.handles.get(job.index());
                if (h.commitTick < 0 && f.routing.get().get(h.change.unit()).owner().equals(h.change.to())) h.commitTick = tick + 1L;
                if (!h.state().equals(state[job.index()]) || h.rounds() != rounds[job.index()]) {
                    events.add(Map.of("job", job.index(), "tick", tick + 1, "state", h.state(), "round", h.rounds(), "sentBytes", h.sentBytes()));
                    state[job.index()] = h.state(); rounds[job.index()] = h.rounds();
                }
            }
        }
        MovementExperiment.require(scheduler.finished() && f.commits == plan.size() && f.routing.get().version().value() == plan.size(), "incomplete scheduler comparison");
        MovementExperiment.require(f.guardData.size() == 8 && f.guardData.values().stream().allMatch(v -> v.equals(MigrationFixture.value(-1)))
                && f.routing.get().get(f.guard).epoch().value() == 0
                && f.nodes.get(plan.getFirst().from()).tablets.get(f.guard).get().equals(f.guardData), "guard changed");
        List<Map<String, Object>> jobs = new ArrayList<>();
        for (var j : scheduler.jobs()) {
            var h = f.handles.get(j.index());
            MovementExperiment.require(j.status() == MigrationScheduler.Status.DONE && h.sourceData.isEmpty() && h.oracle.equals(h.finalData()), "job data mismatch");
            MovementExperiment.require(f.nodes.get(h.change.to()).tablets.get(h.change.unit()).get().equals(h.oracle)
                    && f.routing.get().get(h.change.unit()).owner().equals(h.change.to()) && f.routing.get().get(h.change.unit()).epoch().value() == 1
                    && !f.nodes.get(h.change.from()).frozen.containsKey(h.change.unit()) && !f.nodes.get(h.change.from()).hooks.containsKey(h.change.unit())
                    && h.request(h.change.from(), new Command.Get(h.keys.getFirst())).status() == ResultStatus.MOVED, "ownership/cleanup mismatch");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("job", j.index()); row.put("startTick", j.startTick()); row.put("commitTick", h.commitTick); row.put("endTick", j.endTick()); row.put("status", "DONE");
            row.put("source", h.change.from().value()); row.put("destination", h.change.to().value()); row.put("deadline", c.deadlines().get(j.index()));
            row.put("sentBytes", h.sentBytes()); row.put("blockedTicks", h.blockedTicks); row.put("writes", h.writes); row.put("acceptedWrites", h.accepted);
            row.put("writeBusy", h.writeBusy); row.put("reads", h.reads); row.put("readBusy", h.readBusy); row.put("rounds", h.rounds()); row.put("stopReason", h.reason());
            row.put("sourceRecords", 0); row.put("exactMatch", true); row.put("oracleHash", MovementExperiment.dataHash(h.oracle)); row.put("destinationHash", MovementExperiment.dataHash(h.finalData()));
            jobs.add(row);
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("scenario", c.scenario()); result.put("method", method); result.put("executionMethod", executionMethod); result.put("variant", variant); result.put("seed", seed);
        result.put("condition", c.config()); result.put("inputHash", MovementExperiment.hash(input(c, seed))); result.put("workloadHash", workloadHash(c, seed));
        result.put("plan", changes(plan)); result.put("planHash", MovementExperiment.hash(changes(plan)));
        result.put("jobs", jobs); result.put("budgets", budgets); result.put("phaseEvents", events); result.put("metrics", metrics(jobs, tick, c));
        result.put("plannerWallNanos", initialPlannerNanos + timed.nanos); result.put("policyCalls", algorithm.calls); result.put("policyOperations", algorithm.operations);
        return result;
    }
    static Map<String, Object> metrics(List<Map<String, Object>> jobs, long tick, Condition c) {
        Map<String, Object> metrics = new TreeMap<>();
        metrics.put("makespan_ticks", tick); metrics.put("mean_completion_ticks", jobs.stream().mapToLong(j -> n(j, "endTick")).average().orElseThrow());
        metrics.put("wait_p99_ticks", MovementExperiment.percentile(jobs.stream().map(j -> n(j, "startTick")).toList(), .99));
        long sent = jobs.stream().mapToLong(j -> n(j, "sentBytes")).sum(); metrics.put("sent_bytes", sent);
        metrics.put("aggregate_bytes_per_tick", sent / (double) tick); metrics.put("initial_transfer_ratio", sent / (double) (c.keys().stream().mapToLong(k -> k * 152L).sum()));
        metrics.put("deadline_misses", jobs.stream().filter(j -> n(j, "endTick") > n(j, "deadline")).count());
        for (String name : List.of("blockedTicks", "writeBusy", "readBusy", "acceptedWrites", "writes", "reads")) metrics.put(name, jobs.stream().mapToLong(j -> n(j, name)).sum());
        metrics.put("exact_comparisons", jobs.size()); metrics.put("invariant_violations", 0); metrics.put("completion_ratio", 1.);
        return metrics;
    }
    static long n(Map<String, Object> map, String key) { return MovementResearchMain.integer(map.get(key)); }
    private static final class TimedPolicy implements MigrationScheduler.Policy {
        private final SchedulerAlgorithms delegate; long nanos;
        TimedPolicy(SchedulerAlgorithms delegate) { this.delegate = delegate; }
        @Override public List<Integer> order(long tick, List<MigrationScheduler.JobView> jobs, MigrationScheduler.Settings s) {
            long start = System.nanoTime(); try { return delegate.order(tick, jobs, s); } finally { nanos += System.nanoTime() - start; }
        }
        @Override public Map<Integer, Long> allocate(long tick, List<MigrationScheduler.JobView> active, MigrationScheduler.Settings s) {
            long start = System.nanoTime(); try { return delegate.allocate(tick, active, s); } finally { nanos += System.nanoTime() - start; }
        }
        @Override public void started(long tick, MigrationScheduler.JobView j) { delegate.started(tick, j); }
    }
    private SchedulerComparisonExperiment() {}
}
