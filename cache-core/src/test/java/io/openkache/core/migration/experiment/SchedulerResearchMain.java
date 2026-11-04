package io.openkache.core.migration.experiment;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/** 선행 Scheduler 비교 전용 backend. 원본을 streaming 검증하고 검증된 전체 조합에서만 요약한다. */
public final class SchedulerResearchMain {
    static final String SCHEMA = "b3-scheduler-json-1";
    static int seeds(String profile) {
        if (!List.of("full", "quick").contains(profile)) throw new IllegalArgumentException("unknown profile");
        return profile.equals("full") ? 5 : 2;
    }
    static int expected(String profile) { return (6 * SchedulerAlgorithms.METHODS.size() + 3) * 3 * seeds(profile); }
    static final List<String> HARD_ENDPOINT = List.of("mongodb", "iter-gwin", "single-mcs", "slamig");

    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) {
            Path folder = Path.of(args[1]).toAbsolutePath().normalize();
            var complete = properties(folder.resolve("completion.properties"));
            require(SCHEMA.equals(complete.getProperty("resultSchemaVersion")), "scheduler schema mismatch");
            regenerate(folder, complete.getProperty("profile")); return;
        }
        if (args.length != 4) throw new IllegalArgumentException("<folder> <full|quick> <repeat> <repeats>");
        Path folder = Path.of(args[0]).toAbsolutePath().normalize(); String profile = args[1];
        int repeat = Integer.parseInt(args[2]), repeats = Integer.parseInt(args[3]);
        require(repeat > 0 && repeat <= repeats, "invalid repeat");
        var context = BaselineResearchMain.context(properties(folder.resolve("launch.properties")), repeat, repeats);
        context.remove("nodeCount");
        context.put("storageLayer", "multi-node-in-memory-protocol-fixture");
        context.put("modelConfig", Map.of("generatorVersion", SchedulerComparisonExperiment.GENERATOR, "keyBytes", 16, "valueBytes", 128,
                "recordBytes", 152, "timeUnit", "tick", "deletePercent", 30, "readsPerJobPerTick", 2));
        Path originals = Files.createDirectory(folder.resolve("results"));
        for (String scenario : SchedulerComparisonExperiment.SCENARIOS) {
            var c = SchedulerComparisonExperiment.condition(scenario, profile);
            for (String method : SchedulerComparisonExperiment.methods(scenario)) for (String variant : MovementExperiment.VARIANTS) {
                for (int seed = 1; seed <= seeds(profile); seed++) {
                    var root = record(c, method, variant, seed);
                    var full = new LinkedHashMap<>(context); full.putAll(root);
                    full.put("executionId", context.get("suiteExecutionId") + "-r" + repeat + "-" + scenario + "-" + method + "-" + variant + "-s" + seed);
                    Files.writeString(originals.resolve(scenario + "-" + method + "-" + variant + "-seed" + seed + ".json"), BaselineJson.encode(full) + "\n", StandardCharsets.UTF_8);
                }
                System.out.println(scenario + "/" + method + "/" + variant + ": " + seeds(profile) + " seeds 완료");
            }
        }
        regenerate(folder, profile);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion=" + SCHEMA
                + "\ncases=" + expected(profile) + "\nprofile=" + profile + "\nviolations=0\n", StandardCharsets.UTF_8);
    }
    static Map<String, Object> record(SchedulerComparisonExperiment.Condition c, String method, String variant, long seed) {
        var root = SchedulerComparisonExperiment.run(c, method, variant, seed);
        root.put("resultSchemaVersion", SCHEMA); root.put("timeDomain", "virtual"); root.put("outcome", "completed"); root.put("validationStatus", "valid");
        root.put("study", "b3-live-migration"); root.put("inputHashKind", "candidate-plan-and-generator-specification-sha256");
        root.put("stage", c.scenario().equals("candidate-plan") ? "candidate-plan" : "fixed-plan");
        root.put("methodConfig", methodConfig(method)); root.put("evidenceHash", evidenceHash(root)); return root;
    }
    static Map<String, Object> methodConfig(String method) {
        Map<String, Object> values = new TreeMap<>(); values.put("implementation", "endpoint-only-research-adaptation-1");
        switch (method) {
            case "fifo" -> values.put("admission", "oldest-eligible");
            case "mongodb", "candidate-first" -> values.put("maxJobsPerEndpoint", 1);
            case "candidate-balanced" -> { values.put("selection", "minimum-destination-planned-bytes"); values.put("executionScheduler", "mongodb"); }
            case "tidb-token", "tidb-feedback" -> {
                values.put("initialTokens", 1); values.put("bucketCapacity", 4); values.put("jobTokenCost", 1);
                values.put("initialTokensPerTick", 1. / 12);
                values.put("feedback", method.equals("tidb-feedback") ? "AIMD-adaptation-not-PD-v2-reproduction" : "none");
                if (method.equals("tidb-feedback")) { values.put("intervalTicks", 4); values.put("pendingThreshold", 2); values.put("additiveIncrease", 1. / 32); values.put("multiplicativeDecrease", .5); values.put("minRate", 1. / 64); values.put("maxRate", .5); }
            }
            case "iter-gwin", "single-mcs" -> { values.put("maxJobsPerEndpoint", 1); values.put("weight", "eq7-slack-beta1-gamma1-or-unit"); }
            case "slamig" -> { values.put("maxJobsPerEndpoint", 1); values.put("alpha", 1); values.put("beta", 1); values.put("gamma", 0); values.put("cliqueCover", "exact-maximum-clique-cover"); }
            case "vmbuddies" -> {
                values.put("horizonRounds", 10); values.put("WWS", "remaining-data-estimate"); values.put("reallocateTicks", 4);
                values.put("dirtyEWMAAlpha", .125); values.put("numericSolver", "nested-bisection-32x28"); values.put("cutoverSynchronization", false);
            }
            case "fpta" -> { values.put("epsilon", .2); values.put("paths", "one-path-cluster-source-destination"); values.put("integerBudget", "floor"); values.put("admission", "FIFO-four-slots"); }
            case "camig-selector" -> {
                values.put("metric", "eq17-21-cliques-and-MIS"); values.put("goal", "all-listed-tablets-relocated"); values.put("singleCost", "initial-bytes/node-budget");
                values.put("kappa", 1); values.put("executionScheduler", "mongodb"); values.put("cpuPlacementModel", false);
            }
            default -> throw new IllegalArgumentException("unknown scheduler method");
        }
        return values;
    }
    static String evidenceHash(Map<String, Object> row) {
        var evidence = new TreeMap<>(row);
        // 실행 출처와 실제 측정 시간은 논리 실행의 재현 hash에서 분리한다.
        var result = new TreeMap<String, Object>();
        for (String key : List.of("stage", "scenario", "method", "executionMethod", "variant", "seed", "condition", "inputHash", "workloadHash", "plan", "planHash", "methodConfig", "jobs", "budgets", "phaseEvents", "metrics", "policyCalls", "policyOperations")) result.put(key, evidence.get(key));
        return MovementExperiment.hash(result);
    }
    static void validate(Map<String, Object> row, SchedulerComparisonExperiment.Condition c, String method, String variant, long seed) {
        require(SCHEMA.equals(row.get("resultSchemaVersion")) && "virtual".equals(row.get("timeDomain")) && "completed".equals(row.get("outcome"))
                && "valid".equals(row.get("validationStatus")), "invalid scheduler outcome/schema");
        require(c.scenario().equals(row.get("scenario")) && method.equals(row.get("method")) && variant.equals(row.get("variant"))
                && seed == n(row, "seed") && SchedulerComparisonExperiment.methods(c.scenario()).contains(method)
                && MovementExperiment.VARIANTS.contains(variant), "invalid result identity");
        require((c.scenario().equals("candidate-plan") ? "candidate-plan" : "fixed-plan").equals(row.get("stage")), "wrong comparison stage");
        require(same(c.config(), row.get("condition")) && same(methodConfig(method), row.get("methodConfig")), "condition/method settings mismatch");
        require(MovementExperiment.hash(SchedulerComparisonExperiment.input(c, seed)).equals(row.get("inputHash"))
                && SchedulerComparisonExperiment.workloadHash(c, seed).equals(row.get("workloadHash")), "scheduler input hash mismatch");
        require(evidenceHash(row).equals(row.get("evidenceHash")), "scheduler evidence hash mismatch");
        String execution = c.scenario().equals("candidate-plan") ? "mongodb" : method;
        require(execution.equals(row.get("executionMethod")), "execution scheduler mismatch");
        var plan = objects(row.get("plan")); require(plan.size() == c.keys().size(), "missing planned jobs");
        require(MovementExperiment.hash(plan).equals(row.get("planHash")), "plan hash mismatch");
        require(same(plan, SchedulerComparisonExperiment.changes(SchedulerComparisonExperiment.planFor(c, method))), "plan selection mismatch");
        var jobs = objects(row.get("jobs")); var budgets = objects(row.get("budgets")); var events = objects(row.get("phaseEvents"));
        require(jobs.size() == c.keys().size() && !budgets.isEmpty() && budgets.size() <= SchedulerComparisonExperiment.MAX_TICKS, "incomplete job/budget observations");
        long[] sent = new long[jobs.size()];
        var ids = new HashSet<Long>();
        for (var j : jobs) {
            long id = n(j, "job"); require(id >= 0 && id < jobs.size() && ids.add(id), "duplicate/invalid job");
            var p = plan.get((int) id); long start = n(j, "startTick"), commit = n(j, "commitTick"), end = n(j, "endTick");
            require(n(p, "tablet") == id && n(j, "source") == n(p, "source") && n(j, "destination") == n(p, "destination")
                    && n(j, "deadline") == c.deadlines().get((int) id), "job plan/deadline mismatch");
            require(start >= 0 && start < commit && commit < end && end <= budgets.size() && "DONE".equals(j.get("status"))
                    && Boolean.TRUE.equals(j.get("exactMatch")) && n(j, "sourceRecords") == 0
                    && j.get("oracleHash") instanceof String hash && hash.matches("[0-9a-f]{64}") && hash.equals(j.get("destinationHash")), "job ownership/data completion mismatch");
            for (String key : List.of("sentBytes", "blockedTicks", "writes", "acceptedWrites", "writeBusy", "reads", "readBusy", "rounds")) require(n(j, key) >= 0, "negative job counter");
            require(n(j, "writes") == n(j, "acceptedWrites") + n(j, "writeBusy") && n(j, "readBusy") <= n(j, "reads"), "request accounting mismatch");
            var history = events.stream().filter(e -> n(e, "job") == id).toList();
            require(!history.isEmpty() && "DONE".equals(history.getLast().get("state")) && n(history.getLast(), "tick") == end
                    && n(history.getLast(), "sentBytes") == n(j, "sentBytes") && n(history.getLast(), "round") == n(j, "rounds"), "missing terminal phase event");
            long lastTick = start, lastBytes = 0, lastRound = 0;
            for (var e : history) {
                require(n(e, "tick") > lastTick && n(e, "tick") <= end && n(e, "sentBytes") >= lastBytes && n(e, "sentBytes") <= n(j, "sentBytes")
                        && n(e, "round") >= lastRound, "invalid phase order");
                lastTick = n(e, "tick"); lastBytes = n(e, "sentBytes"); lastRound = n(e, "round");
            }
        }
        Map<Long, Double> tokens = new HashMap<>(), rates = new HashMap<>();
        for (var p : plan) for (String endpoint : List.of("source", "destination")) { tokens.put(n(p, endpoint), 1.); rates.put(n(p, endpoint), 1. / 12); }
        for (int t = 0; t < budgets.size(); t++) {
            var b = budgets.get(t); require(n(b, "tick") == t, "missing/out-of-order budget tick");
            var grants = objects(b.get("grants")); var active = new HashSet<Long>(); Map<Long, long[]> nodes = new HashMap<>(); long allocated = 0, consumed = 0;
            long time = t;
            var expectedActive = jobs.stream().filter(j -> n(j, "startTick") <= time && time < n(j, "endTick")).toList();
            require(grants.size() == expectedActive.size() && grants.size() <= 4, "concurrency/missing service mismatch");
            for (var g : grants) {
                long id = n(g, "job"); require(id >= 0 && id < jobs.size() && active.add(id), "duplicate budget job");
                var j = jobs.stream().filter(job -> n(job, "job") == id).findFirst().orElseThrow();
                long grant = n(g, "allocatedBytes"), used = n(g, "consumedBytes");
                require(n(j, "startTick") <= t && t < n(j, "endTick") && grant >= 0 && used >= 0 && used <= grant, "invalid lifetime/grant");
                allocated += grant; consumed += used; sent[(int) id] += used;
                for (String endpoint : List.of("source", "destination")) { var node = nodes.computeIfAbsent(n(j, endpoint), ignored -> new long[3]); node[0] += grant; node[1] += used; node[2]++; }
            }
            require(allocated <= c.nodeBudget() * 4 && consumed <= allocated, "cluster budget exceeded");
            int cap = HARD_ENDPOINT.contains(execution) ? 1 : 4;
            require(nodes.values().stream().allMatch(a -> a[0] <= c.nodeBudget() && a[1] <= a[0] && a[2] <= cap), "node budget/concurrency exceeded");
            if (execution.startsWith("tidb-")) {
                if (t > 0) for (long node : tokens.keySet()) {
                    long pending = jobs.stream().filter(j -> n(j, "startTick") < time && time < n(j, "endTick")
                            && (n(j, "source") == node || n(j, "destination") == node)).count();
                    if (execution.equals("tidb-feedback") && t % 4 == 0) rates.put(node, pending >= 2 ? Math.max(1. / 64, rates.get(node) * .5) : Math.min(.5, rates.get(node) + 1. / 32));
                    tokens.put(node, Math.min(4., tokens.get(node) + rates.get(node)));
                }
                for (var j : jobs) if (n(j, "startTick") == t) for (String endpoint : List.of("source", "destination")) {
                    long node = n(j, endpoint); require(tokens.get(node) >= 1. - 1e-9, "store token overspend"); tokens.put(node, tokens.get(node) - 1.);
                }
            }
        }
        for (var j : jobs) require(n(j, "sentBytes") == sent[(int) n(j, "job")], "transfer byte total mismatch");
        require(jobs.stream().mapToLong(j -> n(j, "endTick")).max().orElseThrow() == budgets.size(), "extra budget ticks");
        require(events.stream().allMatch(e -> n(e, "job") >= 0 && n(e, "job") < jobs.size()), "unknown phase job");
        require(same(SchedulerComparisonExperiment.metrics(jobs, budgets.size(), c), row.get("metrics")), "derived scheduler metrics mismatch");
        require(n(row, "plannerWallNanos") >= 0 && n(row, "policyCalls") >= 0 && n(row, "policyOperations") >= 0, "invalid planner observations");
    }

    static void regenerate(Path folder, String profile) throws IOException {
        int count = expected(profile); var unique = new HashSet<String>(); var executions = new HashSet<String>();
        Map<String, String> pairedInput = new HashMap<>(); List<Map<String, Object>> rows = new ArrayList<>(); String provenance = null;
        // 큰 budget 원본은 한 번에 한 run만 읽고 검증한다. 요약에는 scalar만 유지한다.
        try (BufferedWriter jobs = writer(folder, "jobs.csv", "scenario,method,variant,seed,job,source,destination,startTick,commitTick,endTick,deadline,sentBytes,blockedTicks,writeBusy,readBusy,rounds,stopReason,status,exactMatch\n");
             BufferedWriter budgets = writer(folder, "budgets.csv", "scenario,method,variant,seed,tick,node,allocatedBytes,consumedBytes\n");
             BufferedWriter phases = writer(folder, "rounds.csv", "scenario,method,variant,seed,job,tick,state,round,sentBytes\n");
             BufferedWriter timing = writer(folder, "timings.csv", "scenario,method,variant,seed,plannerWallNanos,policyCalls,policyOperations\n");
             var paths = Files.list(folder.resolve("results"))) {
            for (Path file : paths.filter(p -> p.toString().endsWith(".json")).sorted().toList()) {
                var row = BaselineJson.object(BaselineJson.decode(Files.readString(file, StandardCharsets.UTF_8)));
                String scenario = (String) row.get("scenario"), method = (String) row.get("method"), variant = (String) row.get("variant");
                long seed = n(row, "seed"); require(seed >= 1 && seed <= seeds(profile), "invalid scheduler seed");
                var c = SchedulerComparisonExperiment.condition(scenario, profile);
                String id = scenario + ":" + method + ":" + variant + ":" + seed;
                require(unique.add(id) && row.get("executionId") instanceof String && executions.add((String) row.get("executionId")), "duplicate scheduler result");
                for (String key : List.of("suiteExecutionId", "sourceHash", "backendHash", "runnerHash", "runNumber")) require(row.get(key) != null, "missing provenance");
                String signature = List.of(row.get("suiteExecutionId"), row.get("sourceHash"), row.get("backendHash"), row.get("runnerHash"), row.get("runNumber")).toString();
                if (provenance == null) provenance = signature; require(provenance.equals(signature), "mixed scheduler provenance");
                validate(row, c, method, variant, seed);
                String pair = scenario + ":" + seed;
                String prior = pairedInput.putIfAbsent(pair, (String) row.get("inputHash"));
                require(prior == null || prior.equals(row.get("inputHash")), "unpaired algorithm input");
                details(row, jobs, budgets, phases, timing);
                var summary = new LinkedHashMap<>(row);
                summary.remove("jobs"); summary.remove("budgets"); summary.remove("phaseEvents"); summary.remove("modelConfig"); summary.remove("configSnapshot");
                rows.add(summary);
            }
        }
        require(rows.size() == count, "missing scheduler results: " + rows.size() + "/" + count);
        writeReport(folder, rows, profile);
    }
    private static void details(Map<String, Object> row, BufferedWriter jobs, BufferedWriter budgets, BufferedWriter phases, BufferedWriter timing) throws IOException {
        String prefix = row.get("scenario") + "," + row.get("method") + "," + row.get("variant") + "," + row.get("seed") + ",";
        for (var j : objects(row.get("jobs"))) jobs.write(prefix + join(j, List.of("job", "source", "destination", "startTick", "commitTick", "endTick", "deadline", "sentBytes", "blockedTicks", "writeBusy", "readBusy", "rounds", "stopReason", "status", "exactMatch")) + "\n");
        var plan = objects(row.get("plan"));
        for (var b : objects(row.get("budgets"))) {
            Map<Long, long[]> nodes = new TreeMap<>();
            for (var g : objects(b.get("grants"))) for (String endpoint : List.of("source", "destination")) {
                var p = plan.get((int) n(g, "job")); var node = nodes.computeIfAbsent(n(p, endpoint), ignored -> new long[2]); node[0] += n(g, "allocatedBytes"); node[1] += n(g, "consumedBytes");
            }
            for (var entry : nodes.entrySet()) budgets.write(prefix + b.get("tick") + "," + entry.getKey() + "," + entry.getValue()[0] + "," + entry.getValue()[1] + "\n");
        }
        for (var e : objects(row.get("phaseEvents"))) phases.write(prefix + join(e, List.of("job", "tick", "state", "round", "sentBytes")) + "\n");
        timing.write(prefix + row.get("plannerWallNanos") + "," + row.get("policyCalls") + "," + row.get("policyOperations") + "\n");
    }
    private static void writeReport(Path folder, List<Map<String, Object>> rows, String profile) throws IOException {
        StringBuilder csv = new StringBuilder("stage,scenario,variant,method,seeds,makespan_mean_ticks,makespan_sample_sd,mean_completion_ticks,wait_p99_ticks,sent_bytes,initial_transfer_ratio,blocked_job_ticks,write_busy,read_busy,deadline_misses\n");
        StringBuilder md = new StringBuilder("# 선행 Scheduler 비교\n\nprofile=" + profile + "; " + rows.size() + "회 × 12 jobs. 실제 Java Stop/Pre/Post-copy 프로토콜을 endpoint-only 다중 노드 인메모리 fixture에서 실행했다. 시간 단위는 논리 tick이다.\n\n");
        md.append("원 논문 시스템·MongoDB·TiDB 제품 자체의 성능 재현이 아니라 아래 핵심 알고리즘을 이 환경에 옮긴 연구 구현이다. CAMIG는 같은 후보 집합에서 목적지를 고르는 별도 비교이며 고정 계획 Scheduler 순위에 섞지 않는다. methodConfig와 상위 README의 원문 대비 변경표를 함께 읽는다. 평균±SD는 seed 간 표본 표준편차다.\n");
        for (String scenario : SchedulerComparisonExperiment.SCENARIOS) {
            md.append("\n## ").append(scenario).append("\n");
            for (String variant : MovementExperiment.VARIANTS) {
                md.append("\n### ").append(variant).append("\n\n| 방법 | 전체 완료 tick 평균±SD | job 완료 tick 평균 | 대기 p99 tick | 초기 대비 전송 비율 | write BUSY | read BUSY | 기한 초과 jobs |\n|---|---:|---:|---:|---:|---:|---:|---:|\n");
                for (String method : SchedulerComparisonExperiment.methods(scenario)) {
                    var selected = rows.stream().filter(r -> scenario.equals(r.get("scenario")) && variant.equals(r.get("variant")) && method.equals(r.get("method"))).toList();
                    require(selected.size() == seeds(profile), "missing comparison pair");
                    double mean = avg(selected, "makespan_ticks");
                    double sd = Math.sqrt(selected.stream().mapToDouble(r -> Math.pow(metric(r, "makespan_ticks") - mean, 2)).sum() / (selected.size() - 1));
                    csv.append(selected.getFirst().get("stage")).append(',').append(scenario).append(',').append(variant).append(',').append(method).append(',').append(selected.size()).append(',')
                            .append(fmt(mean)).append(',').append(fmt(sd));
                    for (String m : List.of("mean_completion_ticks", "wait_p99_ticks", "sent_bytes", "initial_transfer_ratio", "blockedTicks", "writeBusy", "readBusy", "deadline_misses")) csv.append(',').append(fmt(avg(selected, m)));
                    csv.append('\n');
                    md.append("| ").append(method).append(" | ").append(fmt(mean)).append(" ± ").append(fmt(sd));
                    for (String m : List.of("mean_completion_ticks", "wait_p99_ticks", "initial_transfer_ratio", "writeBusy", "readBusy", "deadline_misses")) md.append(" | ").append(fmt(avg(selected, m)));
                    md.append(" |\n");
                }
            }
        }
        md.append("\n## 검증·지표 범위\n\n총 ").append(rows.size() * 12L).append("개 job의 exact match·단일 소유권/epoch 전환·source 정리·같은 source의 guard 보존을 실행 시 검증했다. JSON에서 job lifecycle·전송 bytes·매 tick 전체/노드 예산·동시 상한·TiDB 변형의 token 소비·최종 phase·집계를 다시 검증했다. 위반 0, 모든 job DONE이다.\n\n");
        md.append("동일 scenario/seed의 초기 데이터·외부 요청 생성 규격·후보 계획 hash를 모든 방법/복사 방식에 공통 적용했다. 성공 요청 및 종료 시각은 방식마다 다르므로 각자의 성공 요청 oracle로 비교한다. 쓰기율을 실제 배분 예산에 맞춰 낮추지 않는다. Post-copy의 freeze/commit은 같은 controller step이어서 write 차단 tick이 0일 수 있고, 실제 downtime 0이라는 뜻은 아니다. 전송 bytes는 protocol payload/header 기준이며 ACK/control/TCP·SSD 비용은 제외한다.\n\n");
        md.append("대기 p99는 run 내부 12 job 시작 대기의 nearest-rank 분위수 후 seed 평균이다. 평균 job 완료 시간은 입력 시각 0부터 각 DONE까지의 평균이다. deadline scenario의 기한만 유효하며 나머지는 10000 tick의 비긴급 조건이다. plannerWallNanos는 fixture 적재를 제외한 한 JVM 안의 관측으로 warm-up/하드웨어가 통제된 CPU 벤치마크가 아니다. timings.csv에 보존하되 결정적 summary/results 및 evidenceHash에는 포함하지 않는다.\n\n");
        md.append("정식 진입점은 공통 Runner config이다. JSON `results/*.json`이 원본이며 jobs.csv/budgets.csv/rounds.csv/paired.csv/timings.csv는 파생 자료다. Git에는 정식 반복의 summary.csv/results.md만 보관하고 원본·상세 자료·manifest는 로컬에 남긴다.\n");
        Files.writeString(folder.resolve("summary.csv"), "\ufeff" + csv, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("results.md"), md.toString(), StandardCharsets.UTF_8);
        StringBuilder paired = new StringBuilder("scenario,variant,seed,method,baseline,makespan_ratio,write_busy_delta,read_busy_delta,deadline_miss_delta,inputHash,planHash\n");
        for (var row : rows) {
            String baseline = row.get("stage").equals("candidate-plan") ? "candidate-balanced" : "fifo";
            var base = rows.stream().filter(r -> r.get("scenario").equals(row.get("scenario")) && r.get("variant").equals(row.get("variant"))
                    && n(r, "seed") == n(row, "seed") && baseline.equals(r.get("method"))).findFirst().orElseThrow();
            paired.append(row.get("scenario")).append(',').append(row.get("variant")).append(',').append(row.get("seed")).append(',').append(row.get("method")).append(',').append(baseline)
                    .append(',').append(fmt(metric(row, "makespan_ticks") / metric(base, "makespan_ticks")));
            for (String m : List.of("writeBusy", "readBusy", "deadline_misses")) paired.append(',').append(fmt(metric(row, m) - metric(base, m)));
            paired.append(',').append(row.get("inputHash")).append(',').append(row.get("planHash")).append('\n');
        }
        Files.writeString(folder.resolve("paired.csv"), "\ufeff" + paired, StandardCharsets.UTF_8);
    }
    private static BufferedWriter writer(Path folder, String name, String header) throws IOException { var writer = Files.newBufferedWriter(folder.resolve(name), StandardCharsets.UTF_8); writer.write("\ufeff" + header); return writer; }
    private static Properties properties(Path file) throws IOException { var p = new Properties(); try (var r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) { p.load(r); } return p; }
    private static boolean same(Object a, Object b) { return MovementExperiment.hash(a).equals(MovementExperiment.hash(b)); }
    private static String join(Map<String, Object> map, List<String> keys) { return String.join(",", keys.stream().map(k -> map.get(k).toString()).toList()); }
    static double metric(Map<String, Object> row, String key) { return ((Number) BaselineJson.object(row.get("metrics")).get(key)).doubleValue(); }
    private static double avg(List<Map<String, Object>> rows, String key) { return rows.stream().mapToDouble(r -> metric(r, key)).average().orElseThrow(); }
    private static String fmt(double n) { return String.format(Locale.ROOT, "%.4f", n); }
    private static long n(Map<String, Object> row, String key) { return MovementResearchMain.integer(row.get(key)); }
    private static List<Map<String, Object>> objects(Object value) { return MovementResearchMain.objects(value); }
    private static void require(boolean condition, String message) { MovementExperiment.require(condition, message); }
    private SchedulerResearchMain() {}
}
