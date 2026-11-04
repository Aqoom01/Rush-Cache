package io.openkache.core.migration.experiment;

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

/** M1~M5 공통 Runner backend. JSON 검증을 마친 뒤에만 요약·완료 표시를 생성한다. */
public final class MovementResearchMain {
    static final String SCHEMA = "b3-movement-json-1";
    static int seeds(String profile) { MovementExperiment.cases(profile); return profile.equals("full") ? 5 : 2; }
    static int expected(String profile) { return MovementExperiment.cases(profile).size() * seeds(profile); }

    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) {
            Path folder = Path.of(args[1]).toAbsolutePath().normalize();
            Properties complete = properties(folder.resolve("completion.properties"));
            require(SCHEMA.equals(complete.getProperty("resultSchemaVersion")), "movement schema mismatch");
            regenerate(folder, complete.getProperty("profile"));
            return;
        }
        if (args.length != 4) throw new IllegalArgumentException("<출력 폴더> <full|quick> <반복 번호> <전체 반복 수>");
        String profile = args[1];
        int repeat = Integer.parseInt(args[2]), repeats = Integer.parseInt(args[3]);
        require(repeat > 0 && repeat <= repeats, "invalid repeat");
        Path folder = Path.of(args[0]).toAbsolutePath().normalize();
        var context = BaselineResearchMain.context(properties(folder.resolve("launch.properties")), repeat, repeats);
        context.put("storageLayer", "multi-node-in-memory-protocol-fixture");
        context.put("modelConfig", Map.of("keyBytes", 16, "valueBytes", 128, "recordBytes", 152,
                "generatorVersion", MovementExperiment.GENERATOR, "timeUnit", "tick"));
        Path originals = Files.createDirectory(folder.resolve("results"));
        for (var c : MovementExperiment.cases(profile)) {
            for (int seed = 1; seed <= seeds(profile); seed++) {
                var root = result(c, seed, MovementExperiment.run(c, seed));
                var full = new LinkedHashMap<>(context);
                full.putAll(root);
                full.put("executionId", context.get("suiteExecutionId") + "-repeat" + repeat + "-" + c.id() + "-seed" + seed);
                Files.writeString(originals.resolve(c.id() + "-seed" + seed + ".json"), BaselineJson.encode(full) + "\n", StandardCharsets.UTF_8);
            }
            System.out.println(c.id() + ": seed " + seeds(profile) + "개 완료");
        }
        regenerate(folder, profile);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion=" + SCHEMA
                + "\ncases=" + expected(profile) + "\nprofile=" + profile + "\nviolations=0\n", StandardCharsets.UTF_8);
    }

    static Map<String, Object> result(MovementExperiment.Case c, long seed, MovementExperiment.Run run) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("resultSchemaVersion", SCHEMA); root.put("study", "b3-live-migration"); root.put("scenario", c.scenario());
        root.put("caseId", c.id()); root.put("seed", seed); root.put("variant", c.variant()); root.put("condition", c.config());
        root.put("timeDomain", "virtual"); root.put("outcome", "completed"); root.put("validationStatus", "valid");
        root.put("nodeCount", nodeCount(c));
        root.put("changes", changes(c)); root.put("workloadHash", MovementExperiment.workloadHash(c, seed));
        root.put("inputHashKind", "change-plan-and-generator-specification-sha256");
        root.put("inputHash", inputHash(c, seed)); root.put("metrics", run.metrics()); root.put("jobs", run.jobs());
        root.put("budgets", run.budgets()); root.put("phaseEvents", run.events());
        root.put("evidenceHash", MovementExperiment.hash(evidence(root)));
        return root;
    }

    static List<Map<String, Object>> changes(MovementExperiment.Case c) {
        List<Map<String, Object>> changes = new ArrayList<>();
        for (int i = 0; i < c.jobs(); i++) changes.add(Map.of("job", i, "tablet", i,
                "source", c.endpoints().equals("shared-source") ? 1 : i + 1,
                "destination", c.endpoints().equals("shared-destination") ? c.jobs() + 1 : c.jobs() + i + 1));
        return changes;
    }
    private static String inputHash(MovementExperiment.Case c, long seed) {
        return MovementExperiment.hash(Map.of("condition", c.config(), "changes", changes(c), "workloadHash", MovementExperiment.workloadHash(c, seed)));
    }
    private static long nodeCount(MovementExperiment.Case c) {
        return changes(c).stream().flatMap(j -> List.of(j.get("source"), j.get("destination")).stream()).distinct().count();
    }
    private static Map<String, Object> evidence(Map<String, Object> root) {
        Map<String, Object> result = new TreeMap<>();
        for (String key : List.of("condition", "changes", "seed", "metrics", "jobs", "budgets", "phaseEvents")) result.put(key, root.get(key));
        return result;
    }
    private static Properties properties(Path file) throws IOException {
        var result = new Properties();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) { result.load(reader); }
        return result;
    }

    static void regenerate(Path folder, String profile) throws IOException {
        var cases = MovementExperiment.cases(profile);
        Map<String, MovementExperiment.Case> conditions = new HashMap<>();
        cases.forEach(c -> conditions.put(c.id(), c));
        var unique = new HashSet<String>(); var ids = new HashSet<String>();
        List<Map<String, Object>> rows = new ArrayList<>();
        String provenance = null;
        try (var paths = Files.list(folder.resolve("results"))) {
            for (Path file : paths.filter(p -> p.toString().endsWith(".json")).sorted().toList()) {
                var row = BaselineJson.object(BaselineJson.decode(Files.readString(file, StandardCharsets.UTF_8)));
                var c = conditions.get(row.get("caseId"));
                require(c != null, "unknown movement case");
                long seed = integer(row.get("seed"));
                require(seed >= 1 && seed <= seeds(profile) && unique.add(c.id() + ":" + seed), "invalid/duplicate movement seed");
                require(row.get("executionId") instanceof String && ids.add((String) row.get("executionId")), "duplicate execution ID");
                for (String key : List.of("sourceHash", "backendHash", "runnerHash", "suiteExecutionId", "runNumber")) require(row.get(key) != null, "missing provenance: " + key);
                String signature = List.of(row.get("sourceHash"), row.get("backendHash"), row.get("runnerHash"), row.get("suiteExecutionId"), row.get("runNumber")).toString();
                if (provenance == null) provenance = signature;
                require(provenance.equals(signature), "mixed movement provenance");
                validate(row, c, seed);
                rows.add(row);
            }
        }
        require(rows.size() == expected(profile), "missing movement results: " + rows.size());
        writeReports(folder, rows, cases, profile);
    }

    static void validate(Map<String, Object> row, MovementExperiment.Case c, long seed) {
        require(SCHEMA.equals(row.get("resultSchemaVersion")) && "virtual".equals(row.get("timeDomain"))
                && "completed".equals(row.get("outcome")) && "valid".equals(row.get("validationStatus")), "invalid movement outcome/schema");
        require(c.id().equals(row.get("caseId")) && c.variant().equals(row.get("variant")) && c.scenario().equals(row.get("scenario")), "case identity mismatch");
        require(integer(row.get("nodeCount")) == nodeCount(c), "node count mismatch");
        require(MovementExperiment.hash(c.config()).equals(MovementExperiment.hash(row.get("condition")))
                && MovementExperiment.hash(changes(c)).equals(MovementExperiment.hash(row.get("changes"))), "condition/plan mismatch");
        require(MovementExperiment.workloadHash(c, seed).equals(row.get("workloadHash")) && inputHash(c, seed).equals(row.get("inputHash")), "input hash mismatch");
        require(MovementExperiment.hash(evidence(row)).equals(row.get("evidenceHash")), "evidence hash mismatch");
        for (var value : BaselineJson.object(row.get("metrics")).values()) {
            var observation = BaselineJson.object(value);
            require(observation.get("value") instanceof Number number && Double.isFinite(number.doubleValue()) && number.doubleValue() >= 0
                    && observation.get("unit") instanceof String, "invalid movement metric");
        }
        var jobs = objects(row.get("jobs")); var budgets = objects(row.get("budgets"));
        require(jobs.size() == c.jobs() && !budgets.isEmpty(), "missing job/budget rows");
        long[] sent = new long[c.jobs()]; long finalBytes = 0, blocked = 0, writes = 0, writeBusy = 0, reads = 0, readBusy = 0;
        List<Long> waits = new ArrayList<>();
        var seen = new HashSet<Long>();
        for (var j : jobs) {
            long index = integer(j.get("job"));
            require(index >= 0 && index < c.jobs() && seen.add(index), "duplicate job");
            var plan = changes(c).get((int) index);
            for (String key : List.of("tablet", "source", "destination")) require(integer(j.get(key)) == integer(plan.get(key)), "changed job plan");
            long start = integer(j.get("startTick")), commit = integer(j.get("commitTick")), end = integer(j.get("endTick"));
            require(start >= 0 && start < commit && commit < end && end <= budgets.size()
                    && "DONE".equals(j.get("status")) && Boolean.TRUE.equals(j.get("exactMatch")) && integer(j.get("sourceRecords")) == 0
                    && j.get("oracleHash") instanceof String hash && hash.matches("[0-9a-f]{64}") && hash.equals(j.get("destinationHash")), "job completion/oracle mismatch");
            waits.add(start); finalBytes += integer(j.get("finalBytes")); blocked += integer(j.get("blockedTicks"));
            writes += integer(j.get("writes")); writeBusy += integer(j.get("writeBusy")); reads += integer(j.get("reads")); readBusy += integer(j.get("readBusy"));
            require(integer(j.get("writes")) == integer(j.get("acceptedWrites")) + integer(j.get("writeBusy")), "write accounting mismatch");
        }
        for (int t = 0; t < budgets.size(); t++) {
            var budget = budgets.get(t); require(integer(budget.get("tick")) == t, "missing/out-of-order tick");
            var grants = objects(budget.get("grants")); require(grants.size() <= c.concurrency(), "concurrency exceeded");
            var active = new HashSet<Long>(); Map<String, long[]> nodes = new TreeMap<>(); long clusterAllocated = 0, clusterUsed = 0;
            for (var grant : grants) {
                long index = integer(grant.get("job"));
                require(index >= 0 && index < c.jobs() && active.add(index), "duplicate budget job");
                var job = jobs.stream().filter(j -> integer(j.get("job")) == index).findFirst().orElseThrow();
                require(integer(job.get("startTick")) <= t && t < integer(job.get("endTick")), "service outside job lifetime");
                long allocated = integer(grant.get("allocatedBytes")), used = integer(grant.get("consumedBytes"));
                require(allocated >= 0 && used >= 0 && used <= allocated, "overspent job budget");
                clusterAllocated += allocated; clusterUsed += used; sent[(int) index] += used;
                for (String endpoint : List.of("source", "destination")) {
                    long[] node = nodes.computeIfAbsent(Long.toString(integer(job.get(endpoint))), ignored -> new long[3]);
                    node[0] += allocated; node[1] += used; node[2]++;
                }
            }
            long expectedActive = jobs.stream().filter(j -> integer(j.get("startTick")) <= integer(budget.get("tick"))
                    && integer(budget.get("tick")) < integer(j.get("endTick"))).count();
            require(active.size() == expectedActive && clusterAllocated <= c.clusterBudget() && clusterUsed <= c.clusterBudget(), "cluster budget/admission mismatch");
            var recorded = BaselineJson.object(budget.get("nodes")); require(recorded.size() == nodes.size(), "missing node budget");
            nodes.forEach((id, n) -> {
                require(n[0] <= c.nodeBudget() && n[1] <= c.nodeBudget() && n[2] <= c.endpointLimit(), "node budget/concurrency exceeded");
                var observed = BaselineJson.object(recorded.get(id));
                require(integer(observed.get("allocatedBytes")) == n[0] && integer(observed.get("consumedBytes")) == n[1], "node accounting mismatch");
            });
        }
        for (var j : jobs) require(integer(j.get("sentBytes")) == sent[(int) integer(j.get("job"))], "missing transfer bytes");
        long total = java.util.Arrays.stream(sent).sum();
        equal(row, "makespan_ticks", budgets.size()); equal(row, "sent_bytes", total); equal(row, "final_data_bytes", finalBytes);
        equal(row, "initial_data_bytes", (long) c.jobs() * c.keys() * 152); equal(row, "aggregate_bytes_per_tick", total / (double) budgets.size());
        equal(row, "useful_bytes_per_tick", finalBytes / (double) budgets.size());
        equal(row, "cluster_budget_utilization", total / ((double) budgets.size() * c.clusterBudget()));
        equal(row, "initial_transfer_ratio", total / (double) ((long) c.jobs() * c.keys() * 152));
        equal(row, "final_transfer_ratio", total / (double) finalBytes);
        for (double q : new double[] {.5, .95, .99, 1.}) equal(row, "wait_p" + (int) (q * 100) + "_ticks", MovementExperiment.percentile(waits, q));
        equal(row, "blocked_job_ticks", blocked); equal(row, "writes_attempted", writes); equal(row, "writes_busy", writeBusy);
        equal(row, "reads_attempted", reads); equal(row, "reads_busy", readBusy); equal(row, "read_busy_ratio", readBusy / (double) reads);
        equal(row, "completion_ratio", 1); equal(row, "exact_comparisons", c.jobs()); equal(row, "invariant_violations", 0); equal(row, "starvation_jobs", 0);
        equal(row, "min_job_sent_bytes", java.util.Arrays.stream(sent).min().orElseThrow());
        equal(row, "max_job_sent_bytes", java.util.Arrays.stream(sent).max().orElseThrow());
        require(jobs.stream().mapToLong(j -> integer(j.get("endTick"))).max().orElseThrow() == budgets.size(), "extra empty ticks");
        var events = objects(row.get("phaseEvents"));
        for (var j : jobs) {
            long index = integer(j.get("job"));
            var history = events.stream().filter(e -> integer(e.get("job")) == index).toList();
            require(!history.isEmpty(), "missing job events");
            long last = 0, bytes = 0, round = 0;
            for (var e : history) {
                long t = integer(e.get("tick"));
                require(t > last && t > integer(j.get("startTick")) && t <= integer(j.get("endTick"))
                        && integer(e.get("sentBytes")) >= bytes && integer(e.get("sentBytes")) <= integer(j.get("sentBytes"))
                        && integer(e.get("round")) >= round, "invalid phase event");
                last = t; bytes = integer(e.get("sentBytes")); round = integer(e.get("round"));
            }
            require("DONE".equals(history.getLast().get("state")) && last == integer(j.get("endTick"))
                    && bytes == integer(j.get("sentBytes")) && round == integer(j.get("rounds")), "missing terminal event");
        }
        require(events.stream().allMatch(e -> integer(e.get("job")) >= 0 && integer(e.get("job")) < c.jobs()), "unknown event job");
    }

    private static void writeReports(Path folder, List<Map<String, Object>> rows, List<MovementExperiment.Case> cases, String profile) throws IOException {
        StringBuilder summary = new StringBuilder("시나리오,조건,방식,job수,동시상한,노드동시상한,endpoint,rho,seed수,완료tick평균,완료tick표준편차,전송bytes평균,전송bytes매tick평균,유효bytes매tick평균,대기p99tick평균,차단jobtick평균,쓰기BUSY평균,읽기BUSY평균,초기대비전송비율평균,미시작job수,정합성위반수\n");
        StringBuilder report = new StringBuilder("# 다중 이동 M1~M5 결과\n\nprofile=" + profile + "; 42조건 × seed " + seeds(profile) + "개 = " + rows.size() + "회. 모든 행은 이 반복의 JSON 원본에서 재생성했다.\n\n");
        report.append("실제 Stop/Pre/Post-copy Java 프로토콜을 다중 노드 메모리 저장소·공유 라우팅 CAS에서 실행했다. 시간 단위는 논리 tick이며 TCP·SSD·실제 지연시간 측정이 아니다. 평균±표본 표준편차는 seed 간 변동이며 반복 실행 오차와 구별한다.\n\n");
        String previous = "";
        for (var c : cases) {
            var selected = rows.stream().filter(r -> c.id().equals(r.get("caseId"))).toList();
            double mean = mean(selected, "makespan_ticks"), sd = deviation(selected, "makespan_ticks");
            summary.append(String.join(",", c.scenario(), c.id(), c.variant(), "" + c.jobs(), "" + c.concurrency(), "" + c.endpointLimit(), c.endpoints(), "" + c.rho(), "" + selected.size(),
                    fmt(mean), fmt(sd), fmt(mean(selected, "sent_bytes")), fmt(mean(selected, "aggregate_bytes_per_tick")), fmt(mean(selected, "useful_bytes_per_tick")),
                    fmt(mean(selected, "wait_p99_ticks")), fmt(mean(selected, "blocked_job_ticks")), fmt(mean(selected, "writes_busy")), fmt(mean(selected, "reads_busy")),
                    fmt(mean(selected, "initial_transfer_ratio")), "0", "0")).append('\n');
            if (!previous.equals(c.scenario())) {
                report.append("\n## ").append(c.scenario()).append("\n\n| 조건 | 방식 | 완료 tick 평균±SD | 전송 bytes/tick | 대기 p99 tick | 쓰기 BUSY | 읽기 BUSY | 초기 대비 전송 비율 |\n|---|---|---:|---:|---:|---:|---:|---:|\n"); previous = c.scenario();
            }
            report.append("| ").append(c.id()).append(" | ").append(c.variant()).append(" | ").append(fmt(mean)).append(" ± ").append(fmt(sd))
                    .append(" | ").append(fmt(mean(selected, "aggregate_bytes_per_tick"))).append(" | ").append(fmt(mean(selected, "wait_p99_ticks")))
                    .append(" | ").append(fmt(mean(selected, "writes_busy"))).append(" | ").append(fmt(mean(selected, "reads_busy")))
                    .append(" | ").append(fmt(mean(selected, "initial_transfer_ratio"))).append(" |\n");
        }
        long jobs = rows.stream().mapToLong(r -> objects(r.get("jobs")).size()).sum();
        report.append("\n## 검증·해석 범위\n\n총 ").append(jobs).append("개 job 모두 DONE·최종 데이터 exact match·이전 source 정리·owner/epoch 전환을 확인했다. 매 tick의 전체/노드별 할당·실소비·동시 상한 검증과 JSON 재검증에서 위반 0, 미시작 job 0이다. 대기 p99는 run 내부 job 시작 대기의 nearest-rank 분위수를 구한 뒤 seed 평균을 냈다. 처리량은 전송 bytes/전체 완료 tick이다.\n\n");
        report.append("M1~M3는 Stop-copy, M4는 dirty 변화가 드러나는 Pre-copy를 기준으로 삼았다. M5가 M2/M4의 같은 조건을 세 방식 모두로 반복한다. rho는 job당 고정 reference bytes/tick으로 생성한 쓰기 시도량이며 실제 할당량으로 낮추지 않는다. SET 70%·DELETE 30%, job/tick마다 GET 2개를 시도한다. 대기/완료한 job에도 suite 종료까지 같은 생성 규격을 적용한다. BUSY와 완료 시각 때문에 성공 요청·최종 데이터량은 방식마다 다르며 각자의 성공 요청 oracle로 비교했다. workloadHash는 전체 trace 파일이 아닌 생성 규격의 hash다.\n\n");
        report.append("FIFO는 실행 가능한 가장 오래된 job을 고른다. 공유 endpoint는 송수신 합산 예산, 전체 예산은 전송 1 byte당 한 번 차감한다. 회전 progressive filling으로 나누고 미사용 예산은 같은 tick에 재분배하거나 다음 tick으로 이월하지 않는다. ACK/control/실제 network framing은 기존 protocol byte 모델 밖이다. queued starvation=0은 이번 유한 입력의 관측이며 지속 도착·장애 환경의 보장이 아니다.\n\n");
        report.append("원본: `results/*.json` (`").append(SCHEMA).append("`). 상세 파생 파일: `cases.csv`, `runs.csv`, `jobs.csv`, `budgets.csv`, `rounds.csv`. Git에는 반복별 summary.csv/results.md만 보관한다.\n");
        write(folder.resolve("summary.csv"), "\ufeff" + summary); write(folder.resolve("results.md"), report.toString());
        StringBuilder runs = new StringBuilder("case,seed,variant,inputHash,workloadHash,makespanTicks,sentBytes\n");
        StringBuilder jobCsv = new StringBuilder("case,seed,job,tablet,source,destination,startTick,commitTick,endTick,sentBytes,blockedTicks,writes,acceptedWrites,writeBusy,reads,readBusy,rounds,stopReason,status,exactMatch\n");
        StringBuilder budgetCsv = new StringBuilder("case,seed,tick,node,allocatedBytes,consumedBytes\n");
        StringBuilder roundCsv = new StringBuilder("case,seed,job,tick,state,round,sentBytes\n");
        for (var row : rows) {
            String prefix = row.get("caseId") + "," + row.get("seed") + ",";
            runs.append(prefix).append(row.get("variant")).append(',').append(row.get("inputHash")).append(',').append(row.get("workloadHash")).append(',')
                    .append(fmt(metric(row, "makespan_ticks"))).append(',').append(fmt(metric(row, "sent_bytes"))).append('\n');
            for (var j : objects(row.get("jobs"))) {
                jobCsv.append(prefix);
                jobCsv.append(String.join(",", List.of("job", "tablet", "source", "destination", "startTick", "commitTick", "endTick", "sentBytes", "blockedTicks", "writes", "acceptedWrites", "writeBusy", "reads", "readBusy", "rounds", "stopReason", "status", "exactMatch").stream().map(k -> j.get(k).toString()).toList())).append('\n');
            }
            for (var b : objects(row.get("budgets"))) for (var node : BaselineJson.object(b.get("nodes")).entrySet()) {
                var n = BaselineJson.object(node.getValue());
                budgetCsv.append(prefix).append(b.get("tick")).append(',').append(node.getKey()).append(',').append(n.get("allocatedBytes")).append(',').append(n.get("consumedBytes")).append('\n');
            }
            for (var e : objects(row.get("phaseEvents"))) roundCsv.append(prefix).append(e.get("job")).append(',').append(e.get("tick")).append(',').append(e.get("state")).append(',').append(e.get("round")).append(',').append(e.get("sentBytes")).append('\n');
        }
        StringBuilder caseCsv = new StringBuilder("case,scenario,configJson,changesHash\n");
        for (var c : cases) caseCsv.append(c.id()).append(',').append(c.scenario()).append(',').append('"').append(BaselineJson.encode(c.config()).replace("\"", "\"\"")).append('"').append(',').append(MovementExperiment.hash(changes(c))).append('\n');
        write(folder.resolve("cases.csv"), "\ufeff" + caseCsv); write(folder.resolve("runs.csv"), "\ufeff" + runs);
        write(folder.resolve("jobs.csv"), "\ufeff" + jobCsv); write(folder.resolve("budgets.csv"), "\ufeff" + budgetCsv); write(folder.resolve("rounds.csv"), "\ufeff" + roundCsv);
    }

    static double metric(Map<String, Object> row, String name) { return ((Number) BaselineJson.object(BaselineJson.object(row.get("metrics")).get(name)).get("value")).doubleValue(); }
    private static double mean(List<Map<String, Object>> rows, String name) { return rows.stream().mapToDouble(r -> metric(r, name)).average().orElseThrow(); }
    private static double deviation(List<Map<String, Object>> rows, String name) {
        double mean = mean(rows, name); return rows.size() < 2 ? 0 : Math.sqrt(rows.stream().mapToDouble(r -> Math.pow(metric(r, name) - mean, 2)).sum() / (rows.size() - 1));
    }
    private static void equal(Map<String, Object> row, String name, double value) { require(Math.abs(metric(row, name) - value) <= Math.max(1e-9, Math.abs(value) * 1e-12), "derived metric mismatch: " + name); }
    private static String fmt(double value) { return String.format(Locale.ROOT, "%.3f", value); }
    static long integer(Object value) { require(value instanceof Number n && Double.isFinite(n.doubleValue()) && n.doubleValue() == n.longValue(), "expected integer"); return ((Number) value).longValue(); }
    static List<Map<String, Object>> objects(Object value) { require(value instanceof List<?>, "expected list"); return ((List<?>) value).stream().map(BaselineJson::object).toList(); }
    private static void write(Path path, String value) throws IOException { Files.writeString(path, value, StandardCharsets.UTF_8); }
    private static void require(boolean condition, String message) { MovementExperiment.require(condition, message); }
    private MovementResearchMain() {}
}
