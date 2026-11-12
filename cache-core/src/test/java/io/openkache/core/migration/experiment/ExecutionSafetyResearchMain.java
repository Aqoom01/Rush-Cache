package io.openkache.core.migration.experiment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** 독립 drain/operator/장애 주입 suite. JSON 검증 후 요약과 상세 journal을 생성한다. */
public final class ExecutionSafetyResearchMain {
    static final String SCHEMA = "b3-execution-safety-json-1";

    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) {
            Path folder = Path.of(args[1]);
            var completion = properties(folder.resolve("completion.properties"));
            require(SCHEMA.equals(completion.getProperty("resultSchemaVersion")), "schema mismatch");
            regenerate(folder, completion.getProperty("profile")); return;
        }
        if (args.length != 4) throw new IllegalArgumentException("<folder> <full|quick> <repeat> <repeats>");
        Path folder = Path.of(args[0]);
        String profile = args[1];
        int repeat = Integer.parseInt(args[2]), repeats = Integer.parseInt(args[3]);
        require(repeat >= 1 && repeat <= repeats, "invalid repeat");
        var context = BaselineResearchMain.context(properties(folder.resolve("launch.properties")), repeat, repeats);
        context.put("storageLayer", "b3-private-multi-node-in-memory-fixture");
        var originals = Files.createDirectory(folder.resolve("results"));
        var cases = ExecutionSafetyExperiment.cases(profile);
        for (int i = 0; i < cases.size(); i++) {
            var root = new LinkedHashMap<>(context);
            var c = cases.get(i);
            root.putAll(ExecutionSafetyExperiment.run(c));
            root.put("resultSchemaVersion", SCHEMA); root.put("caseIndex", i);
            root.put("executionId", context.get("suiteExecutionId") + "-r" + repeat + "-case" + i);
            Files.writeString(originals.resolve(String.format(java.util.Locale.ROOT, "case-%04d.json", i)),
                    BaselineJson.encode(root) + "\n", StandardCharsets.UTF_8);
        }
        regenerate(folder, profile);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion=" + SCHEMA
                + "\nprofile=" + profile + "\ncases=" + cases.size() + "\nviolations=0\n", StandardCharsets.UTF_8);
    }

    private static Properties properties(Path path) throws IOException {
        var result = new Properties();
        try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) { result.load(reader); }
        return result;
    }

    static Map<String, Object> evidence(Map<String, Object> row) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String key : List.of("condition", "resolvedFault", "outcome", "plan", "preflight", "inputHash", "inputHashKind", "metrics", "jobs", "events"))
            result.put(key, row.get(key));
        return result;
    }
    static void validate(Map<String, Object> row, ExecutionSafetyExperiment.Case c) {
        require(SCHEMA.equals(row.get("resultSchemaVersion")), "unknown safety schema");
        require(MovementExperiment.hash(c.config()).equals(MovementExperiment.hash(row.get("condition")))
                && MovementExperiment.hash(c.config()).equals(row.get("inputHash")), "safety input mismatch");
        require(ExecutionSafetyExperiment.resolveFault(c).equals(row.get("resolvedFault"))
                && MovementExperiment.hash(SchedulerComparisonExperiment.changes(ExecutionSafetyExperiment.planFor(c)))
                    .equals(MovementExperiment.hash(row.get("plan"))), "fault/plan mismatch");
        require(MovementExperiment.hash(evidence(row)).equals(row.get("evidenceHash")), "safety evidence mismatch");
        var metrics = BaselineJson.object(row.get("metrics"));
        var preflight = BaselineJson.object(row.get("preflight"));
        long nodeCount = ExecutionSafetyExperiment.planFor(c).stream()
                .flatMap(move -> List.of(move.from(), move.to()).stream()).distinct().count();
        require(n(preflight, "nodesBefore") == nodeCount && n(preflight, "incomingBytes") == (ExecutionSafetyExperiment.JOBS - 1L) * c.keys() * 152
                && n(preflight, "replicaMinimum") == (c.scenario().equals("replica-floor") ? nodeCount : 1),
                "preflight resource metadata mismatch");
        var jobs = MovementResearchMain.objects(row.get("jobs"));
        require(jobs.size() == ExecutionSafetyExperiment.JOBS && n(metrics, "invariant_violations") == 0, "missing safety jobs");
        long published = 0, complete = 0, sent = 0;
        for (int i = 0; i < jobs.size(); i++) {
            var j = jobs.get(i);
            require(n(j, "job") == i && n(j, "sourceRecords") >= 0 && n(j, "sentBytes") >= 0, "invalid job row");
            boolean done = Boolean.TRUE.equals(j.get("complete")), committed = Boolean.TRUE.equals(j.get("published"));
            if (done) {
                require(committed && n(j, "sourceRecords") == 0 && "DONE".equals(j.get("state"))
                        && j.get("oracleHash").equals(j.get("observedHash")), "completed safety data mismatch");
                complete++;
            } else if (!committed) require(j.get("oracleHash").equals(j.get("observedHash")), "source mismatch");
            else require("HELD".equals(row.get("outcome")) && c.variant().equals("post-copy")
                        && "FAILED".equals(j.get("state")) && n(j, "sourceRecords") > 0, "lost published job");
            if (committed) published++;
            sent += n(j, "sentBytes");
        }
        require(sent == n(metrics, "sent_bytes") && published == n(metrics, "ownership_commits"), "safety accounting mismatch");
        String outcome = (String) row.get("outcome");
        switch (outcome) {
            case "REMOVED" -> require(n(metrics, "node_removed") == 1 && n(metrics, "remaining_tablets") == 0
                    && complete == ExecutionSafetyExperiment.JOBS, "unsafe node removal");
            case "COMPLETED" -> require(complete == ExecutionSafetyExperiment.JOBS && n(metrics, "node_removed") == 0, "incomplete operator");
            case "BLOCKED" -> require(n(metrics, "node_removed") == 0 && n(metrics, "remaining_tablets") > 0
                    && n(metrics, "initial_failed_jobs") == 0, "invalid blocked drain");
            case "REJECTED" -> require(published == 0 && n(metrics, "node_removed") == 0 && n(metrics, "initial_jobs") == 0
                    && (n(preflight, "freeDestinationBytes") < n(preflight, "incomingBytes")
                    || n(preflight, "nodesBefore") - 1 < n(preflight, "replicaMinimum")
                    || n(preflight, "unavailableBefore") > 0), "invalid preflight rejection");
            case "HELD" -> require(complete == ExecutionSafetyExperiment.JOBS - 1 && n(metrics, "node_removed") == 0
                    && n(metrics, "initial_failed_jobs") == 1, "failure isolation mismatch");
            default -> throw new IllegalStateException("unknown safety outcome");
        }
        var events = MovementResearchMain.objects(row.get("events"));
        long previousTick = 0;
        for (int i = 0; i < events.size(); i++) {
            var e = events.get(i); long tick = n(e, "tick");
            require(n(e, "order") == i && tick >= previousTick && tick <= n(metrics, "total_ticks"), "invalid event timeline");
            previousTick = tick;
        }
        if (n(metrics, "node_removed") == 1) require("node-removed".equals(events.getLast().get("kind")), "missing remove barrier");
        if (c.scenario().equals("controller-reload")) require(n(metrics, "controller_reloads") == 1, "reload not exercised");
        if (c.scenario().equals("db-primary")) require(n(metrics, "admin_wait_ticks") == 5, "admin drain barrier missing");
    }

    static void regenerate(Path folder, String profile) throws IOException {
        var cases = ExecutionSafetyExperiment.cases(profile);
        var rows = new ArrayList<Map<String, Object>>();
        String provenance = null;
        for (int i = 0; i < cases.size(); i++) {
            var row = BaselineJson.object(BaselineJson.decode(Files.readString(
                    folder.resolve("results").resolve(String.format(java.util.Locale.ROOT, "case-%04d.json", i)), StandardCharsets.UTF_8)));
            require(n(row, "caseIndex") == i, "duplicate/changed safety identity");
            String signature = List.of(row.get("suiteExecutionId"), row.get("sourceHash"), row.get("backendHash"),
                    row.get("runnerHash"), row.get("runNumber")).toString();
            if (provenance == null) provenance = signature;
            require(provenance.equals(signature), "mixed safety provenance");
            validate(row, cases.get(i)); rows.add(row);
        }
        try (var files = Files.list(folder.resolve("results"))) { require(files.count() == cases.size(), "extra safety originals"); }
        StringBuilder csv = new StringBuilder("\ufeffmethod,scenario,variant,seed,outcome,total_ticks,sent_bytes,initial_failed_jobs,healthy_jobs_completed_before_recovery,ownership_commits,remaining_tablets,node_removed,busy,unavailable,controller_reloads,admin_wait_ticks,invariant_violations\n");
        StringBuilder journal = new StringBuilder("\ufeffcase,method,scenario,variant,seed,order,tick,kind,job,detail,routingVersion\n");
        for (int i = 0; i < rows.size(); i++) {
            var r = rows.get(i); var c = cases.get(i); var metrics = BaselineJson.object(r.get("metrics"));
            csv.append(c.method()).append(',').append(c.scenario()).append(',').append(c.variant()).append(',').append(c.seed()).append(',').append(r.get("outcome"));
            for (String key : List.of("total_ticks", "sent_bytes", "initial_failed_jobs", "healthy_jobs_completed_before_recovery",
                    "ownership_commits", "remaining_tablets", "node_removed", "busy", "unavailable", "controller_reloads", "admin_wait_ticks", "invariant_violations"))
                csv.append(',').append(metrics.get(key));
            csv.append('\n');
            for (var e : MovementResearchMain.objects(r.get("events"))) {
                journal.append(i).append(',').append(c.method()).append(',').append(c.scenario()).append(',').append(c.variant()).append(',').append(c.seed());
                for (String key : List.of("order", "tick", "kind", "job", "detail", "routingVersion"))
                    journal.append(',').append('"').append(e.get(key).toString().replace("\"", "\"\"")).append('"');
                journal.append('\n');
            }
        }
        StringBuilder md = new StringBuilder("# 안전한 축소·다중 이동 장애 검증\n\nprofile=" + profile + "; " + rows.size()
                + "회. 실제 세 protocol을 자체 메모리 fixture에서 실행했다. 실서버·원 제품 재현이 아니다.\n\n"
                + "| 방법 | 복사 방식 | 실행 | 완료/제거 | 거부 | 보류 | 미복구 유지 | 위반 |\n|---|---|---:|---:|---:|---:|---:|---:|\n");
        for (String method : List.of("scylla-drain", "mongodb-drain", "pd-operator", "foundationdb-fault"))
            for (String variant : MovementExperiment.VARIANTS) {
                var selected = rows.stream().filter(r -> method.equals(BaselineJson.object(r.get("condition")).get("method"))
                        && variant.equals(BaselineJson.object(r.get("condition")).get("variant"))).toList();
                md.append("| ").append(method).append(" | ").append(variant).append(" | ").append(selected.size()).append(" | ")
                        .append(selected.stream().filter(r -> List.of("REMOVED", "COMPLETED").contains(r.get("outcome"))).count());
                for (String outcome : List.of("REJECTED", "BLOCKED", "HELD"))
                    md.append(" | ").append(selected.stream().filter(r -> outcome.equals(r.get("outcome"))).count());
                md.append(" | 0 |\n");
            }
        md.append("\n진행 중 상태를 완료로 바꾸지 않는다. 노드 제거는 모든 Tablet 이동·정리와 관리 metadata 확인 뒤에만 수행한다. "
                + "실패 전 작업의 bytes도 재시도 전송량에 포함한다. 논리적 노드 불가·정지와 검출 후 abort/fail을 주입하며 실제 TCP·디스크 crash는 재현하지 않는다.\n");
        Files.writeString(folder.resolve("summary.csv"), csv, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("events.csv"), journal, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("results.md"), md, StandardCharsets.UTF_8);
    }
    private static long n(Map<String, Object> row, String key) { return MovementResearchMain.integer(row.get(key)); }
    private static void require(boolean condition, String message) { MovementExperiment.require(condition, message); }
    private ExecutionSafetyResearchMain() {}
}
