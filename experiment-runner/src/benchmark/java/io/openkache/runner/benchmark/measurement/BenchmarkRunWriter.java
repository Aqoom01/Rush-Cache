package io.openkache.runner.benchmark.measurement;

import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import static io.openkache.runner.benchmark.client.ClientOutcome.*;

/**
 * 종료된 단일 JVM run의 표준 result envelope·전수 histogram·계수를 새 디렉터리에 저장한다.
 * ready 분산 manifest/완전 correctness 이력 생성기는 아니다. 저장 실패는 IOException이며 완료 표시는 남기지 않는다.
 * 파일 I/O는 run 종료 후 호출 thread에서만 한다. 동일 디렉터리 재사용/기존 결과 덮어쓰기는 거절한다.
 */
public final class BenchmarkRunWriter {
    private BenchmarkRunWriter() {}
    public record Metadata(String variant, String scenario, int nodeCount, String sourceCommit,
            Boolean sourceDirty, String clockId, Set<String> requiredMetrics, Map<String, String> adapterConfiguration) {
        /** 기존 O00/V01 호출과 호환된다. 전체 adapter 설정은 명시적인 새 overload에서 제공한다. */
        public Metadata(String variant, String scenario, int nodeCount, String sourceCommit,
                Boolean sourceDirty, String clockId, Set<String> requiredMetrics) {
            this(variant, scenario, nodeCount, sourceCommit, sourceDirty, clockId, requiredMetrics, Map.of());
        }
        public Metadata {
            if (Objects.requireNonNull(variant).isBlank() || Objects.requireNonNull(scenario).isBlank()
                    || nodeCount < 1 || Objects.requireNonNull(clockId).isBlank()) {
                throw new IllegalArgumentException("run metadata required");
            }
            TreeSet<String> copy = new TreeSet<>(requiredMetrics);
            for (String id : copy) { if (!id.matches("M(0[1-9]|[12][0-9]|3[0-6])")) { throw new IllegalArgumentException("unknown metric"); } }
            requiredMetrics = Collections.unmodifiableSet(copy);
            adapterConfiguration = Collections.unmodifiableMap(new TreeMap<>(Map.copyOf(adapterConfiguration)));
        }
    }
    public record Export(Path directory, boolean valid, List<String> invalidReasons, String manifestSha256) {
        public Export { invalidReasons = List.copyOf(invalidReasons); }
    }
    private record Row(TracePhase phase, Operation operation, long considered, long admitted,
            long success, long errorResponse, long noResponse, long notAdmitted, long unresolved,
            long attempts, long retriedLogical) {}
    private record View(String runId, String loadSlot, String mode, Object plan, Object execution,
            long elapsed, boolean completed, List<String> issues, List<Row> rows, Object events) {}

    public static Export write(Path directory, Metadata metadata, TraceConfig trace, OpenLoopPlan plan,
            OpenLoopResult result, LatencyCollector.Snapshot latency) throws IOException {
        List<String> issues = new ArrayList<>();
        if (!result.collectionValid()) { issues.add("EXECUTION_COLLECTION_INVALID:" + result.failure()); }
        if (!result.scheduleQualified()) { issues.add("SCHEDULE_NOT_QUALIFIED"); }
        List<Row> rows = new ArrayList<>();
        for (TracePhase phase : TracePhase.values()) {
            var p = result.phases().get(phase);
            if (p == null) { issues.add("PHASE_NOT_RUN:" + phase); continue; }
            long expected = phase == TracePhase.LOAD ? trace.keyCount() : phase == TracePhase.WARMUP ? plan.warmupRequests() : plan.measuredRequests();
            long rate = phase == TracePhase.LOAD ? plan.loadOpsPerSecond() : phase == TracePhase.WARMUP ? plan.warmupOpsPerSecond() : plan.measureOpsPerSecond();
            if (p.schedule().requests() != expected || p.schedule().opsPerSecond() != rate
                    || p.considered() + p.unissued() != expected) { issues.add("PLAN_RESULT_MISMATCH:" + phase); }
            for (Operation op : Operation.values()) {
                var c = p.byOperation().get(op);
                long admitted = p.clientAfter().byOperation().get(op).admitted() - p.clientBefore().byOperation().get(op).admitted();
                rows.add(new Row(phase, op, c.considered(), admitted, c.success(), c.errorResponse(),
                        c.noResponse(), c.capacityDrop() + c.timingMiss() + c.clientRejected() + c.submitFailed(),
                        c.unresolved(), c.attempts(), c.retriedLogical()));
            }
        }
        return write(directory, metadata, trace, latency, new View(plan.runId(), plan.loadSlot(), "open_loop",
                plan, result, result.finishedNanos() - result.startedNanos(), result.completed(), issues, rows, result.events()));
    }
    public static Export write(Path directory, Metadata metadata, TraceConfig trace, ClosedLoopPlan plan,
            ClosedLoopResult result, LatencyCollector.Snapshot latency) throws IOException {
        List<String> issues = new ArrayList<>();
        if (!result.passed()) { issues.add("EXECUTION_FAILED:" + result.failure()); }
        List<Row> rows = new ArrayList<>();
        for (TracePhase phase : TracePhase.values()) {
            var p = result.phases().get(phase);
            if (p == null) { issues.add("PHASE_NOT_RUN:" + phase); continue; }
            long expected = phase == TracePhase.LOAD ? trace.keyCount() : phase == TracePhase.WARMUP ? plan.warmupRequests() : plan.measuredRequests();
            if (p.requested() != expected) { issues.add("PLAN_RESULT_MISMATCH:" + phase); }
            for (Operation op : Operation.values()) {
                var c = p.byOperation().get(op);
                long admitted = p.clientAfter().byOperation().get(op).admitted() - p.clientBefore().byOperation().get(op).admitted();
                rows.add(new Row(phase, op, c.scheduled(), admitted, c.success(), c.errorResponse(),
                        c.noResponse(), c.notAdmitted(), c.unresolved(), c.attempts(), c.retriedLogical()));
            }
        }
        return write(directory, metadata, trace, latency, new View(plan.runId(), plan.loadSlot(), "closed_loop",
                plan, result, result.finishedNanos() - result.startedNanos(), result.passed(), issues, rows, result.events()));
    }
    private static Export write(Path directory, Metadata metadata, TraceConfig trace,
            LatencyCollector.Snapshot latency, View view) throws IOException {
        Objects.requireNonNull(metadata); Objects.requireNonNull(trace); Objects.requireNonNull(latency);
        LinkedHashSet<String> issues = new LinkedHashSet<>(view.issues());
        if (!view.runId().equals(latency.runId()) || !view.loadSlot().equals(latency.loadSlot())
                || !metadata.clockId().equals(latency.clockId())) { issues.add("COLLECTOR_IDENTITY_MISMATCH"); }
        if (latency.failure() != null) { issues.add(latency.failure()); }
        if (view.elapsed() <= 0) { issues.add("INVALID_RUN_CLOCK"); }
        reconcile(view.rows(), latency, issues);
        List<Map<String, Object>> histogramRows = histogramRows(latency);
        List<Map<String, Object>> metrics = metrics(view, histogramRows);
        for (String required : metadata.requiredMetrics()) {
            if (metrics.stream().noneMatch(m -> required.equals(m.get("metric_id")) && "measured".equals(m.get("status")))) {
                issues.add("REQUIRED_METRIC_UNAVAILABLE:" + required);
            }
        }
        Map<String, Object> cohort = cohort(view.rows(), issues);
        boolean valid = issues.isEmpty();
        Path target = directory.toAbsolutePath().normalize();
        Files.createDirectory(target);
        writeText(target.resolve("STARTED"), "Not a complete result until COMPLETE and integrity.json exist.\n");
        List<Map<String, Object>> artifacts = new ArrayList<>();
        Map<String, Object> manifest = map("schema_version", "benchmark-local-manifest-v1", "run_id", view.runId(),
                "load_slot", view.loadSlot(), "scope", "single_jvm_local", "readiness", "local_functional_only",
                "metadata", metadata, "trace", trace, "plan", view.plan(), "mode", view.mode(),
                "trace_hash", null, "source_tree_sha256", null,
                "limitations", List.of("not_a_ready_distributed_manifest", "no_full_history_or_ack_audit",
                        "no_windows_or_host_resource_metrics", "instrumentation_overhead_not_qualified"));
        addJson(target, "manifest.json", manifest, artifacts);
        String manifestHash = (String) artifacts.getFirst().get("sha256");
        addJson(target, "environment.json", map("client_java_version", System.getProperty("java.version"),
                "client_vm", System.getProperty("java.vm.name"), "client_os", System.getProperty("os.name"),
                "client_arch", System.getProperty("os.arch"), "clock_id", latency.clockId(),
                "clock_method", "single_process_System.nanoTime", "cross_host_clock_error_ns", null,
                "server_environment", null, "storage_kind", null), artifacts);
        addJson(target, "execution.json", view.execution(), artifacts);
        addText(target, "histograms.jsonl", jsonLines(histogramRows), artifacts);
        addText(target, "events.jsonl", jsonLines(((List<?>) view.events()).stream()
                .map(event -> map("clock_id", latency.clockId(), "run_id", view.runId(), "event", event)).toList()), artifacts);
        StringBuilder csv = new StringBuilder("phase,operation,considered,admitted,success,error_response,no_response,not_admitted,unresolved,attempts,retried_logical\n");
        for (Row r : view.rows()) { csv.append(r.phase()).append(',').append(r.operation()).append(',')
                .append(r.considered()).append(',').append(r.admitted()).append(',').append(r.success()).append(',')
                .append(r.errorResponse()).append(',').append(r.noResponse()).append(',').append(r.notAdmitted()).append(',')
                .append(r.unresolved()).append(',').append(r.attempts()).append(',').append(r.retriedLogical()).append('\n'); }
        addText(target, "counters.csv", csv.toString(), artifacts);
        Map<String, Object> result = map("schema_version", "replication-result-v1", "run_id", view.runId(),
                "manifest_sha256", manifestHash, "execution_status", view.completed() ? "completed" : "failed",
                "validity", valid ? "valid" : "invalid", "invalid_reasons", List.copyOf(issues),
                "cohort", cohort, "elapsed_ns", view.elapsed() > 0 ? view.elapsed() : null,
                "metrics", metrics, "artifacts", List.copyOf(artifacts));
        addJson(target, "result.json", result, artifacts);
        addJson(target, "integrity.json", map("schema_version", "benchmark-integrity-v1", "complete", true,
                "files", List.copyOf(artifacts), "collector_reconciled", issues.stream().noneMatch(s -> s.startsWith("COLLECTOR") || s.startsWith("HISTOGRAM")),
                "invalid_reasons", List.copyOf(issues), "correctness_verdict", "not_evaluated",
                "clock_qualification", "same_process_only", "observer_overhead_qualified", false), new ArrayList<>());
        // COMPLETE만 완료 표시다. STARTED는 시작 증거로 남기며 COMPLETE 없이는 부분 결과로 취급한다.
        writeText(target.resolve("COMPLETE"), "integrity_sha256=" + hash(Files.readAllBytes(target.resolve("integrity.json"))) + "\n");
        return new Export(target, valid, List.copyOf(issues), manifestHash);
    }
    private static void reconcile(List<Row> rows, LatencyCollector.Snapshot snapshot, Set<String> issues) {
        Map<LatencyCollector.Key, Row> expected = new HashMap<>();
        for (Row row : rows) {
            var key = new LatencyCollector.Key(row.phase(), row.operation()); expected.put(key, row);
            var n = snapshot.counts().getOrDefault(key, new LatencyCollector.Tally(0, 0, 0, 0, 0));
            if (n.success() != row.success() || n.errorResponse() != row.errorResponse() || n.noResponse() != row.noResponse()
                    || n.attempts() != row.attempts() || n.retried() != row.retriedLogical()) {
                issues.add("COLLECTOR_COUNTER_MISMATCH:" + key);
            }
            if (row.admitted() < row.success() + row.errorResponse() + row.noResponse()
                    || row.considered() != row.success() + row.errorResponse() + row.noResponse() + row.notAdmitted() + row.unresolved()) {
                issues.add("COHORT_ACCOUNTING_MISMATCH:" + key);
            }
            for (var kind : LatencyCollector.Kind.values()) {
                long count = snapshot.series().stream().filter(s -> s.population().phase() == row.phase()
                        && s.population().operation() == row.operation() && s.population().kind() == kind)
                        .mapToLong(s -> s.histogram().count()).sum();
                if (count != n.observed()) { issues.add("HISTOGRAM_POPULATION_MISMATCH:" + key + ":" + kind); }
            }
        }
        if (!expected.keySet().containsAll(snapshot.counts().keySet())) { issues.add("COLLECTOR_EXTRA_PHASE"); }
        for (var series : snapshot.series()) {
            var h = series.histogram();
            if (!expected.containsKey(new LatencyCollector.Key(series.population().phase(), series.population().operation()))) {
                issues.add("HISTOGRAM_EXTRA_PHASE");
            }
            if (h.count() != Math.addExact(h.overflowCount(), h.buckets().values().stream().mapToLong(Long::longValue).sum())) {
                issues.add("HISTOGRAM_BUCKET_MISMATCH");
            }
        }
    }
    private static Map<String, Object> cohort(List<Row> rows, Set<String> issues) {
        long admitted = 0, success = 0, error = 0, noResponse = 0;
        for (Row row : rows) { if (row.phase() == TracePhase.MEASURE) {
            admitted += row.admitted(); success += row.success(); error += row.errorResponse(); noResponse += row.noResponse();
        } }
        long pending = admitted - success - error - noResponse;
        if (pending < 0) { issues.add("NEGATIVE_MEASURE_PENDING"); return null; }
        if (pending > 0) { issues.add("MEASURE_PENDING"); }
        return map("admitted", admitted, "success", success, "error_response", error, "no_response", noResponse, "pending", pending);
    }
    private static List<Map<String, Object>> histogramRows(LatencyCollector.Snapshot latency) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (var series : latency.series()) {
            var h = series.histogram();
            List<Map<String, Object>> buckets = new ArrayList<>();
            h.buckets().forEach((upper, count) -> buckets.add(map("upper_bound_ns", upper, "count", count)));
            rows.add(map("population", series.population(), "clock_id", latency.clockId(), "bin_version", h.binVersion(),
                    "highest_finite_ns", h.highestFiniteNanos(), "count", h.count(), "sum_ns", h.sumNanos(),
                    "min_ns", h.minimumNanos(), "max_ns", h.maximumNanos(), "mean_ns", h.meanNanos(),
                    "overflow_count", h.overflowCount(), "buckets", buckets,
                    "percentile_method", "nearest_rank_bucket_upper_bound", "p50", h.quantile(50, 100),
                    "p95", h.quantile(95, 100), "p99", h.quantile(99, 100), "p99_9", h.quantile(999, 1000)));
        }
        rows.sort(Comparator.comparing(BenchmarkJson::encode)); return rows;
    }
    private static List<Map<String, Object>> metrics(View view, List<Map<String, Object>> histograms) {
        List<Map<String, Object>> records = new ArrayList<>();
        List<Row> measured = view.rows().stream().filter(r -> r.phase() == TracePhase.MEASURE).toList();
        long admitted = measured.stream().mapToLong(Row::admitted).sum();
        long failed = measured.stream().mapToLong(r -> r.errorResponse() + r.noResponse()).sum();
        long retried = measured.stream().mapToLong(Row::retriedLogical).sum();
        for (int i = 1; i <= 36; i++) {
            String id = String.format(Locale.ROOT, "M%02d", i);
            String status = "unsupported", reason = "collector_not_implemented_in_local_export", unit = unit(id);
            Object value = null; Number numerator = null, denominator = null;
            Map<String, Object> dimensions = map("scope", "single_jvm", "phase", "ALL_SEPARATE", "mode", view.mode());
            switch (id) {
                case "M01" -> value = view.rows();
                case "M02" -> { value = view.rows().stream().map(r -> map("phase", r.phase(), "operation", r.operation(), "attempts", r.attempts())).toList(); dimensions.put("layer", "client_execute_not_protocol_rpc"); }
                case "M03" -> value = view.rows().stream().map(r -> map("phase", r.phase(), "operation", r.operation(), "success", r.success())).toList();
                case "M04", "M10", "M11" -> {
                    unit = "ns";
                    var kind = switch (id) { case "M04" -> LatencyCollector.Kind.DISPATCH_LAG; case "M10" -> LatencyCollector.Kind.SERVICE; default -> LatencyCollector.Kind.SCHEDULED; };
                    var selected = histograms.stream().filter(h -> ((LatencyCollector.Population) h.get("population")).kind() == kind).toList();
                    if (selected.isEmpty()) { status = "not_run"; reason = "no_observed_latency_samples"; }
                    else { value = selected; }
                    dimensions.put("artifact", "histograms.jsonl"); dimensions.put("sampling", "all_observed_outcomes");
                }
                case "M07", "M08" -> {
                    unit = "ratio"; numerator = id.equals("M07") ? failed : retried; denominator = admitted;
                    dimensions.put("phase", "MEASURE"); dimensions.put("population", "admitted_excludes_admission_rejections");
                    if (admitted > 0) { value = numerator.doubleValue() / admitted; }
                    else { reason = "zero_admitted_denominator"; }
                }
                case "M09" -> reason = "partial_unresolved_and_lifetime_late_counters_in_execution_json_no_phase_late_ledger";
                default -> { }
            }
            if (value != null) { status = "measured"; reason = null; }
            records.add(map("metric_id", id, "status", status, "unit", unit, "value", value, "reason", reason,
                    "dimensions", dimensions, "numerator", numerator, "denominator", denominator));
        }
        return records;
    }
    private static String jsonLines(List<?> rows) {
        StringBuilder out = new StringBuilder(); for (Object row : rows) { out.append(BenchmarkJson.encode(row)).append('\n'); } return out.toString();
    }
    private static String unit(String id) {
        return switch (id) {
            case "M04", "M10", "M11", "M13" -> "ns";
            case "M05", "M06" -> "logical_ops/s";
            case "M07", "M08", "M20", "M36" -> "ratio";
            case "M12" -> "requests";
            case "M14" -> "requests,bytes,ns";
            case "M15" -> "messages/logical_op";
            case "M16" -> "bytes/logical_op";
            case "M17" -> "count,connections/s";
            case "M18" -> "cpu_ns/logical_op";
            case "M19" -> "bytes,ops,ns";
            case "M21" -> "count,ns";
            case "M25" -> "count,ns,bytes,ratio";
            case "M22" -> "count,ratio,bytes";
            case "M23" -> "bytes,ns,count";
            case "M24" -> "requests,ns,ratio";
            case "M26" -> "count,ns,ratio";
            case "M27" -> "ns,ratio";
            case "M28", "M29" -> "count,status";
            case "M30" -> "bytes,count,ns";
            case "M31" -> "items,bytes,ns,ratio";
            case "M32" -> "keys,workers,ratio";
            case "M33" -> "count,ns,ratio";
            case "M34" -> "requests,ns,count";
            case "M35" -> "ratio,logical_ops/s,ns";
            default -> "count";
        };
    }
    static Map<String, Object> map(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) { result.put((String) pairs[i], pairs[i + 1]); } return result;
    }
    private static void addJson(Path directory, String name, Object value, List<Map<String, Object>> artifacts) throws IOException {
        addText(directory, name, BenchmarkJson.encode(value) + "\n", artifacts);
    }
    private static void addText(Path directory, String name, String text, List<Map<String, Object>> artifacts) throws IOException {
        Path file = directory.resolve(name); writeText(file, text);
        artifacts.add(map("path", name, "sha256", hash(Files.readAllBytes(file))));
    }
    private static void writeText(Path file, String content) throws IOException {
        Path temporary = file.resolveSibling(file.getFileName() + ".part");
        Files.writeString(temporary, content, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        try { Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE); }
        catch (AtomicMoveNotSupportedException unsupported) { throw new IOException("atomic publication required", unsupported); }
    }
    private static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}
