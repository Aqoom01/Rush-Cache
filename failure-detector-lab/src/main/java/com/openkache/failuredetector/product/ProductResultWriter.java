package com.openkache.failuredetector.product;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ProductResultWriter {
    public void write(Path output, List<ProductRunResult> results) throws IOException {
        refuseOverwrite(output);
        Path raw = output.resolve("raw");
        Path summary = output.resolve("summary");
        Path report = output.resolve("report");
        Files.createDirectories(raw);
        Files.createDirectories(summary);
        Files.createDirectories(report);
        for (ProductRunResult result : results) {
            writeEvents(raw.resolve("run-" + result.runId() + ".jsonl"), result);
        }
        writeRuns(summary.resolve("runs.csv"), results);
        writeDetectionLatency(summary.resolve("detection_latency.csv"), results);
        writeFalsePositive(summary.resolve("false_positive.csv"), results);
        writeMembership(summary.resolve("membership_convergence.csv"), results);
        writeNetwork(summary.resolve("network_overhead.csv"), results);
        writeDetectionSuccess(summary.resolve("detection_success.csv"), results);
        writeComparison(report.resolve("algorithm_comparison.csv"), results);
        writeConfig(report.resolve("experiment_config.json"), results);
    }

    private static void refuseOverwrite(Path output) throws IOException {
        if (Files.notExists(output)) {
            return;
        }
        try (var stream = Files.walk(output)) {
            if (stream.anyMatch(path -> !path.equals(output))) {
                throw new FileAlreadyExistsException(
                        output.toString(), null, "existing benchmark results are preserved");
            }
        }
    }

    private static void writeEvents(Path path, ProductRunResult result) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (BenchmarkEvent event : result.events()) {
                writer.write("{"
                        + "\"run_id\":\"" + json(result.runId()) + "\","
                        + "\"algorithm\":\"" + result.algorithm().id() + "\","
                        + "\"implementation_type\":\"" + result.implementationType() + "\","
                        + "\"scenario\":\"" + result.scenario().id() + "\","
                        + "\"seed\":" + result.seed() + ","
                        + "\"node_count\":" + result.nodeCount() + ","
                        + "\"observer_id\":\"" + json(event.observer()) + "\","
                        + "\"target_id\":\"" + json(event.target()) + "\","
                        + "\"fault_start_timestamp\":" + result.faultStartNanos() + ","
                        + "\"event_type\":\"" + event.type() + "\","
                        + "\"event_timestamp\":" + event.atNanos() + ","
                        + "\"parameters\":" + mapJson(result.parameters()) + ","
                        + "\"details\":" + mapJson(event.details()) + ","
                        + "\"tx_bytes\":" + result.txBytes() + ","
                        + "\"rx_bytes\":" + result.rxBytes() + ","
                        + "\"success\":" + result.executionSuccess() + ","
                        + "\"error\":\"" + json(result.error()) + "\"}");
                writer.newLine();
            }
        }
    }

    private static void writeRuns(Path path, List<ProductRunResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,algorithm,implementation_type,scenario,seed,node_count,mode,"
                    + "detection_latency_ms,cluster_confirmation_latency_ms,"
                    + "membership_convergence_ms,false_unavailable_events,false_dead_events,"
                    + "false_unavailable_duration_ms,detected_targets,total_targets,"
                    + "detection_success,tx_messages,rx_messages,tx_bytes,rx_bytes,"
                    + "latency_source,network_cost_source,execution_success,error\n");
            for (ProductRunResult result : results) {
                writer.write(String.join(",",
                        csv(result.runId()),
                        csv(result.algorithm().id()),
                        csv(result.implementationType()),
                        csv(result.scenario().id()),
                        Long.toString(result.seed()),
                        Integer.toString(result.nodeCount()),
                        csv(result.mode()),
                        millis(result.detectionLatencyNanos()),
                        millis(result.clusterConfirmationLatencyNanos()),
                        millis(result.membershipConvergenceNanos()),
                        Long.toString(result.falseUnavailableEvents()),
                        Long.toString(result.falseDeadEvents()),
                        format(result.falseUnavailableNanos() / 1_000_000.0),
                        Integer.toString(result.detectedTargets()),
                        Integer.toString(result.targets().size()),
                        result.scenario().expectsCrashDetection()
                                ? Boolean.toString(result.detectionSuccess()) : "",
                        Long.toString(result.txMessages()),
                        Long.toString(result.rxMessages()),
                        Long.toString(result.txBytes()),
                        Long.toString(result.rxBytes()),
                        csv(result.latencySource()),
                        csv(result.networkCostSource()),
                        Boolean.toString(result.executionSuccess()),
                        csv(result.error())
                ));
                writer.newLine();
            }
        }
    }

    private static void writeDetectionLatency(Path path, List<ProductRunResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("algorithm,scenario,total_runs,successful_detections,missed_detections,"
                    + "p50_ms,p95_ms,p99_ms,detection_success_rate,source\n");
            for (Map.Entry<String, List<ProductRunResult>> entry : groups(results).entrySet()) {
                List<ProductRunResult> group = entry.getValue();
                List<Double> values = group.stream()
                        .map(ProductRunResult::detectionLatencyNanos)
                        .filter(value -> value != null)
                        .map(value -> value / 1_000_000.0)
                        .sorted().toList();
                String[] key = entry.getKey().split("\\|", 2);
                boolean expected = ProductScenario.fromId(key[1]).expectsCrashDetection();
                writer.write(String.join(",",
                        csv(key[0]), csv(key[1]), Integer.toString(group.size()),
                        Integer.toString(values.size()),
                        expected ? Integer.toString(group.size() - values.size()) : "",
                        percentile(values, 0.50), percentile(values, 0.95), percentile(values, 0.99),
                        expected ? format(group.stream().filter(ProductRunResult::detectionSuccess)
                                .count() / (double) group.size()) : "",
                        csv("simulation-observed")
                ));
                writer.newLine();
            }
        }
    }

    private static void writeFalsePositive(Path path, List<ProductRunResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("algorithm,scenario,total_runs,false_unavailable_events,false_dead_events,"
                    + "false_unavailable_duration_ms,false_positive_run_rate,source\n");
            for (Map.Entry<String, List<ProductRunResult>> entry : groups(results).entrySet()) {
                List<ProductRunResult> group = entry.getValue();
                String[] key = entry.getKey().split("\\|", 2);
                long unavailable = group.stream().mapToLong(
                        ProductRunResult::falseUnavailableEvents).sum();
                long dead = group.stream().mapToLong(ProductRunResult::falseDeadEvents).sum();
                long falseRuns = group.stream().filter(result ->
                        result.falseUnavailableEvents() > 0 || result.falseDeadEvents() > 0).count();
                writer.write(String.join(",",
                        csv(key[0]), csv(key[1]), Integer.toString(group.size()),
                        Long.toString(unavailable), Long.toString(dead),
                        format(group.stream().mapToLong(ProductRunResult::falseUnavailableNanos)
                                .sum() / 1_000_000.0),
                        format(falseRuns / (double) group.size()), csv("simulation-observed")
                ));
                writer.newLine();
            }
        }
    }

    private static void writeMembership(Path path, List<ProductRunResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("algorithm,scenario,total_runs,n,missing_n,p50_ms,p95_ms,success_rate,source\n");
            for (Map.Entry<String, List<ProductRunResult>> entry : groups(results).entrySet()) {
                List<ProductRunResult> group = entry.getValue();
                List<Double> values = group.stream().map(ProductRunResult::membershipConvergenceNanos)
                        .filter(value -> value != null).map(value -> value / 1_000_000.0)
                        .sorted().toList();
                String[] key = entry.getKey().split("\\|", 2);
                writer.write(String.join(",", csv(key[0]), csv(key[1]),
                        Integer.toString(group.size()), Integer.toString(values.size()),
                        Integer.toString(group.size() - values.size()), percentile(values, 0.50),
                        percentile(values, 0.95), format(values.size() / (double) group.size()),
                        csv("simulation-observed; N/A when protocol has no membership removal")));
                writer.newLine();
            }
        }
    }

    private static void writeNetwork(Path path, List<ProductRunResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("algorithm,scenario,total_runs,tx_messages_mean,rx_messages_mean,"
                    + "tx_bytes_mean,rx_bytes_mean,bytes_per_node_second_mean,source\n");
            for (Map.Entry<String, List<ProductRunResult>> entry : groups(results).entrySet()) {
                List<ProductRunResult> group = entry.getValue();
                String[] key = entry.getKey().split("\\|", 2);
                writer.write(String.join(",", csv(key[0]), csv(key[1]),
                        Integer.toString(group.size()),
                        mean(group.stream().mapToLong(ProductRunResult::txMessages).boxed().toList()),
                        mean(group.stream().mapToLong(ProductRunResult::rxMessages).boxed().toList()),
                        mean(group.stream().mapToLong(ProductRunResult::txBytes).boxed().toList()),
                        mean(group.stream().mapToLong(ProductRunResult::rxBytes).boxed().toList()),
                        format(group.stream().mapToDouble(result ->
                                (result.txBytes() + result.rxBytes())
                                        / (result.nodeCount() * (result.durationNanos() / 1e9)))
                                .average().orElse(0)),
                        csv("model-estimated")));
                writer.newLine();
            }
        }
    }

    private static void writeDetectionSuccess(Path path, List<ProductRunResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("algorithm,scenario,total_runs,successful_runs,failed_runs,"
                    + "detected_targets,total_targets,success_rate,source\n");
            for (Map.Entry<String, List<ProductRunResult>> entry : groups(results).entrySet()) {
                List<ProductRunResult> group = entry.getValue();
                String[] key = entry.getKey().split("\\|", 2);
                boolean expected = ProductScenario.fromId(key[1]).expectsCrashDetection();
                long success = group.stream().filter(ProductRunResult::detectionSuccess).count();
                writer.write(String.join(",", csv(key[0]), csv(key[1]),
                        Integer.toString(group.size()), expected ? Long.toString(success) : "",
                        expected ? Long.toString(group.size() - success) : "",
                        expected ? Long.toString(group.stream().mapToLong(
                                ProductRunResult::detectedTargets).sum()) : "",
                        expected ? Long.toString(group.stream().mapToLong(
                                result -> result.targets().size()).sum()) : "",
                        expected ? format(success / (double) group.size()) : "",
                        csv("simulation-observed; N/A for non-crash scenarios")));
                writer.newLine();
            }
        }
    }

    private static void writeComparison(Path path, List<ProductRunResult> results) throws IOException {
        Map<ProductAlgorithm, List<ProductRunResult>> byAlgorithm = new LinkedHashMap<>();
        for (ProductAlgorithm algorithm : ProductAlgorithm.values()) {
            byAlgorithm.put(algorithm, results.stream()
                    .filter(result -> result.algorithm() == algorithm).toList());
        }
        try (BufferedWriter writer = writer(path)) {
            writer.write("algorithm,total_runs,crash_success_rate,crash_p95_ms,"
                    + "loss_false_unavailable_events,bytes_per_node_second,implementation_type\n");
            for (Map.Entry<ProductAlgorithm, List<ProductRunResult>> entry : byAlgorithm.entrySet()) {
                List<ProductRunResult> all = entry.getValue();
                List<ProductRunResult> crashes = all.stream()
                        .filter(result -> result.scenario() == ProductScenario.CRASH).toList();
                List<Double> latencies = crashes.stream().map(ProductRunResult::detectionLatencyNanos)
                        .filter(value -> value != null).map(value -> value / 1_000_000.0)
                        .sorted().toList();
                double nodeSeconds = all.stream().mapToDouble(result ->
                        result.nodeCount() * result.durationNanos() / 1e9).sum();
                writer.write(String.join(",", csv(entry.getKey().id()),
                        Integer.toString(all.size()),
                        crashes.isEmpty() ? "" : format(crashes.stream()
                                .filter(ProductRunResult::detectionSuccess).count()
                                / (double) crashes.size()),
                        percentile(latencies, 0.95),
                        Long.toString(all.stream().filter(result ->
                                result.scenario() == ProductScenario.NORMAL
                                        || result.scenario() == ProductScenario.LOSS_15
                                        || result.scenario() == ProductScenario.LOSS_45)
                                .mapToLong(ProductRunResult::falseUnavailableEvents).sum()),
                        nodeSeconds == 0 ? "" : format(all.stream().mapToLong(result ->
                                result.txBytes() + result.rxBytes()).sum() / nodeSeconds),
                        csv("inspired")));
                writer.newLine();
            }
        }
    }

    private static void writeConfig(Path path, List<ProductRunResult> results) throws IOException {
        GitMetadata git = GitMetadata.capture();
        List<String> algorithms = results.stream().map(result -> result.algorithm().id())
                .distinct().sorted().toList();
        List<String> scenarios = results.stream().map(result -> result.scenario().id())
                .distinct().sorted().toList();
        List<Long> seeds = results.stream().map(ProductRunResult::seed).distinct().sorted().toList();
        List<Integer> nodeCounts = results.stream().map(ProductRunResult::nodeCount)
                .distinct().sorted().toList();
        List<String> modes = results.stream().map(ProductRunResult::mode)
                .distinct().sorted().toList();
        try (BufferedWriter writer = writer(path)) {
            writer.write("{\n"
                    + "  \"schema_version\": 1,\n"
                    + "  \"generated_at\": \"" + Instant.now() + "\",\n"
                    + "  \"git_commit\": \"" + json(git.commit) + "\",\n"
                    + "  \"git_dirty\": " + git.dirty + ",\n"
                    + "  \"run_count\": " + results.size() + ",\n"
                    + "  \"algorithms\": " + jsonArray(algorithms) + ",\n"
                    + "  \"scenarios\": " + jsonArray(scenarios) + ",\n"
                    + "  \"seeds\": " + seeds + ",\n"
                    + "  \"node_counts\": " + nodeCounts + ",\n"
                    + "  \"modes\": " + jsonArray(modes) + ",\n"
                    + "  \"latency_source\": \"simulation-observed\",\n"
                    + "  \"network_cost_source\": \"model-estimated\",\n"
                    + "  \"note\": \"Inspired benchmark; not native product execution.\",\n"
                    + "  \"profile_parameters\": {\n");
            for (int index = 0; index < algorithms.size(); index++) {
                String algorithm = algorithms.get(index);
                Map<String, Object> parameters = results.stream()
                        .filter(result -> result.algorithm().id().equals(algorithm))
                        .findFirst().orElseThrow().parameters();
                writer.write("    \"" + json(algorithm) + "\": " + mapJson(parameters));
                writer.write(index + 1 == algorithms.size() ? "\n" : ",\n");
            }
            writer.write("  }\n}\n");
        }
    }

    private static Map<String, List<ProductRunResult>> groups(List<ProductRunResult> results) {
        Map<String, List<ProductRunResult>> groups = new java.util.TreeMap<>();
        for (ProductRunResult result : results) {
            groups.computeIfAbsent(result.algorithm().id() + "|" + result.scenario().id(),
                    ignored -> new ArrayList<>()).add(result);
        }
        return groups;
    }

    private static String percentile(List<Double> values, double quantile) {
        if (values.isEmpty()) {
            return "";
        }
        double position = (values.size() - 1) * quantile;
        int lower = (int) position;
        int upper = Math.min(values.size() - 1, lower + 1);
        double fraction = position - lower;
        return format(values.get(lower) * (1 - fraction) + values.get(upper) * fraction);
    }

    private static String mean(List<Long> values) {
        return format(values.stream().mapToLong(Long::longValue).average().orElse(0));
    }

    private static String millis(Long nanos) {
        return nanos == null ? "" : format(nanos / 1_000_000.0);
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String jsonArray(List<String> values) {
        return values.stream().map(value -> "\"" + json(value) + "\"")
                .reduce("[", (left, right) -> left.equals("[")
                        ? left + right : left + ", " + right) + "]";
    }

    private static String mapJson(Map<String, Object> values) {
        StringBuilder builder = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : values.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey)).toList()) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            builder.append('"').append(json(entry.getKey())).append("\":");
            Object value = entry.getValue();
            if (value instanceof Number || value instanceof Boolean) {
                builder.append(value);
            } else {
                builder.append('"').append(json(String.valueOf(value))).append('"');
            }
        }
        return builder.append('}').toString();
    }

    private record GitMetadata(String commit, boolean dirty) {
        private static GitMetadata capture() {
            try {
                Process commit = new ProcessBuilder("git", "rev-parse", "HEAD")
                        .redirectErrorStream(true).start();
                String hash = read(commit);
                if (commit.waitFor() != 0) {
                    return new GitMetadata("unavailable", true);
                }
                Process status = new ProcessBuilder("git", "status", "--porcelain")
                        .redirectErrorStream(true).start();
                boolean dirty = !read(status).isBlank();
                status.waitFor();
                return new GitMetadata(hash.strip(), dirty);
            } catch (IOException exception) {
                return new GitMetadata("unavailable", true);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return new GitMetadata("unavailable", true);
            }
        }

        private static String read(Process process) throws IOException {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8))) {
                return reader.lines().reduce("", (left, right) -> left + right + "\n");
            }
        }
    }
}
