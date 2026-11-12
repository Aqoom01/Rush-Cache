package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.GroundTruthEvent;
import com.openkache.failuredetector.modules.CorrectionKind;
import com.openkache.failuredetector.modules.DecisionKind;
import com.openkache.failuredetector.modules.ObservationKind;
import com.openkache.failuredetector.modules.ProfileModules;

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
import java.util.function.Function;

public final class ResultWriter {
    private static final List<String> OWNED_OUTPUTS = List.of(
            "run-manifest.json",
            "events.jsonl",
            "runs.jsonl",
            "runs.csv",
            "summary.csv",
            "aggregate.csv",
            "module-contributions.csv",
            "compatibility.csv",
            "REPORT.md"
    );

    public void write(Path outputDirectory, List<ExperimentResult> results) throws IOException {
        Files.createDirectories(outputDirectory);
        refuseOverwrite(outputDirectory);
        writeManifest(outputDirectory.resolve("run-manifest.json"), results);
        writeRunsCsv(outputDirectory.resolve("runs.csv"), results);
        writeSummary(outputDirectory.resolve("summary.csv"), results);
        writeAggregate(outputDirectory.resolve("aggregate.csv"), results);
        writeRunsJsonl(outputDirectory.resolve("runs.jsonl"), results);
        writeEvents(outputDirectory.resolve("events.jsonl"), results);
        writeModuleContributions(outputDirectory.resolve("module-contributions.csv"), results);
        writeCompatibility(outputDirectory.resolve("compatibility.csv"));
        new SvgChartWriter().write(outputDirectory.resolve("charts"), results);
        writeReport(outputDirectory.resolve("REPORT.md"), results);
    }

    private static void refuseOverwrite(Path outputDirectory) throws IOException {
        for (String name : OWNED_OUTPUTS) {
            Path output = outputDirectory.resolve(name);
            if (Files.exists(output)) {
                throw new FileAlreadyExistsException(
                        output.toString(), null, "existing experiment result is preserved");
            }
        }
        if (Files.exists(outputDirectory.resolve("charts"))) {
            throw new FileAlreadyExistsException(
                    outputDirectory.resolve("charts").toString(),
                    null,
                    "existing experiment charts are preserved"
            );
        }
    }

    private void writeManifest(Path path, List<ExperimentResult> results) throws IOException {
        GitMetadata git = GitMetadata.capture();
        try (BufferedWriter writer = writer(path)) {
            writer.write("{\n");
            writer.write("  \"schemaVersion\": 3,\n");
            writer.write("  \"generatedAt\": \"" + json(Instant.now().toString()) + "\",\n");
            writer.write("  \"gitCommit\": \"" + json(git.commit()) + "\",\n");
            writer.write("  \"gitDirty\": " + git.dirty() + ",\n");
            writer.write("  \"environment\": {\"java\": \""
                    + json(System.getProperty("java.version")) + "\", \"os\": \""
                    + json(System.getProperty("os.name") + " " + System.getProperty("os.version"))
                    + "\"},\n");
            writer.write("  \"runs\": [\n");
            for (int index = 0; index < results.size(); index++) {
                ExperimentResult result = results.get(index);
                DetectorProfile profile = DetectorProfile.findById(result.algorithm()).orElse(null);
                writer.write("    {"
                        + "\"runId\":\"" + json(result.runId()) + "\","
                        + "\"profile\":\"" + json(result.algorithm()) + "\","
                        + "\"scenario\":\"" + json(result.scenario().id()) + "\","
                        + "\"seed\":" + result.seed() + ","
                        + "\"nodeCount\":" + result.nodeCount() + ","
                        + "\"durationNanos\":" + result.durationNanos() + ","
                        + "\"faultStartNanos\":" + result.faultStartNanos() + ","
                        + "\"comparisonMode\":\"" + json(result.comparisonMode()) + "\","
                        + "\"executionSuccess\":" + result.executionSuccess()
                        + moduleManifest(profile)
                        + "}");
                writer.write(index + 1 == results.size() ? "\n" : ",\n");
            }
            writer.write("  ]\n}\n");
        }
    }

    private static String moduleManifest(DetectorProfile profile) {
        if (profile == null) {
            return "";
        }
        return ",\"profileCode\":\"" + json(profile.code()) + "\""
                + ",\"modules\":{"
                + "\"decision\":\"" + json(profile.modules().decision().name()) + "\","
                + "\"observation\":\"" + json(profile.modules().observation().name()) + "\","
                + "\"correction\":\"" + json(profile.modules().correction().name()) + "\"}";
    }

    private void writeRunsCsv(Path path, List<ExperimentResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,profile,profile_code,decision,observation,correction,");
            writer.write("scenario,node_count,seed,comparison_mode,execution_success,");
            writer.write("detected_suspect,detected_dead,suspicion_latency_ms,detection_latency_ms,");
            writer.write("false_suspect_events,false_suspect_seconds,false_dead_fail_stop_events,");
            writer.write("false_dead_fail_stop_seconds,unreachable_but_alive_events,");
            writer.write("unreachable_but_alive_seconds,false_negative_events,right_censored,");
            writer.write("flap_count,protocol_messages,protocol_bytes,messages_per_node_second,");
            writer.write("bytes_per_node_second,latency_source,protocol_cost_source\n");
            for (ExperimentResult result : results) {
                RunMetrics metric = result.metrics();
                double nodeSeconds = nodeSeconds(result);
                DetectorProfile profile = DetectorProfile.findById(result.algorithm()).orElse(null);
                writer.write(String.join(",",
                        csv(result.runId()),
                        csv(result.algorithm()),
                        csv(profile == null ? "" : profile.code()),
                        csv(profile == null ? "" : profile.modules().decision().name()),
                        csv(profile == null ? "" : profile.modules().observation().name()),
                        csv(profile == null ? "" : profile.modules().correction().name()),
                        csv(result.scenario().id()),
                        Integer.toString(result.nodeCount()),
                        Long.toString(result.seed()),
                        csv(result.comparisonMode()),
                        Boolean.toString(result.executionSuccess()),
                        Boolean.toString(metric.suspicionLatencyNanos() != null),
                        Boolean.toString(metric.detectionLatencyNanos() != null),
                        nullable(metric.suspicionLatencyMillis()),
                        nullable(metric.detectionLatencyMillis()),
                        Long.toString(metric.falsePositiveEvents()),
                        format(metric.falseSuspectSeconds()),
                        Long.toString(metric.falseDeadEvents()),
                        format(metric.falseDeadSeconds()),
                        Long.toString(metric.unreachableButAliveEvents()),
                        format(metric.unreachableButAliveSeconds()),
                        Long.toString(metric.falseNegativeEvents()),
                        Boolean.toString(metric.falseStateRightCensored()),
                        Long.toString(metric.flapCount()),
                        Long.toString(metric.protocolMessages()),
                        Long.toString(metric.protocolBytes()),
                        format(metric.protocolMessages() / nodeSeconds),
                        format(metric.protocolBytes() / nodeSeconds),
                        csv(result.latencySource()),
                        csv(result.protocolCostSource())
                ));
                writer.newLine();
            }
        }
    }

    private void writeSummary(Path path, List<ExperimentResult> results) throws IOException {
        Map<String, List<ExperimentResult>> groups = groups(results);
        try (BufferedWriter writer = writer(path)) {
            writer.write("profile,profile_code,decision,observation,correction,");
            writer.write("scenario,node_count,comparison_mode,metric,unit,source,");
            writer.write("total_runs,n,missing_n,mean,p50,p95,p99,ci95_low,ci95_high\n");
            for (String key : groups.keySet().stream().sorted().toList()) {
                List<ExperimentResult> group = groups.get(key);
                String[] parts = key.split("\\|", 4);
                writeMetric(writer, parts, group, "false_suspect_events", "count",
                        "simulation-observed", result -> (double) result.metrics().falsePositiveEvents());
                writeMetric(writer, parts, group, "false_suspect_duration", "seconds",
                        "simulation-observed", result -> result.metrics().falseSuspectSeconds());
                writeMetric(writer, parts, group, "false_dead_fail_stop_events", "count",
                        "simulation-observed", result -> (double) result.metrics().falseDeadEvents());
                writeMetric(writer, parts, group, "unreachable_but_alive_duration", "seconds",
                        "simulation-observed", result -> result.metrics().unreachableButAliveSeconds());
                writeMetric(writer, parts, group, "suspicion_latency", "milliseconds",
                        "simulation-observed", result -> result.metrics().suspicionLatencyMillis());
                writeMetric(writer, parts, group, "detection_latency", "milliseconds",
                        "simulation-observed", result -> result.metrics().detectionLatencyMillis());
                writeMetric(writer, parts, group, "dead_detection_rate", "ratio",
                        "simulation-observed",
                        result -> result.metrics().detectionLatencyNanos() == null ? 0.0 : 1.0);
                writeMetric(writer, parts, group, "protocol_messages", "frames",
                        "model-estimated", result -> (double) result.metrics().protocolMessages());
                writeMetric(writer, parts, group, "protocol_bytes", "estimated_wire_bytes",
                        "model-estimated", result -> (double) result.metrics().protocolBytes());
                writeMetric(writer, parts, group, "messages_per_node_second", "frames/node/s",
                        "model-estimated", result -> result.metrics().protocolMessages() / nodeSeconds(result));
                writeMetric(writer, parts, group, "bytes_per_node_second", "bytes/node/s",
                        "model-estimated", result -> result.metrics().protocolBytes() / nodeSeconds(result));
            }
        }
    }

    private static void writeMetric(
            BufferedWriter writer,
            String[] group,
            List<ExperimentResult> results,
            String metric,
            String unit,
            String source,
            Function<ExperimentResult, Double> extractor
    ) throws IOException {
        List<Double> values = results.stream()
                .map(extractor)
                .filter(value -> value != null && Double.isFinite(value))
                .sorted()
                .toList();
        ConfidenceInterval interval = confidenceInterval(values);
        DetectorProfile profile = DetectorProfile.findById(group[0]).orElse(null);
        writer.write(String.join(",",
                csv(group[0]),
                csv(profile == null ? "" : profile.code()),
                csv(profile == null ? "" : profile.modules().decision().name()),
                csv(profile == null ? "" : profile.modules().observation().name()),
                csv(profile == null ? "" : profile.modules().correction().name()),
                csv(group[1]),
                group[2],
                csv(group[3]),
                csv(metric),
                csv(unit),
                csv(source),
                Integer.toString(results.size()),
                Integer.toString(values.size()),
                Integer.toString(results.size() - values.size()),
                values.isEmpty() ? "" : format(mean(values)),
                values.isEmpty() ? "" : format(percentile(values, 0.50)),
                values.isEmpty() ? "" : format(percentile(values, 0.95)),
                values.isEmpty() ? "" : format(percentile(values, 0.99)),
                interval == null ? "" : format(interval.low()),
                interval == null ? "" : format(interval.high())
        ));
        writer.newLine();
    }

    /** Compatibility wide table retained for the existing chart/report workflow. */
    private void writeAggregate(Path path, List<ExperimentResult> results) throws IOException {
        Map<String, List<ExperimentResult>> groups = groups(results);
        try (BufferedWriter writer = writer(path)) {
            writer.write("algorithm,scenario,node_count,comparison_mode,runs,missing_detection,");
            writer.write("detection_latency_ms_p50,detection_latency_ms_p95,");
            writer.write("detection_latency_ms_p99,false_dead_mean,messages_mean,bytes_mean,source\n");
            for (String key : groups.keySet().stream().sorted().toList()) {
                List<ExperimentResult> group = groups.get(key);
                List<Double> detection = group.stream()
                        .map(result -> result.metrics().detectionLatencyMillis())
                        .filter(value -> value != null)
                        .sorted()
                        .toList();
                String[] parts = key.split("\\|", 4);
                writer.write(String.join(",",
                        csv(parts[0]),
                        csv(parts[1]),
                        parts[2],
                        csv(parts[3]),
                        Integer.toString(group.size()),
                        Integer.toString(group.size() - detection.size()),
                        detection.isEmpty() ? "" : format(percentile(detection, 0.50)),
                        detection.isEmpty() ? "" : format(percentile(detection, 0.95)),
                        detection.isEmpty() ? "" : format(percentile(detection, 0.99)),
                        format(group.stream().mapToDouble(
                                result -> result.metrics().falseDeadEvents()).average().orElse(0)),
                        format(group.stream().mapToDouble(
                                result -> result.metrics().protocolMessages()).average().orElse(0)),
                        format(group.stream().mapToDouble(
                                result -> result.metrics().protocolBytes()).average().orElse(0)),
                        csv("mixed: simulation-observed latency; model-estimated cost")
                ));
                writer.newLine();
            }
        }
    }

    private void writeRunsJsonl(Path path, List<ExperimentResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (ExperimentResult result : results) {
                RunMetrics metric = result.metrics();
                DetectorProfile profile = DetectorProfile.findById(result.algorithm()).orElse(null);
                writer.write("{"
                        + "\"runId\":\"" + json(result.runId()) + "\","
                        + "\"profile\":\"" + json(result.algorithm()) + "\","
                        + "\"profileCode\":\"" + json(profile == null ? "" : profile.code()) + "\","
                        + "\"modules\":{"
                        + "\"decision\":\"" + json(profile == null ? "" : profile.modules().decision().name()) + "\","
                        + "\"observation\":\"" + json(profile == null ? "" : profile.modules().observation().name()) + "\","
                        + "\"correction\":\"" + json(profile == null ? "" : profile.modules().correction().name()) + "\"},"
                        + "\"scenario\":\"" + json(result.scenario().id()) + "\","
                        + "\"nodeCount\":" + result.nodeCount() + ","
                        + "\"seed\":" + result.seed() + ","
                        + "\"comparisonMode\":\"" + json(result.comparisonMode()) + "\","
                        + "\"suspicionLatencyNanos\":" + nullableLong(metric.suspicionLatencyNanos()) + ","
                        + "\"detectionLatencyNanos\":" + nullableLong(metric.detectionLatencyNanos()) + ","
                        + "\"falseDeadFailStopEvents\":" + metric.falseDeadEvents() + ","
                        + "\"unreachableButAliveNanos\":" + metric.unreachableButAliveNanos() + ","
                        + "\"protocolMessages\":" + metric.protocolMessages() + ","
                        + "\"protocolBytes\":" + metric.protocolBytes() + ","
                        + "\"latencySource\":\"" + json(result.latencySource()) + "\","
                        + "\"protocolCostSource\":\"" + json(result.protocolCostSource()) + "\""
                        + "}");
                writer.newLine();
            }
        }
    }

    private void writeEvents(Path path, List<ExperimentResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (ExperimentResult result : results) {
                for (GroundTruthEvent event : result.groundTruthEvents()) {
                    writer.write(baseEvent(result, "ground-truth", event.atNanos())
                            + ",\"observer\":\"" + ExperimentRunner.OBSERVER + "\""
                            + ",\"target\":\"" + json(event.target()) + "\""
                            + ",\"nodeAlive\":" + event.alive()
                            + ",\"reachableFromObserver\":" + event.reachable()
                            + ",\"serviceHealthy\":" + event.serviceHealthy()
                            + ",\"faultId\":\"" + json(event.faultId()) + "\""
                            + ",\"reason\":\"" + json(event.faultType()) + "\"}");
                    writer.newLine();
                }
                for (SimulationEvent event : result.simulationEvents()) {
                    writer.write(baseEvent(result, "simulation", event.atNanos())
                            + ",\"eventType\":\"" + event.type() + "\""
                            + ",\"observer\":\"" + json(event.observer()) + "\""
                            + ",\"target\":\"" + json(event.target()) + "\""
                            + ",\"reason\":\"" + json(event.reason()) + "\""
                            + ",\"details\":" + mapJson(event.details()) + "}");
                    writer.newLine();
                }
                for (DetectorEvent event : result.detectorEvents()) {
                    writer.write(baseEvent(result, "detector", event.atNanos())
                            + ",\"observer\":\"" + json(event.observer()) + "\""
                            + ",\"target\":\"" + json(event.target()) + "\""
                            + ",\"oldStatus\":\"" + event.oldStatus() + "\""
                            + ",\"newStatus\":\"" + event.newStatus() + "\""
                            + ",\"reason\":\"" + json(event.reason()) + "\""
                            + ",\"details\":" + mapJson(event.details()) + "}");
                    writer.newLine();
                }
            }
        }
    }

    private static String baseEvent(ExperimentResult result, String kind, long atNanos) {
        return "{\"runId\":\"" + json(result.runId()) + "\""
                + ",\"kind\":\"" + kind + "\""
                + ",\"atNanos\":" + atNanos
                + ",\"profile\":\"" + json(result.algorithm()) + "\""
                + ",\"scenario\":\"" + json(result.scenario().id()) + "\""
                + ",\"seed\":" + result.seed();
    }

    private void writeModuleContributions(Path path, List<ExperimentResult> results)
            throws IOException {
        List<ContributionPair> pairs = List.of(
                new ContributionPair("A:F0-vs-F1", "fixed-deadline", "phi-gossip"),
                new ContributionPair("A:F0-vs-F4", "fixed-deadline", "adaptive-deadline"),
                new ContributionPair("B:F0-vs-F5", "fixed-deadline", "fixed-deadline-ring"),
                new ContributionPair("C:F5-vs-F6", "fixed-deadline-ring", "fixed-deadline-ring-verify"),
                new ContributionPair("C:F2-vs-F3", "swim-base", "swim-lifeguard"),
                new ContributionPair("C:F2-vs-F7a", "swim-base", "swim-lha-probe"),
                new ContributionPair("C:F2-vs-F7b", "swim-base", "swim-lha-suspicion"),
                new ContributionPair("C:F2-vs-F7c", "swim-base", "swim-buddy")
        );
        Map<String, ExperimentResult> byProfileAndCondition = new HashMap<>();
        for (ExperimentResult result : results) {
            byProfileAndCondition.put(result.algorithm() + "|" + conditionKey(result), result);
        }
        try (BufferedWriter writer = writer(path)) {
            writer.write("comparison,baseline_profile,variant_profile,scenario,node_count,seed,");
            writer.write("comparison_mode,metric,unit,source,baseline_value,variant_value,delta\n");
            for (ContributionPair pair : pairs) {
                for (ExperimentResult baseline : results) {
                    if (!baseline.algorithm().equals(pair.baseline())) {
                        continue;
                    }
                    ExperimentResult variant = byProfileAndCondition.get(
                            pair.variant() + "|" + conditionKey(baseline));
                    if (variant == null) {
                        continue;
                    }
                    writeContribution(writer, pair, baseline, variant,
                            "suspicion_latency", "milliseconds", "simulation-observed",
                            baseline.metrics().suspicionLatencyMillis(),
                            variant.metrics().suspicionLatencyMillis());
                    writeContribution(writer, pair, baseline, variant,
                            "detection_latency", "milliseconds", "simulation-observed",
                            baseline.metrics().detectionLatencyMillis(),
                            variant.metrics().detectionLatencyMillis());
                    writeContribution(writer, pair, baseline, variant,
                            "false_suspect_events", "count", "simulation-observed",
                            (double) baseline.metrics().falsePositiveEvents(),
                            (double) variant.metrics().falsePositiveEvents());
                    writeContribution(writer, pair, baseline, variant,
                            "false_dead_fail_stop_events", "count", "simulation-observed",
                            (double) baseline.metrics().falseDeadEvents(),
                            (double) variant.metrics().falseDeadEvents());
                    writeContribution(writer, pair, baseline, variant,
                            "protocol_messages", "frames", "model-estimated",
                            (double) baseline.metrics().protocolMessages(),
                            (double) variant.metrics().protocolMessages());
                    writeContribution(writer, pair, baseline, variant,
                            "protocol_bytes", "estimated_wire_bytes", "model-estimated",
                            (double) baseline.metrics().protocolBytes(),
                            (double) variant.metrics().protocolBytes());
                }
            }
        }
    }

    private static void writeContribution(
            BufferedWriter writer,
            ContributionPair pair,
            ExperimentResult baseline,
            ExperimentResult variant,
            String metric,
            String unit,
            String source,
            Double baselineValue,
            Double variantValue
    ) throws IOException {
        Double delta = baselineValue == null || variantValue == null
                ? null
                : variantValue - baselineValue;
        writer.write(String.join(",",
                csv(pair.name()),
                csv(pair.baseline()),
                csv(pair.variant()),
                csv(baseline.scenario().id()),
                Integer.toString(baseline.nodeCount()),
                Long.toString(baseline.seed()),
                csv(baseline.comparisonMode()),
                csv(metric),
                csv(unit),
                csv(source),
                nullable(baselineValue),
                nullable(variantValue),
                nullable(delta)
        ));
        writer.newLine();
    }

    private void writeCompatibility(Path path) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("decision,observation,correction,compatible,declared_profiles,reason\n");
            for (DecisionKind decision : DecisionKind.values()) {
                for (ObservationKind observation : ObservationKind.values()) {
                    for (CorrectionKind correction : CorrectionKind.values()) {
                        ProfileModules modules = new ProfileModules(decision, observation, correction);
                        List<String> errors = modules.validationErrors();
                        String declared = java.util.Arrays.stream(DetectorProfile.values())
                                .filter(profile -> profile.modules().equals(modules))
                                .map(profile -> profile.code() + ":" + profile.id())
                                .sorted()
                                .reduce((left, right) -> left + ";" + right)
                                .orElse("");
                        writer.write(String.join(",",
                                csv(decision.name()),
                                csv(observation.name()),
                                csv(correction.name()),
                                Boolean.toString(errors.isEmpty()),
                                csv(declared),
                                csv(errors.isEmpty() ? "compatible capabilities" : String.join("; ", errors))
                        ));
                        writer.newLine();
                    }
                }
            }
        }
    }

    private static String conditionKey(ExperimentResult result) {
        return result.scenario().id() + "|" + result.nodeCount() + "|"
                + result.seed() + "|" + result.comparisonMode();
    }

    private void writeReport(Path path, List<ExperimentResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("# Failure Detector Experiment Report\n\n");
            writer.write("동일 scenario/seed/fault-offset를 프로필 간 재사용한 discrete-event 결과다.\n\n");
            writer.write("- 전체 Run: " + results.size() + "\n");
            writer.write("- 감지 지연/오판 상태: simulation-observed\n");
            writer.write("- frame/byte 비용: model-estimated (실제 UDP wire 계측이 아님)\n");
            writer.write("- 미검출 latency: 0이 아니라 null이며 summary의 missing_n에 포함\n");
            writer.write("- partition: fail-stop 관점 false DEAD와 unreachable-but-alive를 별도 집계\n");
            writer.write("- 기존 결과 보호: 같은 출력 디렉터리의 산출물을 덮어쓰지 않음\n\n");
            writer.write("최종 채택은 실제 UDP/netem 검증 전까지 보류해야 한다.\n");
        }
    }

    private static Map<String, List<ExperimentResult>> groups(List<ExperimentResult> results) {
        Map<String, List<ExperimentResult>> groups = new HashMap<>();
        for (ExperimentResult result : results) {
            String key = result.algorithm() + "|" + result.scenario().id() + "|"
                    + result.nodeCount() + "|" + result.comparisonMode();
            groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(result);
        }
        return groups;
    }

    private static double nodeSeconds(ExperimentResult result) {
        return result.nodeCount() * result.durationNanos() / 1_000_000_000.0;
    }

    private static ConfidenceInterval confidenceInterval(List<Double> values) {
        if (values.size() < 2) {
            return null;
        }
        double mean = mean(values);
        double sumSquares = values.stream()
                .mapToDouble(value -> (value - mean) * (value - mean))
                .sum();
        double standardDeviation = Math.sqrt(sumSquares / (values.size() - 1));
        double margin = 1.96 * standardDeviation / Math.sqrt(values.size());
        return new ConfidenceInterval(mean - margin, mean + margin);
    }

    private static double mean(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    private static double percentile(List<Double> values, double quantile) {
        double position = (values.size() - 1) * quantile;
        int lower = (int) position;
        int upper = Math.min(lower + 1, values.size() - 1);
        double fraction = position - lower;
        return values.get(lower) * (1 - fraction) + values.get(upper) * fraction;
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(
                path,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW
        );
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String nullable(Double value) {
        return value == null ? "" : format(value);
    }

    private static String nullableLong(Long value) {
        return value == null ? "null" : value.toString();
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String mapJson(Map<String, Object> map) {
        Map<String, Object> ordered = new LinkedHashMap<>();
        map.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> ordered.put(entry.getKey(), entry.getValue()));
        StringBuilder builder = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : ordered.entrySet()) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            builder.append('\"').append(json(entry.getKey())).append("\":");
            Object value = entry.getValue();
            if (value instanceof Number || value instanceof Boolean) {
                builder.append(value);
            } else {
                builder.append('\"').append(json(String.valueOf(value))).append('\"');
            }
        }
        return builder.append('}').toString();
    }

    private record ConfidenceInterval(double low, double high) {
    }

    private record ContributionPair(String name, String baseline, String variant) {
    }

    private record GitMetadata(String commit, boolean dirty) {
        private static GitMetadata capture() {
            try {
                Process commitProcess = new ProcessBuilder("git", "rev-parse", "HEAD")
                        .redirectErrorStream(true)
                        .start();
                String commit = readFirstLine(commitProcess);
                if (commitProcess.waitFor() != 0) {
                    return new GitMetadata("unavailable", true);
                }
                Process statusProcess = new ProcessBuilder("git", "status", "--porcelain")
                        .redirectErrorStream(true)
                        .start();
                boolean dirty = !readAll(statusProcess).isBlank();
                if (statusProcess.waitFor() != 0) {
                    dirty = true;
                }
                return new GitMetadata(commit, dirty);
            } catch (IOException exception) {
                return new GitMetadata("unavailable", true);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return new GitMetadata("unavailable", true);
            }
        }

        private static String readFirstLine(Process process) throws IOException {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                return line == null ? "unavailable" : line;
            }
        }

        private static String readAll(Process process) throws IOException {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                return reader.lines().reduce("", (left, right) -> left + right + "\n");
            }
        }
    }
}
