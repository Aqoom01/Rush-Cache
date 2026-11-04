package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.experiment.RingMultiCrashExperiment;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Runs a static Ring-k consecutive-node crash edge-case matrix. */
public final class RingAdjacentCrashMain {
    private RingAdjacentCrashMain() {
    }

    public static void main(String[] args) throws IOException {
        Options options = Options.parse(args);
        if (Files.exists(options.outputDirectory())) {
            throw new IllegalArgumentException(
                    "Output directory already exists; existing results are preserved: "
                            + options.outputDirectory());
        }
        Files.createDirectories(options.outputDirectory());

        RingMultiCrashExperiment experiment = new RingMultiCrashExperiment();
        List<NamedResult> results = new ArrayList<>();
        for (int nodeCount : options.nodeCounts()) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                RingMultiCrashExperiment.Result result = experiment.runConsecutiveCrash(
                        nodeCount,
                        options.neighborCount(),
                        firstCrash,
                        options.crashCount()
                );
                results.add(new NamedResult(
                        runId(options, nodeCount, firstCrash),
                        result
                ));
            }
        }

        writeManifest(options, results);
        writeRuns(options.outputDirectory().resolve("runs.csv"), results);
        writeTargets(options.outputDirectory().resolve("targets.csv"), results);
        writeEvents(options.outputDirectory().resolve("events.jsonl"), results);
        writeSummary(options.outputDirectory().resolve("summary.md"), results);

        long completeRuns = results.stream()
                .filter(named -> named.result().allDetectedDead())
                .count();
        long detectedTargets = results.stream()
                .mapToLong(named -> named.result().detectedDeadCount())
                .sum();
        long totalTargets = results.stream()
                .mapToLong(named -> named.result().outcomes().size())
                .sum();
        System.out.printf(Locale.ROOT,
                "Ring-%d consecutive-%d crash: completeRuns=%d/%d, "
                        + "detectedTargets=%d/%d, output=%s%n",
                options.neighborCount(), options.crashCount(), completeRuns, results.size(),
                detectedTargets, totalTargets, options.outputDirectory());
    }

    private static void writeManifest(Options options, List<NamedResult> results)
            throws IOException {
        Path path = options.outputDirectory().resolve("manifest.json");
        try (BufferedWriter writer = writer(path)) {
            writer.write("{\n");
            writer.write("  \"experiment\": \"" + experimentName(options) + "\",\n");
            writer.write("  \"generatedAt\": \"" + Instant.now() + "\",\n");
            writer.write("  \"nodeCounts\": \"" + join(options.nodeCounts()) + "\",\n");
            writer.write("  \"neighborCount\": " + options.neighborCount() + ",\n");
            writer.write("  \"crashCount\": " + options.crashCount() + ",\n");
            writer.write("  \"faultStartSeconds\": 20,\n");
            writer.write("  \"durationSeconds\": 40,\n");
            writer.write("  \"membershipReconfiguration\": false,\n");
            writer.write("  \"latencySource\": \"simulation-observed\",\n");
            writer.write("  \"protocolCostSource\": \"model-estimated\",\n");
            writer.write("  \"runs\": " + results.size() + "\n");
            writer.write("}\n");
        }
    }

    private static void writeRuns(Path path, List<NamedResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,node_count,neighbor_count,crashed_targets,");
            writer.write("detected_dead,target_count,all_detected,protocol_messages,");
            writer.write("protocol_bytes,latency_source,protocol_cost_source\n");
            for (NamedResult named : results) {
                RingMultiCrashExperiment.Result result = named.result();
                writer.write(String.join(",",
                        csv(named.runId()),
                        Integer.toString(result.nodeCount()),
                        Integer.toString(result.neighborCount()),
                        csv(String.join("|", result.crashedTargets())),
                        Long.toString(result.detectedDeadCount()),
                        Integer.toString(result.outcomes().size()),
                        Boolean.toString(result.allDetectedDead()),
                        Long.toString(result.protocolMessages()),
                        Long.toString(result.protocolBytes()),
                        csv("simulation-observed"),
                        csv("model-estimated")
                ));
                writer.newLine();
            }
        }
    }

    private static void writeTargets(Path path, List<NamedResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,target,assigned_observers,live_observers,");
            writer.write("suspect_latency_ms,dead_latency_ms,detected_dead\n");
            for (NamedResult named : results) {
                for (RingMultiCrashExperiment.TargetOutcome outcome
                        : named.result().outcomes()) {
                    writer.write(String.join(",",
                            csv(named.runId()),
                            csv(outcome.target()),
                            csv(String.join("|", outcome.assignedObservers())),
                            csv(String.join("|", outcome.liveObservers())),
                            nullableMillis(outcome.suspectLatencyNanos()),
                            nullableMillis(outcome.deadLatencyNanos()),
                            Boolean.toString(outcome.detectedDead())
                    ));
                    writer.newLine();
                }
            }
        }
    }

    private static void writeEvents(Path path, List<NamedResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (NamedResult named : results) {
                for (DetectorEvent event : named.result().events()) {
                    writer.write("{\"runId\":\"" + json(named.runId()) + "\""
                            + ",\"atNanos\":" + event.atNanos()
                            + ",\"observer\":\"" + json(event.observer()) + "\""
                            + ",\"target\":\"" + json(event.target()) + "\""
                            + ",\"oldStatus\":\"" + event.oldStatus() + "\""
                            + ",\"newStatus\":\"" + event.newStatus() + "\""
                            + ",\"reason\":\"" + json(event.reason()) + "\"}");
                    writer.newLine();
                }
            }
        }
    }

    private static void writeSummary(Path path, List<NamedResult> results) throws IOException {
        int neighborCount = results.get(0).result().neighborCount();
        int crashCount = results.get(0).result().crashedTargets().size();
        long completeRuns = results.stream()
                .filter(named -> named.result().allDetectedDead())
                .count();
        long detectedTargets = results.stream()
                .mapToLong(named -> named.result().detectedDeadCount())
                .sum();
        long totalTargets = results.stream()
                .mapToLong(named -> named.result().outcomes().size())
                .sum();
        try (BufferedWriter writer = writer(path)) {
            writer.write("# Ring-" + neighborCount + " 연속 " + crashCount
                    + "노드 동시 Crash 실험\n\n");
            writer.write("- 정적 membership view, 재구성 없음\n");
            writer.write("- 연속된 " + crashCount + "개 장애의 시작점을 한 번씩 회전\n");
            writer.write("- 모든 장애를 DEAD로 탐지한 실행: "
                    + completeRuns + "/" + results.size() + "\n");
            writer.write("- DEAD로 탐지한 장애 노드: "
                    + detectedTargets + "/" + totalTargets + "\n\n");
            if (completeRuns == results.size()) {
                writer.write("Ring-" + neighborCount + "에서는 모든 Crash 대상에 "
                        + "살아 있는 관측자가 하나 이상 남아 모든 장애를 탐지했다.\n");
            } else {
                writer.write("Ring-" + neighborCount + "에서는 일부 Crash 대상의 "
                        + "관측자도 함께 Crash해 해당 노드가 관측되지 않았다.\n");
            }
        }
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    }

    private static String nullableMillis(Long nanos) {
        return nanos == null ? "" : String.format(Locale.ROOT, "%.3f", nanos / 1_000_000.0);
    }

    private static String join(List<Integer> values) {
        return values.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(","));
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String experimentName(Options options) {
        return options.crashCount() == 2
                ? "ring-adjacent-two-node-crash"
                : "ring-consecutive-node-crash";
    }

    private static String runId(Options options, int nodeCount, int firstCrash) {
        String scenario = options.crashCount() == 2
                ? "adjacent"
                : "consecutive-" + options.crashCount();
        return "ring-" + options.neighborCount() + "-" + scenario
                + "-crash-n" + nodeCount + "-p" + firstCrash;
    }

    private record NamedResult(String runId, RingMultiCrashExperiment.Result result) {
    }

    private record Options(
            List<Integer> nodeCounts,
            int neighborCount,
            int crashCount,
            Path outputDirectory
    ) {
        private static Options parse(String[] args) {
            List<Integer> nodeCounts = List.of(5, 8);
            int neighborCount = 1;
            int crashCount = 2;
            Path output = Path.of("results-ring1-adjacent-crash");
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--nodes" -> nodeCounts = Arrays.stream(args[++index].split(","))
                            .map(String::trim)
                            .map(Integer::parseInt)
                            .toList();
                    case "--neighbors" -> neighborCount = Integer.parseInt(args[++index]);
                    case "--crash-count" -> crashCount = Integer.parseInt(args[++index]);
                    case "--output-dir" -> output = Path.of(args[++index]);
                    default -> throw new IllegalArgumentException("Unknown option: " + args[index]);
                }
            }
            if (nodeCounts.isEmpty() || nodeCounts.stream().anyMatch(count -> count < 3)) {
                throw new IllegalArgumentException("node counts must all be at least 3");
            }
            if (neighborCount < 1) {
                throw new IllegalArgumentException("neighbors must be positive");
            }
            boolean exceedsAnyNodeCount = false;
            for (int nodeCount : nodeCounts) {
                if (crashCount > nodeCount) {
                    exceedsAnyNodeCount = true;
                    break;
                }
            }
            if (crashCount < 2 || exceedsAnyNodeCount) {
                throw new IllegalArgumentException(
                        "crash count must be between 2 and every node count");
            }
            return new Options(List.copyOf(nodeCounts), neighborCount, crashCount, output);
        }
    }
}
