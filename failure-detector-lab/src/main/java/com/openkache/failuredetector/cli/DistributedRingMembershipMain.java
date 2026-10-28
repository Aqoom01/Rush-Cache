package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.experiment.DistributedRingMembershipExperiment;
import com.openkache.failuredetector.experiment.RingMembershipReconfigurationExperiment;
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

/** Runs delayed per-node Ring-2 membership dissemination experiments. */
public final class DistributedRingMembershipMain {
    private DistributedRingMembershipMain() {
    }

    public static void main(String[] args) throws IOException {
        Options options = Options.parse(args);
        if (Files.exists(options.outputDirectory())) {
            throw new IllegalArgumentException(
                    "Output directory already exists; existing results are preserved: "
                            + options.outputDirectory());
        }
        Files.createDirectories(options.outputDirectory());

        DistributedRingMembershipExperiment experiment =
                new DistributedRingMembershipExperiment();
        List<NamedResult> results = new ArrayList<>();
        for (int nodeCount : options.nodeCounts()) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                results.add(new NamedResult(
                        "ring-2-delayed-view-n" + nodeCount + "-p" + firstCrash,
                        experiment.runConsecutiveCrash(nodeCount, 2, firstCrash, 3)
                ));
            }
        }

        writeManifest(options, results);
        writeRuns(options.outputDirectory().resolve("runs.csv"), results);
        writeTargets(options.outputDirectory().resolve("targets.csv"), results);
        writeDetectorEvents(options.outputDirectory().resolve("detector-events.jsonl"), results);
        writeViewEvents(options.outputDirectory().resolve("view-events.jsonl"), results);
        writeControlEvents(options.outputDirectory().resolve("control-events.jsonl"), results);
        writeSummary(options.outputDirectory().resolve("summary.md"), results);

        long complete = results.stream().filter(row -> row.result().allDetectedDead()).count();
        long detected = results.stream().mapToLong(row -> row.result().detectedDeadCount()).sum();
        System.out.printf(Locale.ROOT,
                "ring-2-delayed-view: completeRuns=%d/%d, detectedTargets=%d/%d, output=%s%n",
                complete, results.size(), detected, results.size() * 3,
                options.outputDirectory());
    }

    private static void writeManifest(Options options, List<NamedResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(options.outputDirectory().resolve("manifest.json"))) {
            writer.write("{\n");
            writer.write("  \"experiment\": \"distributed-ring-2-membership-reconfiguration\",\n");
            writer.write("  \"generatedAt\": \"" + Instant.now() + "\",\n");
            writer.write("  \"nodeCounts\": \"" + join(options.nodeCounts()) + "\",\n");
            writer.write("  \"crashCount\": 3,\n");
            writer.write("  \"propagationDelayMillis\": 1000,\n");
            writer.write("  \"perNodeViews\": true,\n");
            writer.write("  \"versioning\": \"lamport-counter-plus-origin\",\n");
            writer.write("  \"reconfigurationDecisionSource\": \"detector-dead-events\",\n");
            writer.write("  \"injectedCrashSetUsedForDecision\": false,\n");
            writer.write("  \"latencySource\": \"simulation-observed\",\n");
            writer.write("  \"trafficSource\": \"model-estimated\",\n");
            writer.write("  \"runs\": " + results.size() + "\n");
            writer.write("}\n");
        }
    }

    private static void writeRuns(Path path, List<NamedResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,node_count,detected_dead,target_count,all_detected,");
            writer.write("last_dead_latency_ms,view_convergence_latency_ms,");
            writer.write("heartbeat_messages,membership_control_messages,total_messages,");
            writer.write("verification_messages,heartbeat_bytes,membership_control_bytes,");
            writer.write("verification_bytes,total_bytes,");
            writer.write("stale_or_duplicate_deliveries,latency_source,traffic_source\n");
            for (NamedResult row : results) {
                DistributedRingMembershipExperiment.Result result = row.result();
                writer.write(String.join(",",
                        csv(row.runId()),
                        Integer.toString(result.nodeCount()),
                        Long.toString(result.detectedDeadCount()),
                        Integer.toString(result.outcomes().size()),
                        Boolean.toString(result.allDetectedDead()),
                        millis(result.lastDeadLatencyNanos()),
                        millis(result.viewConvergenceLatencyNanos()),
                        Long.toString(result.heartbeatMessages()),
                        Long.toString(result.membershipControlMessages()),
                        Long.toString(result.heartbeatMessages()
                                + result.membershipControlMessages()
                                + result.verificationMessages()),
                        Long.toString(result.verificationMessages()),
                        Long.toString(result.heartbeatBytes()),
                        Long.toString(result.membershipControlBytes()),
                        Long.toString(result.verificationBytes()),
                        Long.toString(result.heartbeatBytes()
                                + result.membershipControlBytes()
                                + result.verificationBytes()),
                        Long.toString(result.staleOrDuplicateDeliveries()),
                        csv("simulation-observed"),
                        csv("model-estimated")
                ));
                writer.newLine();
            }
        }
    }

    private static void writeTargets(Path path, List<NamedResult> results) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,target,suspect_latency_ms,dead_latency_ms,");
            writer.write("first_live_observer_latency_ms,detected_dead\n");
            for (NamedResult row : results) {
                for (DistributedRingMembershipExperiment.TargetOutcome target
                        : row.result().outcomes()) {
                    writer.write(String.join(",",
                            csv(row.runId()),
                            csv(target.target()),
                            millis(target.suspectLatencyNanos()),
                            millis(target.deadLatencyNanos()),
                            millis(target.firstLiveObserverLatencyNanos()),
                            Boolean.toString(target.detectedDead())
                    ));
                    writer.newLine();
                }
            }
        }
    }

    private static void writeDetectorEvents(Path path, List<NamedResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (NamedResult row : results) {
                for (DetectorEvent event : row.result().detectorEvents()) {
                    writer.write("{\"runId\":\"" + json(row.runId()) + "\""
                            + ",\"atNanos\":" + event.atNanos()
                            + ",\"observer\":\"" + json(event.observer()) + "\""
                            + ",\"target\":\"" + json(event.target()) + "\""
                            + ",\"newStatus\":\"" + event.newStatus() + "\"}");
                    writer.newLine();
                }
            }
        }
    }

    private static void writeViewEvents(Path path, List<NamedResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (NamedResult row : results) {
                for (DistributedRingMembershipExperiment.NodeViewEvent event
                        : row.result().viewEvents()) {
                    writer.write("{\"runId\":\"" + json(row.runId()) + "\""
                            + ",\"atNanos\":" + event.atNanos()
                            + ",\"node\":\"" + json(event.node()) + "\""
                            + ",\"viewVersion\":" + event.viewVersion()
                            + ",\"removedMember\":\"" + json(event.removedMember()) + "\""
                            + ",\"membersAfter\":" + jsonArray(event.membersAfter())
                            + ",\"stampLogicalTime\":" + event.stamp().logicalTime()
                            + ",\"stampOrigin\":\"" + json(event.stamp().origin()) + "\""
                            + ",\"source\":\"" + json(event.source()) + "\"}");
                    writer.newLine();
                }
            }
        }
    }

    private static void writeControlEvents(Path path, List<NamedResult> results)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (NamedResult row : results) {
                for (DistributedRingMembershipExperiment.ControlDeliveryEvent event
                        : row.result().deliveryEvents()) {
                    writer.write("{\"runId\":\"" + json(row.runId()) + "\""
                            + ",\"atNanos\":" + event.atNanos()
                            + ",\"sender\":\"" + json(event.sender()) + "\""
                            + ",\"receiver\":\"" + json(event.receiver()) + "\""
                            + ",\"target\":\"" + json(event.target()) + "\""
                            + ",\"outcome\":\"" + json(event.outcome()) + "\"}");
                    writer.newLine();
                }
            }
        }
    }

    private static void writeSummary(Path path, List<NamedResult> results) throws IOException {
        RingMultiCrashExperiment staticExperiment = new RingMultiCrashExperiment();
        RingMembershipReconfigurationExperiment idealExperiment =
                new RingMembershipReconfigurationExperiment();
        try (BufferedWriter writer = writer(path)) {
            writer.write("# 실험 8-2 — 분산 Ring-2 Membership 재구성\n\n");
            writer.write("- 노드별 독립 View, DEAD update 직접 broadcast\n");
            writer.write("- 전파 지연 1초, remove-only versioned tombstone\n");
            writer.write("- 중복·오래된 update는 View를 되돌리지 않음\n\n");
            writer.write("| 프로필 | 전체 탐지 | 마지막 DEAD | View 수렴 |\n");
            writer.write("|---|---:|---:|---:|\n");
            writer.write("| Ring-2 Static | 0/13 | 미완료 | 해당 없음 |\n");
            writer.write("| Ring-2 즉시 공유 View | 13/13 | 9초 | 9초 |\n");
            writer.write("| Ring-2 지연 분산 View | 13/13 | 9초 | 10초 |\n");
            writer.write("| Ring-3 Static | 13/13 | 4초 | 해당 없음 |\n\n");
            writer.write("| 노드 | Static Ring-2 | 즉시 Ring-2 | 지연 Ring-2 total | Ring-3 |\n");
            writer.write("|---:|---:|---:|---:|---:|\n");
            for (int nodeCount : List.of(5, 8)) {
                RingMultiCrashExperiment.Result staticTwo =
                        staticExperiment.runConsecutiveCrash(nodeCount, 2, 0, 3);
                RingMembershipReconfigurationExperiment.Result ideal =
                        idealExperiment.runConsecutiveCrash(nodeCount, 2, 0, 3);
                DistributedRingMembershipExperiment.Result distributed = results.stream()
                        .map(NamedResult::result)
                        .filter(result -> result.nodeCount() == nodeCount)
                        .findFirst().orElseThrow();
                RingMultiCrashExperiment.Result staticThree =
                        staticExperiment.runConsecutiveCrash(nodeCount, 3, 0, 3);
                writer.write("| " + nodeCount + " | " + staticTwo.protocolMessages()
                        + " | " + ideal.heartbeatMessages()
                        + " | " + (distributed.heartbeatMessages()
                        + distributed.membershipControlMessages())
                        + " | " + staticThree.protocolMessages() + " |\n");
            }
            writer.write("\n시간은 `simulation-observed`, Heartbeat와 membership "
                    + "메시지는 `model-estimated`다. 실제 UDP 결과가 아니다.\n");
        }
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    }

    private static String millis(Long nanos) {
        return nanos == null ? "" : String.format(Locale.ROOT, "%.3f", nanos / 1_000_000.0);
    }

    private static String join(List<Integer> values) {
        return values.stream().map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(","));
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String jsonArray(List<String> values) {
        return values.stream().map(value -> "\"" + json(value) + "\"")
                .collect(java.util.stream.Collectors.joining(",", "[", "]"));
    }

    private record NamedResult(
            String runId,
            DistributedRingMembershipExperiment.Result result
    ) {
    }

    private record Options(List<Integer> nodeCounts, Path outputDirectory) {
        private static Options parse(String[] args) {
            List<Integer> nodeCounts = List.of(5, 8);
            Path output = Path.of("results-ring-distributed-membership");
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--nodes" -> nodeCounts = Arrays.stream(args[++index].split(","))
                            .map(String::trim).map(Integer::parseInt).toList();
                    case "--output-dir" -> output = Path.of(args[++index]);
                    default -> throw new IllegalArgumentException("Unknown option: " + args[index]);
                }
            }
            if (nodeCounts.isEmpty() || nodeCounts.stream().anyMatch(count -> count < 5)) {
                throw new IllegalArgumentException("node counts must all be at least 5");
            }
            return new Options(List.copyOf(nodeCounts), output);
        }
    }
}
