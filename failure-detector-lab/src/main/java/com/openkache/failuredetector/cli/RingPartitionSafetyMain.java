package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.deadline.FixedDeadlineConfig;
import com.openkache.failuredetector.experiment.DistributedRingMembershipExperiment;
import com.openkache.failuredetector.experiment.RingMultiCrashExperiment;
import com.openkache.failuredetector.experiment.RingPartitionMembershipSafetyExperiment;

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

/** Compares distributed Ring-2 membership removal with and without Verify Suspect. */
public final class RingPartitionSafetyMain {
    private RingPartitionSafetyMain() {
    }

    public static void main(String[] args) throws IOException {
        Options options = Options.parse(args);
        if (Files.exists(options.outputDirectory())) {
            throw new IllegalArgumentException(
                    "Output directory already exists; existing results are preserved: "
                            + options.outputDirectory());
        }
        Files.createDirectories(options.outputDirectory());

        List<PartitionRow> partitionRows = new ArrayList<>();
        for (int nodeCount : options.nodeCounts()) {
            for (int target = 0; target < nodeCount; target++) {
                partitionRows.add(new PartitionRow(
                        "ring-2-no-verify-n" + nodeCount + "-t" + target,
                        "ring-2-no-verify",
                        new RingPartitionMembershipSafetyExperiment(false)
                                .run(nodeCount, target)));
                partitionRows.add(new PartitionRow(
                        "ring-2-verify-n" + nodeCount + "-t" + target,
                        "ring-2-verify",
                        new RingPartitionMembershipSafetyExperiment(true)
                                .run(nodeCount, target)));
            }
        }

        DistributedRingMembershipExperiment verifiedCrash =
                new DistributedRingMembershipExperiment(
                        FixedDeadlineConfig.ringVerified(),
                        RingMultiCrashExperiment.DEFAULT_FAULT_START,
                        RingMultiCrashExperiment.DEFAULT_DURATION,
                        DistributedRingMembershipExperiment.DEFAULT_PROPAGATION_DELAY);
        List<CrashRow> crashRows = new ArrayList<>();
        for (int nodeCount : options.nodeCounts()) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                crashRows.add(new CrashRow(
                        "ring-2-verify-crash-n" + nodeCount + "-p" + firstCrash,
                        verifiedCrash.runConsecutiveCrash(nodeCount, 2, firstCrash, 3)));
            }
        }

        writeManifest(options, partitionRows, crashRows);
        writePartitionRuns(options.outputDirectory().resolve("partition-runs.csv"), partitionRows);
        writeCrashRuns(options.outputDirectory().resolve("crash-runs.csv"), crashRows);
        writeSummary(options.outputDirectory().resolve("summary.md"), partitionRows, crashRows);

        long unsafe = partitionRows.stream()
                .filter(row -> row.profile().equals("ring-2-no-verify"))
                .filter(row -> row.result().removedViewCount() > 0).count();
        long safe = partitionRows.stream()
                .filter(row -> row.profile().equals("ring-2-verify"))
                .filter(row -> row.result().removedViewCount() == 0).count();
        long crashComplete = crashRows.stream()
                .filter(row -> row.result().allDetectedDead()).count();
        System.out.printf(Locale.ROOT,
                "partition unsafe(no-verify)=%d/13, safe(verify)=%d/13, "
                        + "verifiedCrash=%d/13, output=%s%n",
                unsafe, safe, crashComplete, options.outputDirectory());
    }

    private static void writeManifest(
            Options options,
            List<PartitionRow> partitionRows,
            List<CrashRow> crashRows
    ) throws IOException {
        try (BufferedWriter writer = writer(options.outputDirectory().resolve("manifest.json"))) {
            writer.write("{\n");
            writer.write("  \"experiment\": \"ring-2-partition-membership-safety\",\n");
            writer.write("  \"generatedAt\": \"" + Instant.now() + "\",\n");
            writer.write("  \"nodeCounts\": \"" + join(options.nodeCounts()) + "\",\n");
            writer.write("  \"partitionStartSeconds\": 20,\n");
            writer.write("  \"partitionEndSeconds\": 32,\n");
            writer.write("  \"partitionModel\": \"ring-observers-blocked-helper-reachable\",\n");
            writer.write("  \"membershipPropagationDelayMillis\": 1000,\n");
            writer.write("  \"partitionRuns\": " + partitionRows.size() + ",\n");
            writer.write("  \"verifiedCrashRuns\": " + crashRows.size() + ",\n");
            writer.write("  \"latencySource\": \"simulation-observed\",\n");
            writer.write("  \"trafficSource\": \"model-estimated\"\n");
            writer.write("}\n");
        }
    }

    private static void writePartitionRuns(Path path, List<PartitionRow> rows)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,profile,node_count,target,false_dead_events,");
            writer.write("removed_view_count,false_removal_convergence_ms,");
            writer.write("heartbeat_messages,verification_messages,membership_messages,");
            writer.write("verification_successes,verification_failures,");
            writer.write("total_messages,total_bytes,latency_source,traffic_source\n");
            for (PartitionRow row : rows) {
                RingPartitionMembershipSafetyExperiment.Result result = row.result();
                writer.write(String.join(",",
                        csv(row.runId()), csv(row.profile()),
                        Integer.toString(result.nodeCount()), csv(result.target()),
                        Long.toString(result.falseDeadEvents()),
                        Long.toString(result.removedViewCount()),
                        millis(result.falseRemovalConvergenceLatencyNanos()),
                        Long.toString(result.heartbeatMessages()),
                        Long.toString(result.verificationMessages()),
                        Long.toString(result.membershipControlMessages()),
                        Long.toString(result.verificationSuccesses()),
                        Long.toString(result.verificationFailures()),
                        Long.toString(result.totalMessages()),
                        Long.toString(result.totalBytes()),
                        csv("simulation-observed"), csv("model-estimated")
                ));
                writer.newLine();
            }
        }
    }

    private static void writeCrashRuns(Path path, List<CrashRow> rows) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,node_count,detected_dead,target_count,all_detected,");
            writer.write("last_dead_latency_ms,view_convergence_latency_ms,");
            writer.write("heartbeat_messages,verification_messages,membership_messages,");
            writer.write("total_messages\n");
            for (CrashRow row : rows) {
                DistributedRingMembershipExperiment.Result result = row.result();
                writer.write(String.join(",",
                        csv(row.runId()), Integer.toString(result.nodeCount()),
                        Long.toString(result.detectedDeadCount()),
                        Integer.toString(result.outcomes().size()),
                        Boolean.toString(result.allDetectedDead()),
                        millis(result.lastDeadLatencyNanos()),
                        millis(result.viewConvergenceLatencyNanos()),
                        Long.toString(result.heartbeatMessages()),
                        Long.toString(result.verificationMessages()),
                        Long.toString(result.membershipControlMessages()),
                        Long.toString(result.heartbeatMessages()
                                + result.verificationMessages()
                                + result.membershipControlMessages())
                ));
                writer.newLine();
            }
        }
    }

    private static void writeSummary(
            Path path,
            List<PartitionRow> partitionRows,
            List<CrashRow> crashRows
    ) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("# 실험 9 — Ring-2 Partition Membership 안전성\n\n");
            writer.write("| 프로필 | Partition 안전 실행 | false DEAD | 잘못 제거된 View |\n");
            writer.write("|---|---:|---:|---:|\n");
            for (String profile : List.of("ring-2-no-verify", "ring-2-verify")) {
                List<PartitionRow> selected = partitionRows.stream()
                        .filter(row -> row.profile().equals(profile)).toList();
                long safe = selected.stream()
                        .filter(row -> row.result().removedViewCount() == 0).count();
                long falseDead = selected.stream()
                        .mapToLong(row -> row.result().falseDeadEvents()).sum();
                long removed = selected.stream()
                        .mapToLong(row -> row.result().removedViewCount()).sum();
                writer.write("| " + profile + " | " + safe + "/" + selected.size()
                        + " | " + falseDead + " | " + removed + " |\n");
            }
            long crashComplete = crashRows.stream()
                    .filter(row -> row.result().allDetectedDead()).count();
            long crashDetected = crashRows.stream()
                    .mapToLong(row -> row.result().detectedDeadCount()).sum();
            writer.write("\nRing-2 + Verify Crash 회귀: " + crashComplete + "/"
                    + crashRows.size() + " 실행, " + crashDetected + "/"
                    + crashRows.size() * 3 + " 장애 탐지.\n\n");
            writer.write("시간·상태는 `simulation-observed`, 메시지·byte는 "
                    + "`model-estimated`다. 실제 UDP partition 결과가 아니다.\n");
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

    private record PartitionRow(
            String runId,
            String profile,
            RingPartitionMembershipSafetyExperiment.Result result
    ) {
    }

    private record CrashRow(
            String runId,
            DistributedRingMembershipExperiment.Result result
    ) {
    }

    private record Options(List<Integer> nodeCounts, Path outputDirectory) {
        private static Options parse(String[] args) {
            List<Integer> nodeCounts = List.of(5, 8);
            Path output = Path.of("results-ring-partition-safety");
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
