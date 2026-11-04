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

/** Sweeps helper partition topology and seeded verification packet loss. */
public final class RingPartitionRobustnessMain {
    private static final int SEEDS = 30;

    private RingPartitionRobustnessMain() {
    }

    public static void main(String[] args) throws IOException {
        Options options = Options.parse(args);
        if (Files.exists(options.outputDirectory())) {
            throw new IllegalArgumentException(
                    "Output directory already exists; existing results are preserved: "
                            + options.outputDirectory());
        }
        Files.createDirectories(options.outputDirectory());

        List<Row> rows = new ArrayList<>();
        List<CrashRow> crashRows = new ArrayList<>();
        for (int nodeCount : options.nodeCounts()) {
            for (int target = 0; target < nodeCount; target++) {
                rows.add(run("helper-reachable", nodeCount, target, true, 0.0, target));
                rows.add(run("helper-blocked", nodeCount, target, false, 0.0, target));
            }
            for (long seed = 0; seed < SEEDS; seed++) {
                int target = (int) (seed % nodeCount);
                for (int threshold : List.of(2, 3, 4)) {
                    rows.add(run("helper-loss-15-v" + threshold,
                            nodeCount, target, true, 0.15, seed, threshold));
                    rows.add(run("helper-loss-45-v" + threshold,
                            nodeCount, target, true, 0.45, seed, threshold));
                }
            }
            for (int threshold : List.of(2, 3, 4)) {
                DistributedRingMembershipExperiment crashExperiment =
                        new DistributedRingMembershipExperiment(
                                verifiedConfig(threshold),
                                RingMultiCrashExperiment.DEFAULT_FAULT_START,
                                RingMultiCrashExperiment.DEFAULT_DURATION,
                                DistributedRingMembershipExperiment.DEFAULT_PROPAGATION_DELAY);
                for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                    crashRows.add(new CrashRow(
                            threshold,
                            crashExperiment.runConsecutiveCrash(
                                    nodeCount, 2, firstCrash, 3)));
                }
            }
        }

        writeManifest(options, rows);
        writeRuns(options.outputDirectory().resolve("runs.csv"), rows);
        writeCrashRuns(options.outputDirectory().resolve("crash-runs.csv"), crashRows);
        writeSummary(options.outputDirectory().resolve("summary.md"), rows, crashRows);
        for (String profile : profiles()) {
            List<Row> selected = rows.stream()
                    .filter(row -> row.profile().equals(profile)).toList();
            long safe = selected.stream().filter(row -> row.result().removedViewCount() == 0)
                    .count();
            System.out.printf(Locale.ROOT, "%s safe=%d/%d%n", profile, safe, selected.size());
        }
        System.out.println("output=" + options.outputDirectory());
    }

    private static FixedDeadlineConfig verifiedConfig(int threshold) {
        FixedDeadlineConfig base = FixedDeadlineConfig.ringVerified();
        return new FixedDeadlineConfig(
                base.heartbeatInterval(), base.deadline(), base.confirm(), base.warmup(),
                base.peerSelectionMode(), base.payloadBytes(), true,
                base.verificationInterval(), threshold);
    }

    private static Row run(
            String profile,
            int nodeCount,
            int target,
            boolean helperReachable,
            double helperLoss,
            long seed
    ) {
        return run(profile, nodeCount, target, helperReachable, helperLoss, seed, 2);
    }

    private static Row run(
            String profile,
            int nodeCount,
            int target,
            boolean helperReachable,
            double helperLoss,
            long seed,
            int threshold
    ) {
        return new Row(
                profile + "-n" + nodeCount + "-t" + target + "-s" + seed,
                profile,
                new RingPartitionMembershipSafetyExperiment(
                        true, helperReachable, helperLoss, seed, threshold).run(nodeCount, target)
        );
    }

    private static void writeManifest(Options options, List<Row> rows) throws IOException {
        try (BufferedWriter writer = writer(options.outputDirectory().resolve("manifest.json"))) {
            writer.write("{\n");
            writer.write("  \"experiment\": \"ring-2-partition-helper-robustness\",\n");
            writer.write("  \"generatedAt\": \"" + Instant.now() + "\",\n");
            writer.write("  \"nodeCounts\": \"" + join(options.nodeCounts()) + "\",\n");
            writer.write("  \"lossSeedsPerNodeCount\": 30,\n");
            writer.write("  \"helperLossRates\": \"0.15,0.45\",\n");
            writer.write("  \"randomSource\": \"java.util.SplittableRandom\",\n");
            writer.write("  \"latencySource\": \"simulation-observed\",\n");
            writer.write("  \"trafficSource\": \"model-estimated\",\n");
            writer.write("  \"runs\": " + rows.size() + "\n");
            writer.write("}\n");
        }
    }

    private static void writeRuns(Path path, List<Row> rows) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,profile,node_count,target,helper_reachable,helper_loss,seed,");
            writer.write("failure_threshold,safe,false_dead_events,removed_view_count,verification_successes,");
            writer.write("verification_failures,total_messages,total_bytes,latency_source,");
            writer.write("traffic_source\n");
            for (Row row : rows) {
                RingPartitionMembershipSafetyExperiment.Result result = row.result();
                writer.write(String.join(",",
                        csv(row.runId()), csv(row.profile()),
                        Integer.toString(result.nodeCount()), csv(result.target()),
                        Boolean.toString(result.helperReachable()),
                        Double.toString(result.helperPacketLoss()),
                        Long.toString(result.seed()),
                        Integer.toString(result.failedVerificationsToDead()),
                        Boolean.toString(result.removedViewCount() == 0),
                        Long.toString(result.falseDeadEvents()),
                        Long.toString(result.removedViewCount()),
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
            writer.write("failure_threshold,node_count,all_detected,last_dead_latency_ms,");
            writer.write("view_convergence_latency_ms,total_messages\n");
            for (CrashRow row : rows) {
                DistributedRingMembershipExperiment.Result result = row.result();
                writer.write(String.join(",",
                        Integer.toString(row.threshold()),
                        Integer.toString(result.nodeCount()),
                        Boolean.toString(result.allDetectedDead()),
                        millis(result.lastDeadLatencyNanos()),
                        millis(result.viewConvergenceLatencyNanos()),
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
            List<Row> rows,
            List<CrashRow> crashRows
    ) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("# 실험 10 — Verify helper Partition·Loss 강건성\n\n");
            writer.write("| 프로필 | 안전 실행 | false DEAD 실행 |\n");
            writer.write("|---|---:|---:|\n");
            for (String profile : profiles()) {
                List<Row> selected = rows.stream()
                        .filter(row -> row.profile().equals(profile)).toList();
                long safe = selected.stream()
                        .filter(row -> row.result().removedViewCount() == 0).count();
                writer.write("| " + profile + " | " + safe + "/" + selected.size()
                        + " | " + (selected.size() - safe) + "/" + selected.size()
                        + " |\n");
            }
            writer.write("\nLoss 프로필은 5·8노드 각각 30 seed다. 상태는 "
                    + "`simulation-observed`, 비용은 `model-estimated`다.\n");
            writer.write("\n| Verify 실패 임계값 | Crash 전체 탐지 | 마지막 DEAD 최대 |\n");
            writer.write("|---:|---:|---:|\n");
            for (int threshold : List.of(2, 3, 4)) {
                List<CrashRow> selected = crashRows.stream()
                        .filter(row -> row.threshold() == threshold).toList();
                long complete = selected.stream()
                        .filter(row -> row.result().allDetectedDead()).count();
                long lastDead = selected.stream()
                        .map(CrashRow::result)
                        .map(DistributedRingMembershipExperiment.Result::lastDeadLatencyNanos)
                        .max(Long::compareTo).orElseThrow();
                writer.write("| " + threshold + " | " + complete + "/" + selected.size()
                        + " | " + millis(lastDead) + "ms |\n");
            }
        }
    }

    private static String millis(Long nanos) {
        return nanos == null ? "" : String.format(Locale.ROOT, "%.3f", nanos / 1_000_000.0);
    }

    private static List<String> profiles() {
        return List.of(
                "helper-reachable",
                "helper-blocked",
                "helper-loss-15-v2",
                "helper-loss-15-v3",
                "helper-loss-15-v4",
                "helper-loss-45-v2",
                "helper-loss-45-v3",
                "helper-loss-45-v4"
        );
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    }

    private static String join(List<Integer> values) {
        return values.stream().map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(","));
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private record Row(
            String runId,
            String profile,
            RingPartitionMembershipSafetyExperiment.Result result
    ) {
    }

    private record CrashRow(
            int threshold,
            DistributedRingMembershipExperiment.Result result
    ) {
    }

    private record Options(List<Integer> nodeCounts, Path outputDirectory) {
        private static Options parse(String[] args) {
            List<Integer> nodeCounts = List.of(5, 8);
            Path output = Path.of("results-ring-partition-robustness");
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--nodes" -> nodeCounts = Arrays.stream(args[++index].split(","))
                            .map(String::trim).map(Integer::parseInt).toList();
                    case "--output-dir" -> output = Path.of(args[++index]);
                    default -> throw new IllegalArgumentException("Unknown option: " + args[index]);
                }
            }
            return new Options(List.copyOf(nodeCounts), output);
        }
    }
}
