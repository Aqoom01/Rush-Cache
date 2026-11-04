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

/** Compares multi-helper failure quorums under partition and independent loss. */
public final class RingVerificationQuorumMain {
    private static final int SEEDS = 30;
    private RingVerificationQuorumMain() {
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
            for (Profile profile : lossProfiles()) {
                for (long seed = 0; seed < SEEDS; seed++) {
                    int target = (int) (seed % nodeCount);
                    rows.add(run(profile, nodeCount, target, seed));
                }
            }
            for (Profile profile : topologyProfiles()) {
                for (int target = 0; target < nodeCount; target++) {
                    rows.add(run(profile, nodeCount, target, target));
                }
            }
            for (CrashProfile profile : crashProfiles()) {
                DistributedRingMembershipExperiment experiment =
                        new DistributedRingMembershipExperiment(
                                verifiedConfig(profile.failedRoundsToDead()),
                                RingMultiCrashExperiment.DEFAULT_FAULT_START,
                                RingMultiCrashExperiment.DEFAULT_DURATION,
                                DistributedRingMembershipExperiment.DEFAULT_PROPAGATION_DELAY,
                                profile.helperCount(),
                                profile.failureQuorum());
                for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                    crashRows.add(new CrashRow(
                            profile.name(),
                            experiment.runConsecutiveCrash(nodeCount, 2, firstCrash, 3)));
                }
            }
        }

        writeManifest(options, rows, crashRows);
        writeRuns(options.outputDirectory().resolve("runs.csv"), rows);
        writeCrashRuns(options.outputDirectory().resolve("crash-runs.csv"), crashRows);
        writeSummary(options.outputDirectory().resolve("summary.md"), rows, crashRows);
        for (Profile profile : allProfiles()) {
            List<Row> selected = rows.stream()
                    .filter(row -> row.profile().equals(profile.name())).toList();
            long safe = selected.stream().filter(Row::safe).count();
            System.out.printf(Locale.ROOT, "%s safe=%d/%d%n",
                    profile.name(), safe, selected.size());
        }
        System.out.println("output=" + options.outputDirectory());
    }

    private static Row run(Profile profile, int nodeCount, int target, long seed) {
        RingPartitionMembershipSafetyExperiment.VerificationPolicy policy =
                new RingPartitionMembershipSafetyExperiment.VerificationPolicy(
                        profile.helperCount(),
                        profile.blockedHelpers(),
                        profile.loss(),
                        profile.failureQuorum(),
                        profile.failedRoundsToDead());
        RingPartitionMembershipSafetyExperiment.Result result =
                new RingPartitionMembershipSafetyExperiment(true, policy, seed)
                        .run(nodeCount, target);
        return new Row(
                profile.name() + "-n" + nodeCount + "-t" + target + "-s" + seed,
                profile.name(),
                result);
    }

    private static FixedDeadlineConfig verifiedConfig(int failedRoundsToDead) {
        FixedDeadlineConfig base = FixedDeadlineConfig.ringVerified();
        return new FixedDeadlineConfig(
                base.heartbeatInterval(), base.deadline(), base.confirm(), base.warmup(),
                base.peerSelectionMode(), base.payloadBytes(), true,
                base.verificationInterval(), failedRoundsToDead);
    }

    private static void writeManifest(
            Options options,
            List<Row> rows,
            List<CrashRow> crashRows
    ) throws IOException {
        try (BufferedWriter writer = writer(options.outputDirectory().resolve("manifest.json"))) {
            writer.write("{\n");
            writer.write("  \"experiment\": \"ring-2-multi-helper-quorum\",\n");
            writer.write("  \"generatedAt\": \"" + Instant.now() + "\",\n");
            writer.write("  \"nodeCounts\": \"" + join(options.nodeCounts()) + "\",\n");
            writer.write("  \"lossSeedsPerNodeCount\": 30,\n");
            writer.write("  \"failedVerificationRoundsToDead\": \"2,3,4\",\n");
            writer.write("  \"randomSource\": \"java.util.SplittableRandom\",\n");
            writer.write("  \"outcomeSource\": \"simulation-observed\",\n");
            writer.write("  \"trafficSource\": \"model-estimated\",\n");
            writer.write("  \"partitionRuns\": " + rows.size() + ",\n");
            writer.write("  \"crashRuns\": " + crashRows.size() + "\n");
            writer.write("}\n");
        }
    }

    private static void writeRuns(Path path, List<Row> rows) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,profile,node_count,target,helper_count,blocked_helpers,");
            writer.write("helper_loss,failure_quorum,failed_rounds_to_dead,seed,safe,");
            writer.write("false_dead_events,removed_view_count,verification_round_successes,");
            writer.write("verification_round_failures,helper_probe_successes,");
            writer.write("helper_probe_failures,heartbeat_messages,verification_messages,");
            writer.write("membership_messages,total_messages,total_bytes,outcome_source,");
            writer.write("traffic_source\n");
            for (Row row : rows) {
                RingPartitionMembershipSafetyExperiment.Result result = row.result();
                writer.write(String.join(",",
                        csv(row.runId()), csv(row.profile()),
                        Integer.toString(result.nodeCount()), csv(result.target()),
                        Integer.toString(result.helperCount()),
                        Integer.toString(result.blockedHelperCount()),
                        Double.toString(result.helperPacketLoss()),
                        Integer.toString(result.helperFailureQuorum()),
                        Integer.toString(result.failedVerificationsToDead()),
                        Long.toString(result.seed()), Boolean.toString(row.safe()),
                        Long.toString(result.falseDeadEvents()),
                        Long.toString(result.removedViewCount()),
                        Long.toString(result.verificationSuccesses()),
                        Long.toString(result.verificationFailures()),
                        Long.toString(result.helperProbeSuccesses()),
                        Long.toString(result.helperProbeFailures()),
                        Long.toString(result.heartbeatMessages()),
                        Long.toString(result.verificationMessages()),
                        Long.toString(result.membershipControlMessages()),
                        Long.toString(result.totalMessages()),
                        Long.toString(result.totalBytes()),
                        csv("simulation-observed"), csv("model-estimated")));
                writer.newLine();
            }
        }
    }

    private static void writeCrashRuns(Path path, List<CrashRow> rows) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("profile,node_count,helper_count,failure_quorum,all_detected,");
            writer.write("last_dead_latency_ms,view_convergence_latency_ms,");
            writer.write("heartbeat_messages,verification_messages,membership_messages,");
            writer.write("total_messages,outcome_source,traffic_source\n");
            for (CrashRow row : rows) {
                DistributedRingMembershipExperiment.Result result = row.result();
                long total = result.heartbeatMessages() + result.verificationMessages()
                        + result.membershipControlMessages();
                writer.write(String.join(",",
                        csv(row.profile()), Integer.toString(result.nodeCount()),
                        Integer.toString(result.verificationHelperCount()),
                        Integer.toString(result.verificationFailureQuorum()),
                        Boolean.toString(result.allDetectedDead()),
                        millis(result.lastDeadLatencyNanos()),
                        millis(result.viewConvergenceLatencyNanos()),
                        Long.toString(result.heartbeatMessages()),
                        Long.toString(result.verificationMessages()),
                        Long.toString(result.membershipControlMessages()),
                        Long.toString(total), csv("simulation-observed"),
                        csv("model-estimated")));
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
            writer.write("# 실험 11 — Ring-2 다중 Verify helper·실패 quorum\n\n");
            writer.write("H/Q는 helper H개 중 한 라운드를 실패로 기록하는 데 필요한 ");
            writer.write("실패 helper 수 Q이며, R은 DEAD 확정에 필요한 연속 실패 ");
            writer.write("라운드 수다.\n\n");
            writer.write("| 프로필 | 안전 실행 | false DEAD 실행 | 평균 Verify 메시지 |\n");
            writer.write("|---|---:|---:|---:|\n");
            for (Profile profile : allProfiles()) {
                List<Row> selected = rows.stream()
                        .filter(row -> row.profile().equals(profile.name())).toList();
                long safe = selected.stream().filter(Row::safe).count();
                double averageVerificationMessages = selected.stream()
                        .mapToLong(row -> row.result().verificationMessages())
                        .average().orElse(0.0);
                writer.write(String.format(Locale.ROOT,
                        "| %s | %d/%d | %d/%d | %.1f |%n",
                        profile.name(), safe, selected.size(), selected.size() - safe,
                        selected.size(), averageVerificationMessages));
            }
            writer.write("\nLoss 프로필은 5·8노드 각각 30 seed다. Topology 프로필은 ");
            writer.write("모든 대상 위치 13개다.\n\n");
            writer.write("| Crash 프로필 | 전체 탐지 | 마지막 DEAD 최대 | 평균 Verify 메시지 |\n");
            writer.write("|---|---:|---:|---:|\n");
            for (CrashProfile profile : crashProfiles()) {
                List<CrashRow> selected = crashRows.stream()
                        .filter(row -> row.profile().equals(profile.name())).toList();
                long complete = selected.stream()
                        .filter(row -> row.result().allDetectedDead()).count();
                long lastDead = selected.stream().map(CrashRow::result)
                        .map(DistributedRingMembershipExperiment.Result::lastDeadLatencyNanos)
                        .max(Long::compareTo).orElseThrow();
                double averageVerificationMessages = selected.stream()
                        .mapToLong(row -> row.result().verificationMessages())
                        .average().orElse(0.0);
                writer.write(String.format(Locale.ROOT,
                        "| %s | %d/%d | %s ms | %.1f |%n",
                        profile.name(), complete, selected.size(), millis(lastDead),
                        averageVerificationMessages));
            }
            writer.write("\n판정·지연은 `simulation-observed`, 메시지·바이트 비용은 ");
            writer.write("`model-estimated`다.\n");
        }
    }

    private static List<Profile> lossProfiles() {
        return List.of(
                new Profile("H1-Q1-R2-loss15", 1, 0, 0.15, 1, 2),
                new Profile("H2-Q2-R2-loss15", 2, 0, 0.15, 2, 2),
                new Profile("H3-Q2-R2-loss15", 3, 0, 0.15, 2, 2),
                new Profile("H3-Q3-R2-loss15", 3, 0, 0.15, 3, 2),
                new Profile("H1-Q1-R2-loss45", 1, 0, 0.45, 1, 2),
                new Profile("H2-Q2-R2-loss45", 2, 0, 0.45, 2, 2),
                new Profile("H2-Q2-R3-loss45", 2, 0, 0.45, 2, 3),
                new Profile("H2-Q2-R4-loss45", 2, 0, 0.45, 2, 4),
                new Profile("H3-Q2-R2-loss45", 3, 0, 0.45, 2, 2),
                new Profile("H3-Q3-R2-loss45", 3, 0, 0.45, 3, 2),
                new Profile("H3-Q3-R3-loss45", 3, 0, 0.45, 3, 3),
                new Profile("H3-Q3-R4-loss45", 3, 0, 0.45, 3, 4));
    }

    private static List<Profile> topologyProfiles() {
        return List.of(
                new Profile("H2-Q2-R2-one-blocked", 2, 1, 0.0, 2, 2),
                new Profile("H2-Q2-R2-all-blocked", 2, 2, 0.0, 2, 2),
                new Profile("H3-Q2-R2-two-blocked", 3, 2, 0.0, 2, 2),
                new Profile("H3-Q3-R2-two-blocked", 3, 2, 0.0, 3, 2));
    }

    private static List<Profile> allProfiles() {
        List<Profile> profiles = new ArrayList<>(lossProfiles());
        profiles.addAll(topologyProfiles());
        return List.copyOf(profiles);
    }

    private static List<CrashProfile> crashProfiles() {
        return List.of(
                new CrashProfile("H1-Q1-R2", 1, 1, 2),
                new CrashProfile("H2-Q2-R2", 2, 2, 2),
                new CrashProfile("H2-Q2-R3", 2, 2, 3),
                new CrashProfile("H2-Q2-R4", 2, 2, 4),
                new CrashProfile("H3-Q3-R2", 3, 3, 2),
                new CrashProfile("H3-Q3-R3", 3, 3, 3),
                new CrashProfile("H3-Q3-R4", 3, 3, 4));
    }

    private static String millis(Long nanos) {
        return nanos == null ? "" : String.format(Locale.ROOT, "%.3f", nanos / 1_000_000.0);
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

    private record Profile(
            String name,
            int helperCount,
            int blockedHelpers,
            double loss,
            int failureQuorum,
            int failedRoundsToDead
    ) {
    }

    private record CrashProfile(
            String name,
            int helperCount,
            int failureQuorum,
            int failedRoundsToDead
    ) {
    }

    private record Row(
            String runId,
            String profile,
            RingPartitionMembershipSafetyExperiment.Result result
    ) {
        private boolean safe() {
            return result.removedViewCount() == 0;
        }
    }

    private record CrashRow(
            String profile,
            DistributedRingMembershipExperiment.Result result
    ) {
    }

    private record Options(List<Integer> nodeCounts, Path outputDirectory) {
        private static Options parse(String[] args) {
            List<Integer> nodeCounts = List.of(5, 8);
            Path output = Path.of("results-ring-verification-quorum");
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--nodes" -> nodeCounts = Arrays.stream(args[++index].split(","))
                            .map(String::trim).map(Integer::parseInt).toList();
                    case "--output-dir" -> output = Path.of(args[++index]);
                    default -> throw new IllegalArgumentException(
                            "Unknown option: " + args[index]);
                }
            }
            return new Options(List.copyOf(nodeCounts), output);
        }
    }
}
