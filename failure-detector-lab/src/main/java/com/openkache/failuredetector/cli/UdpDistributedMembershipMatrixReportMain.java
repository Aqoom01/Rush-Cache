package com.openkache.failuredetector.cli;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** Aggregates repeated real-UDP distributed membership runs. */
public final class UdpDistributedMembershipMatrixReportMain {
    private UdpDistributedMembershipMatrixReportMain() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "usage: UdpDistributedMembershipMatrixReportMain MATRIX_DIR");
        }
        generate(Path.of(args[0]));
    }

    static void generate(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("matrix directory does not exist: " + root);
        }
        Path csv = root.resolve("runs.csv");
        Path summary = root.resolve("summary.md");
        Path manifest = root.resolve("manifest.json");
        for (Path output : List.of(csv, summary, manifest)) {
            if (Files.exists(output)) {
                throw new IllegalArgumentException(
                        "report output already exists; existing results are preserved: " + output);
            }
        }

        List<Run> runs = new ArrayList<>();
        try (var paths = Files.list(root)) {
            for (Path run : paths.filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().matches("run-[0-9]+"))
                    .toList()) {
                String name = run.getFileName().toString();
                int repetition = Integer.parseInt(name.substring("run-".length()));
                runs.add(new Run(
                        repetition,
                        UdpDistributedMembershipReportMain.readExperiment(run)));
            }
        }
        runs.sort(Comparator.comparingInt(Run::repetition));
        if (runs.isEmpty()) {
            throw new IllegalArgumentException("matrix has no run directories: " + root);
        }

        writeCsv(csv, runs);
        writeSummary(summary, runs);
        writeManifest(manifest, runs);
        long successful = runs.stream().filter(Run::successful).count();
        System.out.println("UDP distributed membership matrix=" + successful + "/"
                + runs.size() + " output=" + root);
    }

    private static void writeCsv(Path path, List<Run> runs) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("repetition,successful,last_dead_nanos,view_convergence_nanos,");
            writer.write("hidden_target_observed_nanos,membership_control_frames,");
            writer.write("survivor_sent_frames,survivor_sent_bytes,pre_fault_dead_events,");
            writer.write("membership_attempted_frames,membership_dropped_frames,wire_source\n");
            for (Run run : runs) {
                var result = run.result();
                writer.write(String.format(Locale.ROOT,
                        "%d,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,\"real-udp-observed\"%n",
                        run.repetition(), run.successful(), nullable(result.lastDeadNanos()),
                        nullable(result.convergenceNanos()), nullable(result.firstObserved().get(3)),
                        result.membershipControlFrames(), result.survivorSentFrames(),
                        result.survivorSentBytes(), run.preFaultDeadEvents(),
                        result.membershipAttemptedFrames(),
                        result.membershipDroppedFrames()));
            }
        }
    }

    private static void writeSummary(Path path, List<Run> runs) throws IOException {
        long successful = runs.stream().filter(Run::successful).count();
        List<Long> lastDead = values(runs, Value.LAST_DEAD);
        List<Long> convergence = values(runs, Value.CONVERGENCE);
        List<Long> hiddenObserved = values(runs, Value.HIDDEN_OBSERVED);
        long preFaultDead = runs.stream().mapToLong(Run::preFaultDeadEvents).sum();
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("# Ring-2 H3/Q3/R3 실제 UDP 분산 Membership 반복\n\n");
            writer.write("모든 값은 `real-udp-observed`다. 각 실행은 8노드에서 ");
            writer.write("node-1·2·3을 한 UDP injection으로 process crash시키고, ");
            writer.write("detector DEAD 이벤트만으로 View를 갱신했다.\n\n");
            writer.write("Membership control fault: `"
                    + runs.get(0).result().controlFault() + "`\n\n");
            writer.write("| 측정치 | 결과 |\n|---|---:|\n");
            writer.write("| 전체 View 수렴 | " + successful + "/" + runs.size() + " |\n");
            writer.write("| 성공률 95% Wilson CI | "
                    + wilson(successful, runs.size()) + " |\n");
            writer.write("| 사전 false DEAD | " + preFaultDead + " |\n");
            writer.write("| 숨겨진 node-3 관측자 배정 p50 / p95 / p99 | "
                    + percentiles(hiddenObserved) + " |\n");
            writer.write("| 마지막 장애 DEAD p50 / p95 / p99 | "
                    + percentiles(lastDead) + " |\n");
            writer.write("| 전체 View 수렴 p50 / p95 / p99 | "
                    + percentiles(convergence) + " |\n");
            writer.write(String.format(Locale.ROOT,
                    "| 평균 Membership control frame | %.1f |%n",
                    runs.stream().mapToLong(run -> run.result().membershipControlFrames())
                            .average().orElse(0)));
            writer.write(String.format(Locale.ROOT,
                    "| 평균 Membership injected drop | %.1f |%n",
                    runs.stream().mapToLong(run -> run.result().membershipDroppedFrames())
                            .average().orElse(0)));
            writer.write(String.format(Locale.ROOT,
                    "| 평균 생존 노드 송신 frame / byte | %.1f / %.1f |%n",
                    runs.stream().mapToLong(run -> run.result().survivorSentFrames())
                            .average().orElse(0),
                    runs.stream().mapToLong(run -> run.result().survivorSentBytes())
                            .average().orElse(0)));

            List<Integer> failed = runs.stream().filter(run -> !run.successful())
                    .map(Run::repetition).toList();
            writer.write("\n실패 실행: " + (failed.isEmpty() ? "없음" : failed) + "\n\n");
            writer.write("생존 노드 송신량에는 crash된 세 노드의 fault 이전 송신량이 ");
            writer.write("포함되지 않으므로 전체 클러스터 비용으로 해석하지 않는다.\n");
        }
    }

    private static void writeManifest(Path path, List<Run> runs) throws IOException {
        long successful = runs.stream().filter(Run::successful).count();
        Files.writeString(path, "{\n"
                + "  \"experiment\": \"udp-ring2-distributed-membership-matrix\",\n"
                + "  \"generatedAt\": \"" + Instant.now() + "\",\n"
                + "  \"source\": \"real-udp-observed\",\n"
                + "  \"controlFault\": \""
                + runs.get(0).result().controlFault() + "\",\n"
                + "  \"runs\": " + runs.size() + ",\n"
                + "  \"successful\": " + successful + "\n"
                + "}\n", StandardCharsets.UTF_8);
    }

    private static List<Long> values(List<Run> runs, Value value) {
        return runs.stream().filter(Run::successful).map(run -> switch (value) {
            case LAST_DEAD -> run.result().lastDeadNanos();
            case CONVERGENCE -> run.result().convergenceNanos();
            case HIDDEN_OBSERVED -> run.result().firstObserved().get(3);
        }).filter(java.util.Objects::nonNull).sorted().toList();
    }

    private static String percentiles(List<Long> sorted) {
        if (sorted.isEmpty()) {
            return "—";
        }
        return duration(percentile(sorted, 0.50)) + " / "
                + duration(percentile(sorted, 0.95)) + " / "
                + duration(percentile(sorted, 0.99));
    }

    private static long percentile(List<Long> sorted, double quantile) {
        int index = Math.max(0, (int) Math.ceil(quantile * sorted.size()) - 1);
        return sorted.get(index);
    }

    private static String duration(long nanos) {
        return String.format(Locale.ROOT, "%.3f ms", nanos / 1_000_000.0);
    }

    private static String nullable(Long value) {
        return value == null ? "" : value.toString();
    }

    private static String wilson(long successes, int total) {
        double z = 1.959963984540054;
        double zSquared = z * z;
        double proportion = successes / (double) total;
        double denominator = 1.0 + zSquared / total;
        double center = (proportion + zSquared / (2.0 * total)) / denominator;
        double half = z * Math.sqrt(proportion * (1.0 - proportion) / total
                + zSquared / (4.0 * total * total)) / denominator;
        return String.format(Locale.ROOT, "%.1f%%–%.1f%%",
                Math.max(0.0, center - half) * 100.0,
                Math.min(1.0, center + half) * 100.0);
    }

    private enum Value {
        LAST_DEAD,
        CONVERGENCE,
        HIDDEN_OBSERVED
    }

    private record Run(
            int repetition,
            UdpDistributedMembershipReportMain.ExperimentResult result
    ) {
        private boolean successful() {
            return result.converged()
                    && result.lastDeadNanos() != null
                    && result.convergenceNanos() != null
                    && result.firstObserved().keySet().containsAll(result.crashed());
        }

        private long preFaultDeadEvents() {
            return result.nodes().stream()
                    .mapToLong(UdpDistributedMembershipReportMain.NodeResult::preFaultDeadEvents)
                    .sum();
        }
    }
}
