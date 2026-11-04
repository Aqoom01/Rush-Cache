package com.openkache.failuredetector.cli;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Aggregates real-UDP matrix result.json files without external JSON dependencies. */
public final class UdpComparisonReportMain {
    private static final Pattern JSON_FIELD = Pattern.compile(
            "\\\"([^\\\"]+)\\\":(?:\\\"([^\\\"]*)\\\"|(null|-?\\d+))");

    private UdpComparisonReportMain() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: UdpComparisonReportMain MATRIX_DIR");
        }
        generate(Path.of(args[0]));
    }

    static void generate(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("matrix directory does not exist: " + root);
        }
        Path csv = root.resolve("runs.csv");
        Path jsonl = root.resolve("runs.jsonl");
        Path summary = root.resolve("summary.md");
        Path manifest = root.resolve("manifest.json");
        for (Path output : List.of(csv, jsonl, summary, manifest)) {
            if (Files.exists(output)) {
                throw new IllegalArgumentException(
                        "report output already exists; existing results are preserved: " + output);
            }
        }

        List<Run> runs = new ArrayList<>();
        try (var paths = Files.walk(root)) {
            for (Path result : paths.filter(path -> path.getFileName().toString()
                    .equals("result.json")).toList()) {
                runs.add(readRun(root, result));
            }
        }
        runs.sort(Comparator.comparing(Run::method)
                .thenComparing(Run::fault)
                .thenComparingInt(Run::seed));
        if (runs.isEmpty()) {
            throw new IllegalArgumentException("matrix has no result.json files: " + root);
        }

        writeCsv(csv, runs);
        writeJsonl(jsonl, runs);
        writeSummary(summary, runs);
        writeManifest(manifest, runs);
        System.out.println("UDP comparison runs=" + runs.size() + " output=" + root);
    }

    private static Run readRun(Path root, Path result) throws IOException {
        Path relative = root.relativize(result);
        if (relative.getNameCount() != 4) {
            throw new IllegalArgumentException("unexpected matrix result path: " + result);
        }
        String method = relative.getName(0).toString();
        String fault = relative.getName(1).toString();
        String seedName = relative.getName(2).toString();
        if (!seedName.startsWith("seed-")) {
            throw new IllegalArgumentException("missing seed directory: " + result);
        }
        int seed = Integer.parseInt(seedName.substring("seed-".length()));
        Map<String, String> fields = jsonFields(Files.readString(result));
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(
                result.getParent().resolve("run.properties"), StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return new Run(
                method,
                fault,
                seed,
                Integer.parseInt(properties.getProperty("nodes")),
                required(fields, "profile"),
                number(fields, "verificationHelperCount"),
                number(fields, "verificationFailureQuorum"),
                number(fields, "failedVerificationRoundsToDead"),
                number(fields, "verificationRoundSuccesses"),
                number(fields, "verificationRoundFailures"),
                number(fields, "verificationProbeSuccesses"),
                number(fields, "verificationProbeFailures"),
                optionalNumber(fields, "suspicionGossipFrames"),
                optionalNumber(fields, "aliveGossipFrames"),
                optionalNumber(fields, "independentConfirmations"),
                optionalNumber(fields, "confirmationProbeSuccesses"),
                optionalNumber(fields, "confirmationProbeFailures"),
                optionalNumber(fields, "protocolWarmupMillis"),
                optionalNumber(fields, "suspicionMaxConfirmations"),
                number(fields, "preFaultDeadEvents"),
                nullableNumber(fields, "suspectLatencyNanos"),
                nullableNumber(fields, "deadLatencyNanos"),
                number(fields, "sentFrames"),
                number(fields, "sentBytes"),
                number(fields, "receivedFrames"),
                number(fields, "receivedBytes"),
                required(fields, "timeSource"),
                required(fields, "wireSource")
        );
    }

    private static Map<String, String> jsonFields(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        Matcher matcher = JSON_FIELD.matcher(json);
        while (matcher.find()) {
            result.put(matcher.group(1), matcher.group(2) != null
                    ? matcher.group(2) : matcher.group(3));
        }
        return result;
    }

    private static String required(Map<String, String> fields, String name) {
        String value = fields.get(name);
        if (value == null || value.equals("null")) {
            throw new IllegalArgumentException("result is missing field " + name);
        }
        return value;
    }

    private static long number(Map<String, String> fields, String name) {
        return Long.parseLong(required(fields, name));
    }

    private static long optionalNumber(Map<String, String> fields, String name) {
        String value = fields.get(name);
        return value == null || value.equals("null") ? 0 : Long.parseLong(value);
    }

    private static Long nullableNumber(Map<String, String> fields, String name) {
        String value = fields.get(name);
        return value == null || value.equals("null") ? null : Long.parseLong(value);
    }

    private static void writeCsv(Path path, List<Run> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("method,fault,seed,node_count,profile,verification_helpers,");
            writer.write("verification_failure_quorum,failed_rounds_to_dead,");
            writer.write("verification_round_successes,verification_round_failures,");
            writer.write("verification_probe_successes,verification_probe_failures,");
            writer.write("suspicion_gossip_frames,alive_gossip_frames,");
            writer.write("independent_confirmations,confirmation_probe_successes,");
            writer.write("confirmation_probe_failures,protocol_warmup_millis,");
            writer.write("suspicion_max_confirmations,");
            writer.write("pre_fault_dead_events,suspect_latency_nanos,dead_latency_nanos,");
            writer.write("sent_frames,sent_bytes,received_frames,received_bytes,");
            writer.write("time_source,wire_source,successful\n");
            for (Run run : runs) {
                writer.write(String.join(",",
                        csv(run.method()), csv(run.fault()), Integer.toString(run.seed()),
                        Integer.toString(run.nodeCount()), csv(run.profile()),
                        Long.toString(run.verificationHelpers()),
                        Long.toString(run.verificationFailureQuorum()),
                        Long.toString(run.failedRoundsToDead()),
                        Long.toString(run.verificationRoundSuccesses()),
                        Long.toString(run.verificationRoundFailures()),
                        Long.toString(run.verificationProbeSuccesses()),
                        Long.toString(run.verificationProbeFailures()),
                        Long.toString(run.suspicionGossipFrames()),
                        Long.toString(run.aliveGossipFrames()),
                        Long.toString(run.independentConfirmations()),
                        Long.toString(run.confirmationProbeSuccesses()),
                        Long.toString(run.confirmationProbeFailures()),
                        Long.toString(run.protocolWarmupMillis()),
                        Long.toString(run.suspicionMaxConfirmations()),
                        Long.toString(run.preFaultDeadEvents()), nullable(run.suspectLatencyNanos()),
                        nullable(run.deadLatencyNanos()), Long.toString(run.sentFrames()),
                        Long.toString(run.sentBytes()), Long.toString(run.receivedFrames()),
                        Long.toString(run.receivedBytes()), csv(run.timeSource()),
                        csv(run.wireSource()), Boolean.toString(run.successful())));
                writer.newLine();
            }
        }
    }

    private static void writeJsonl(Path path, List<Run> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (Run run : runs) {
                writer.write("{"
                        + "\"method\":" + json(run.method()) + ","
                        + "\"fault\":" + json(run.fault()) + ","
                        + "\"seed\":" + run.seed() + ","
                        + "\"nodeCount\":" + run.nodeCount() + ","
                        + "\"profile\":" + json(run.profile()) + ","
                        + "\"verificationHelpers\":" + run.verificationHelpers() + ","
                        + "\"verificationFailureQuorum\":"
                        + run.verificationFailureQuorum() + ","
                        + "\"failedRoundsToDead\":" + run.failedRoundsToDead() + ","
                        + "\"verificationRoundSuccesses\":"
                        + run.verificationRoundSuccesses() + ","
                        + "\"verificationRoundFailures\":"
                        + run.verificationRoundFailures() + ","
                        + "\"verificationProbeSuccesses\":"
                        + run.verificationProbeSuccesses() + ","
                        + "\"verificationProbeFailures\":"
                        + run.verificationProbeFailures() + ","
                        + "\"suspicionGossipFrames\":"
                        + run.suspicionGossipFrames() + ","
                        + "\"aliveGossipFrames\":" + run.aliveGossipFrames() + ","
                        + "\"independentConfirmations\":"
                        + run.independentConfirmations() + ","
                        + "\"confirmationProbeSuccesses\":"
                        + run.confirmationProbeSuccesses() + ","
                        + "\"confirmationProbeFailures\":"
                        + run.confirmationProbeFailures() + ","
                        + "\"protocolWarmupMillis\":"
                        + run.protocolWarmupMillis() + ","
                        + "\"suspicionMaxConfirmations\":"
                        + run.suspicionMaxConfirmations() + ","
                        + "\"preFaultDeadEvents\":" + run.preFaultDeadEvents() + ","
                        + "\"suspectLatencyNanos\":" + nullable(run.suspectLatencyNanos()) + ","
                        + "\"deadLatencyNanos\":" + nullable(run.deadLatencyNanos()) + ","
                        + "\"sentFrames\":" + run.sentFrames() + ","
                        + "\"sentBytes\":" + run.sentBytes() + ","
                        + "\"receivedFrames\":" + run.receivedFrames() + ","
                        + "\"receivedBytes\":" + run.receivedBytes() + ","
                        + "\"timeSource\":" + json(run.timeSource()) + ","
                        + "\"wireSource\":" + json(run.wireSource()) + ","
                        + "\"successful\":" + run.successful()
                        + "}");
                writer.newLine();
            }
        }
    }

    private static void writeSummary(Path path, List<Run> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("# Ring-2 H3/Q3/R3 vs SWIM + Lifeguard — 실제 UDP 반복\n\n");
            writer.write("모든 수치는 `real-udp-observed`이며 동일 노드 수·실행 시간·");
            writer.write("fault 주입 시각과 loss별 netem seed를 사용했다.\n\n");
            writer.write("| 방법 | Fault | 성공 실행 | 성공률 95% Wilson CI | 사전 false DEAD | DEAD p50 | DEAD p95 | DEAD p99 | 평균 송신 frame | 평균 송신 byte |\n");
            writer.write("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            for (String method : runs.stream().map(Run::method).distinct().toList()) {
                for (String fault : List.of("crash", "loss15", "loss45")) {
                    List<Run> selected = runs.stream()
                            .filter(run -> run.method().equals(method)
                                    && run.fault().equals(fault)).toList();
                    if (selected.isEmpty()) {
                        continue;
                    }
                    long successful = selected.stream().filter(Run::successful).count();
                    long preFaultDead = selected.stream()
                            .mapToLong(Run::preFaultDeadEvents).sum();
                    List<Long> deadLatencies = selected.stream()
                            .map(Run::deadLatencyNanos).filter(java.util.Objects::nonNull)
                            .sorted().toList();
                    writer.write(String.format(Locale.ROOT,
                            "| %s | %s | %d/%d | %s | %d | %s | %s | %s | %.1f | %.1f |%n",
                            method, fault, successful, selected.size(),
                            wilson(successful, selected.size()), preFaultDead,
                            duration(percentile(deadLatencies, 0.50)),
                            duration(percentile(deadLatencies, 0.95)),
                            duration(percentile(deadLatencies, 0.99)),
                            selected.stream().mapToLong(Run::sentFrames).average().orElse(0),
                            selected.stream().mapToLong(Run::sentBytes).average().orElse(0)));
                }
            }
            writer.write("\nCrash 성공은 fault 이후 DEAD가 있고 사전 DEAD가 없는 실행이다. ");
            writer.write("Loss 성공은 사전·사후 DEAD가 모두 없는 실행이다.\n");
            writer.write("SWIM confirmation Gossip frame과 독립 confirmation은 runs.csv와 ");
            writer.write("runs.jsonl에 별도 실측 필드로 기록한다.\n");
            writePairedSummary(writer, runs);
        }
    }

    private static void writePairedSummary(BufferedWriter writer, List<Run> runs)
            throws IOException {
        writer.write("\n## 동일 seed paired 차이\n\n");
        writer.write("차이는 Ring - SWIM이다. 음수는 Ring의 비용 또는 지연이 더 작다는 뜻이다.\n\n");
        writer.write("| Fault | paired seeds | DEAD 평균 차이 | 송신 frame 평균 차이 | 송신 byte 평균 차이 |\n");
        writer.write("|---|---:|---:|---:|---:|\n");
        for (String fault : List.of("crash", "loss15", "loss45")) {
            Map<Integer, Run> ring = bySeed(runs, "ring-verify-h3-q3-r3", fault);
            Map<Integer, Run> swim = bySeed(runs, "swim-lifeguard", fault);
            List<Integer> seeds = ring.keySet().stream().filter(swim::containsKey).sorted().toList();
            double frameDifference = seeds.stream().mapToLong(seed ->
                    ring.get(seed).sentFrames() - swim.get(seed).sentFrames())
                    .average().orElse(0.0);
            double byteDifference = seeds.stream().mapToLong(seed ->
                    ring.get(seed).sentBytes() - swim.get(seed).sentBytes())
                    .average().orElse(0.0);
            List<Long> latencyDifferences = seeds.stream()
                    .filter(seed -> ring.get(seed).deadLatencyNanos() != null
                            && swim.get(seed).deadLatencyNanos() != null)
                    .map(seed -> ring.get(seed).deadLatencyNanos()
                            - swim.get(seed).deadLatencyNanos())
                    .toList();
            String latency = latencyDifferences.isEmpty() ? "—" : String.format(
                    Locale.ROOT, "%.3f ms", latencyDifferences.stream()
                            .mapToLong(Long::longValue).average().orElse(0.0) / 1_000_000.0);
            writer.write(String.format(Locale.ROOT,
                    "| %s | %d | %s | %.1f | %.1f |%n",
                    fault, seeds.size(), latency, frameDifference, byteDifference));
        }
    }

    private static Map<Integer, Run> bySeed(
            List<Run> runs,
            String method,
            String fault
    ) {
        Map<Integer, Run> selected = new LinkedHashMap<>();
        runs.stream().filter(run -> run.method().equals(method) && run.fault().equals(fault))
                .forEach(run -> selected.put(run.seed(), run));
        return selected;
    }

    private static String wilson(long successes, int total) {
        if (total == 0) {
            return "—";
        }
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

    private static void writeManifest(Path path, List<Run> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("{\n");
            writer.write("  \"experiment\": \"udp-ring-h3-vs-swim-lifeguard\",\n");
            writer.write("  \"generatedAt\": " + json(Instant.now().toString()) + ",\n");
            writer.write("  \"source\": \"real-udp-observed\",\n");
            writer.write("  \"runs\": " + runs.size() + "\n");
            writer.write("}\n");
        }
    }

    private static Long percentile(List<Long> sorted, double quantile) {
        if (sorted.isEmpty()) {
            return null;
        }
        int index = Math.max(0, (int) Math.ceil(quantile * sorted.size()) - 1);
        return sorted.get(index);
    }

    private static String duration(Long nanos) {
        return nanos == null ? "—" : String.format(Locale.ROOT, "%.3f ms", nanos / 1_000_000.0);
    }

    private static String nullable(Long value) {
        return value == null ? "null" : value.toString();
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String json(String value) {
        return csv(value.replace("\\", "\\\\")
                .replace("\n", "\\n").replace("\r", "\\r"));
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    }

    private record Run(
            String method,
            String fault,
            int seed,
            int nodeCount,
            String profile,
            long verificationHelpers,
            long verificationFailureQuorum,
            long failedRoundsToDead,
            long verificationRoundSuccesses,
            long verificationRoundFailures,
            long verificationProbeSuccesses,
            long verificationProbeFailures,
            long suspicionGossipFrames,
            long aliveGossipFrames,
            long independentConfirmations,
            long confirmationProbeSuccesses,
            long confirmationProbeFailures,
            long protocolWarmupMillis,
            long suspicionMaxConfirmations,
            long preFaultDeadEvents,
            Long suspectLatencyNanos,
            Long deadLatencyNanos,
            long sentFrames,
            long sentBytes,
            long receivedFrames,
            long receivedBytes,
            String timeSource,
            String wireSource
    ) {
        private boolean successful() {
            if (preFaultDeadEvents != 0) {
                return false;
            }
            return fault.equals("crash") ? deadLatencyNanos != null : deadLatencyNanos == null;
        }
    }
}
