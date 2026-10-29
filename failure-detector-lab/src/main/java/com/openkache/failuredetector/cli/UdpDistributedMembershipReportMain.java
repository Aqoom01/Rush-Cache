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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Aggregates survivor results from the real-UDP distributed Ring experiment. */
public final class UdpDistributedMembershipReportMain {
    private static final Pattern JSON_FIELD = Pattern.compile(
            "\\\"([^\\\"]+)\\\":(?:\\\"([^\\\"]*)\\\"|(null|-?\\d+))");

    private UdpDistributedMembershipReportMain() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "usage: UdpDistributedMembershipReportMain EXPERIMENT_DIR");
        }
        generate(Path.of(args[0]));
    }

    static void generate(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("experiment directory does not exist: " + root);
        }
        Path csv = root.resolve("nodes.csv");
        Path summary = root.resolve("summary.md");
        Path manifest = root.resolve("manifest.json");
        for (Path output : List.of(csv, summary, manifest)) {
            if (Files.exists(output)) {
                throw new IllegalArgumentException(
                        "report output already exists; existing results are preserved: " + output);
            }
        }

        ExperimentResult result = readExperiment(root);
        writeCsv(csv, result);
        writeSummary(summary, result);
        writeManifest(manifest, result);
        System.out.println("UDP distributed membership survivors=" + result.nodes().size()
                + " converged=" + result.converged() + " output=" + root);
    }

    static ExperimentResult readExperiment(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("experiment directory does not exist: " + root);
        }
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(
                root.resolve("run.properties"), StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        int nodeCount = Integer.parseInt(required(properties, "nodes"));
        String controlFault = properties.getProperty("control_fault", "none");
        long controlFaultSeed = Long.parseLong(
                properties.getProperty("control_fault_seed", "0"));
        Set<Integer> crashed = new LinkedHashSet<>(parseCsvIds(
                required(properties, "crash_targets")));
        Set<Integer> expectedSurvivors = new LinkedHashSet<>();
        for (int node = 0; node < nodeCount; node++) {
            if (!crashed.contains(node)) {
                expectedSurvivors.add(node);
            }
        }

        List<NodeResult> nodes = new ArrayList<>();
        try (var paths = Files.list(root)) {
            for (Path result : paths.filter(path -> path.getFileName().toString()
                    .matches("node-[0-9]+\\.result\\.json")).toList()) {
                nodes.add(readNode(result));
            }
        }
        nodes.sort(Comparator.comparingInt(NodeResult::nodeId));
        if (!nodes.stream().map(NodeResult::nodeId).collect(
                java.util.stream.Collectors.toSet()).equals(expectedSurvivors)) {
            throw new IllegalArgumentException(
                    "survivor result set does not match run.properties: " + root);
        }
        if (nodes.stream().anyMatch(node -> !node.wireSource().equals("real-udp-observed"))) {
            throw new IllegalArgumentException("all node results must be real-udp-observed");
        }

        return summarize(
                nodeCount, crashed, expectedSurvivors, nodes,
                controlFault, controlFaultSeed);
    }

    private static ExperimentResult summarize(
            int nodeCount,
            Set<Integer> crashed,
            Set<Integer> expectedSurvivors,
            List<NodeResult> nodes,
            String controlFault,
            long controlFaultSeed
    ) {
        boolean converged = nodes.stream().allMatch(node ->
                node.preFaultDeadEvents() == 0
                        && node.finalMembers().equals(expectedSurvivors)
                        && node.removalLatencies().keySet().containsAll(crashed));

        Map<Integer, Long> firstDead = new LinkedHashMap<>();
        Map<Integer, Long> firstObserved = new LinkedHashMap<>();
        for (int target : crashed) {
            nodes.stream().map(node -> node.localDeadLatencies().get(target))
                    .filter(java.util.Objects::nonNull)
                    .min(Long::compareTo)
                    .ifPresent(value -> firstDead.put(target, value));
            boolean monitoredAtFault = nodes.stream()
                    .anyMatch(node -> node.monitoredAtFault().contains(target));
            if (monitoredAtFault) {
                firstObserved.put(target, 0L);
            } else {
                nodes.stream().map(node -> node.firstAssignedAfterFault().get(target))
                        .filter(java.util.Objects::nonNull)
                        .min(Long::compareTo)
                        .ifPresent(value -> firstObserved.put(target, value));
            }
        }
        Long lastDead = firstDead.keySet().containsAll(crashed)
                ? firstDead.values().stream().max(Long::compareTo).orElse(null) : null;
        Long convergence = converged
                ? nodes.stream().flatMap(node -> crashed.stream()
                                .map(node.removalLatencies()::get))
                        .max(Long::compareTo).orElse(null)
                : null;
        return new ExperimentResult(
                nodeCount,
                controlFault,
                controlFaultSeed,
                Set.copyOf(crashed),
                Set.copyOf(expectedSurvivors),
                List.copyOf(nodes),
                converged,
                Map.copyOf(firstDead),
                Map.copyOf(firstObserved),
                lastDead,
                convergence,
                nodes.stream().mapToLong(NodeResult::membershipAttemptedFrames).sum(),
                nodes.stream().mapToLong(NodeResult::membershipDroppedFrames).sum(),
                nodes.stream().mapToLong(NodeResult::membershipControlFrames).sum(),
                nodes.stream().mapToLong(NodeResult::sentFrames).sum(),
                nodes.stream().mapToLong(NodeResult::sentBytes).sum()
        );
    }

    private static NodeResult readNode(Path result) throws IOException {
        Map<String, String> fields = jsonFields(Files.readString(result));
        return new NodeResult(
                (int) number(fields, "nodeId"),
                number(fields, "preFaultDeadEvents"),
                (int) number(fields, "viewVersion"),
                parseIds(required(fields, "finalMembers")),
                parseIds(required(fields, "monitoredAtFault")),
                parseLatencyMap(required(fields, "firstAssignedAfterFault")),
                parseLatencyMap(required(fields, "removalLatenciesNanos")),
                parseLatencyMap(required(fields, "localDeadLatenciesNanos")),
                number(fields, "localRemovals"),
                number(fields, "remoteRemovals"),
                optionalNumber(fields, "membershipAttemptedFrames"),
                optionalNumber(fields, "membershipDroppedFrames"),
                number(fields, "membershipControlFrames"),
                number(fields, "sentFrames"),
                number(fields, "sentBytes"),
                required(fields, "wireSource")
        );
    }

    private static void writeCsv(Path path, ExperimentResult result) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("node_id,pre_fault_dead,view_version,final_members,removed_targets,");
            writer.write("local_removals,remote_removals,membership_attempted_frames,");
            writer.write("membership_dropped_frames,membership_control_frames,");
            writer.write("sent_frames,sent_bytes,converged,wire_source\n");
            for (NodeResult node : result.nodes()) {
                writer.write(String.format(Locale.ROOT,
                        "%d,%d,%d,\"%s\",\"%s\",%d,%d,%d,%d,%d,%d,%d,%s,\"%s\"%n",
                        node.nodeId(), node.preFaultDeadEvents(), node.viewVersion(),
                        joinIds(node.finalMembers()), joinIds(node.removalLatencies().keySet()),
                        node.localRemovals(), node.remoteRemovals(),
                        node.membershipAttemptedFrames(), node.membershipDroppedFrames(),
                        node.membershipControlFrames(), node.sentFrames(), node.sentBytes(),
                        nodeConverged(node, result), node.wireSource()));
            }
        }
    }

    private static void writeSummary(Path path, ExperimentResult result) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("# Ring-2 H3/Q3/R3 실제 UDP 분산 Membership 재구성\n\n");
            writer.write("모든 latency·frame·byte는 `real-udp-observed`다. Crash 대상은 ");
            writer.write(joinNodeNames(result.crashed()));
            writer.write("이며, 이 목록은 runner의 fault 주입과 사후 평가에만 사용했다. ");
            writer.write("Membership은 detector가 실제로 발생시킨 DEAD 이벤트만으로 갱신했다.\n\n");
            writer.write("| 결과 | 값 |\n|---|---:|\n");
            writer.write("| Membership control fault / seed | "
                    + result.controlFault() + " / " + result.controlFaultSeed() + " |\n");
            writer.write("| 생존 노드 View 수렴 | "
                    + (result.converged() ? "성공" : "실패") + " |\n");
            writer.write("| 마지막 장애 최초 DEAD | " + duration(result.lastDeadNanos()) + " |\n");
            writer.write("| 모든 생존 View 수렴 | "
                    + duration(result.convergenceNanos()) + " |\n");
            writer.write("| Membership control frame | "
                    + result.membershipControlFrames() + " |\n");
            writer.write("| Membership attempted / injected drop | "
                    + result.membershipAttemptedFrames() + " / "
                    + result.membershipDroppedFrames() + " |\n");
            writer.write("| 생존 노드 송신 frame / byte | "
                    + result.survivorSentFrames() + " / "
                    + result.survivorSentBytes() + " |\n\n");

            writer.write("## 장애별 관측과 탐지\n\n");
            writer.write("| 장애 노드 | 최초 생존 관측자 배정 | 최초 DEAD |\n|---|---:|---:|\n");
            for (int target : result.crashed()) {
                writer.write("| node-" + target + " | "
                        + duration(result.firstObserved().get(target)) + " | "
                        + duration(result.firstDead().get(target)) + " |\n");
            }

            writer.write("\n## 생존 노드별 최종 View\n\n");
            writer.write("| 노드 | 최종 View | 로컬/원격 제거 | 사전 false DEAD | 수렴 |\n");
            writer.write("|---|---|---:|---:|---:|\n");
            for (NodeResult node : result.nodes()) {
                writer.write("| node-" + node.nodeId() + " | "
                        + joinNodeNames(node.finalMembers()) + " | "
                        + node.localRemovals() + "/" + node.remoteRemovals() + " | "
                        + node.preFaultDeadEvents() + " | "
                        + (nodeConverged(node, result) ? "예" : "아니오") + " |\n");
            }

            writer.write("\n생존 노드 송신량에는 실험 중 강제 종료된 세 노드의 fault 이전 ");
            writer.write("송신량이 포함되지 않으므로 전체 클러스터 비용으로 해석하지 않는다. ");
            writer.write("control frame은 별도 Java control process에서 전송되어 노드 송신량에 ");
            writer.write("포함되지 않는다.\n");
        }
    }

    private static void writeManifest(Path path, ExperimentResult result) throws IOException {
        String json = "{\n"
                + "  \"experiment\": \"udp-ring2-distributed-membership\",\n"
                + "  \"generatedAt\": \"" + Instant.now() + "\",\n"
                + "  \"source\": \"real-udp-observed\",\n"
                + "  \"nodeCount\": " + result.nodeCount() + ",\n"
                + "  \"controlFault\": \"" + result.controlFault() + "\",\n"
                + "  \"controlFaultSeed\": " + result.controlFaultSeed() + ",\n"
                + "  \"crashTargets\": \"" + joinIds(result.crashed()) + "\",\n"
                + "  \"survivorResults\": " + result.nodes().size() + ",\n"
                + "  \"converged\": " + result.converged() + "\n"
                + "}\n";
        Files.writeString(path, json, StandardCharsets.UTF_8);
    }

    private static boolean nodeConverged(NodeResult node, ExperimentResult result) {
        return node.preFaultDeadEvents() == 0
                && node.finalMembers().equals(result.expectedSurvivors())
                && node.removalLatencies().keySet().containsAll(result.crashed());
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

    private static Set<Integer> parseCsvIds(String value) {
        if (value.isBlank()) {
            return Set.of();
        }
        Set<Integer> result = new LinkedHashSet<>();
        for (String item : value.split(",")) {
            result.add(Integer.parseInt(item));
        }
        return Set.copyOf(result);
    }

    private static Set<Integer> parseIds(String value) {
        if (value.isBlank()) {
            return Set.of();
        }
        Set<Integer> result = new LinkedHashSet<>();
        for (String item : value.split(";")) {
            result.add(Integer.parseInt(item));
        }
        return Set.copyOf(result);
    }

    private static Map<Integer, Long> parseLatencyMap(String value) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        if (value.isBlank()) {
            return Map.of();
        }
        for (String item : value.split(";")) {
            String[] parts = item.split(":", 2);
            result.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
        }
        return Map.copyOf(result);
    }

    private static String required(Map<String, String> fields, String name) {
        String value = fields.get(name);
        if (value == null || value.equals("null")) {
            throw new IllegalArgumentException("result is missing field " + name);
        }
        return value;
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("run.properties is missing " + name);
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

    private static String duration(Long nanos) {
        return nanos == null ? "—" : String.format(
                Locale.ROOT, "%.3f ms", nanos / 1_000_000.0);
    }

    private static String joinIds(Iterable<Integer> ids) {
        List<Integer> sorted = new ArrayList<>();
        ids.forEach(sorted::add);
        sorted.sort(Comparator.naturalOrder());
        return sorted.stream().map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(";"));
    }

    private static String joinNodeNames(Iterable<Integer> ids) {
        List<Integer> sorted = new ArrayList<>();
        ids.forEach(sorted::add);
        sorted.sort(Comparator.naturalOrder());
        return sorted.stream().map(id -> "node-" + id)
                .collect(java.util.stream.Collectors.joining(", "));
    }

    record NodeResult(
            int nodeId,
            long preFaultDeadEvents,
            int viewVersion,
            Set<Integer> finalMembers,
            Set<Integer> monitoredAtFault,
            Map<Integer, Long> firstAssignedAfterFault,
            Map<Integer, Long> removalLatencies,
            Map<Integer, Long> localDeadLatencies,
            long localRemovals,
            long remoteRemovals,
            long membershipAttemptedFrames,
            long membershipDroppedFrames,
            long membershipControlFrames,
            long sentFrames,
            long sentBytes,
            String wireSource
    ) {
    }

    record ExperimentResult(
            int nodeCount,
            String controlFault,
            long controlFaultSeed,
            Set<Integer> crashed,
            Set<Integer> expectedSurvivors,
            List<NodeResult> nodes,
            boolean converged,
            Map<Integer, Long> firstDead,
            Map<Integer, Long> firstObserved,
            Long lastDeadNanos,
            Long convergenceNanos,
            long membershipAttemptedFrames,
            long membershipDroppedFrames,
            long membershipControlFrames,
            long survivorSentFrames,
            long survivorSentBytes
    ) {
    }
}
