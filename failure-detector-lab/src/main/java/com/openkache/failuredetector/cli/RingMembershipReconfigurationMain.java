package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.core.DetectorEvent;
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

/** Compares static Ring-2, ideal reconfigured Ring-2 and static Ring-3. */
public final class RingMembershipReconfigurationMain {
    private static final int CRASH_COUNT = 3;
    private static final int PAYLOAD_BYTES = 64;
    private static final long NORMAL_TICKS = 41;

    private RingMembershipReconfigurationMain() {
    }

    public static void main(String[] args) throws IOException {
        Options options = Options.parse(args);
        if (Files.exists(options.outputDirectory())) {
            throw new IllegalArgumentException(
                    "Output directory already exists; existing results are preserved: "
                            + options.outputDirectory());
        }
        Files.createDirectories(options.outputDirectory());

        RingMultiCrashExperiment staticExperiment = new RingMultiCrashExperiment();
        RingMembershipReconfigurationExperiment reconfigurationExperiment =
                new RingMembershipReconfigurationExperiment();
        List<RunRow> runs = new ArrayList<>();
        for (int nodeCount : options.nodeCounts()) {
            for (int firstCrash = 0; firstCrash < nodeCount; firstCrash++) {
                runs.add(staticRow(
                        "ring-2-static", "static", nodeCount, 2, firstCrash,
                        staticExperiment.runConsecutiveCrash(
                                nodeCount, 2, firstCrash, CRASH_COUNT)));
                runs.add(reconfiguredRow(
                        nodeCount, firstCrash,
                        reconfigurationExperiment.runConsecutiveCrash(
                                nodeCount, 2, firstCrash, CRASH_COUNT)));
                runs.add(staticRow(
                        "ring-3-static", "static", nodeCount, 3, firstCrash,
                        staticExperiment.runConsecutiveCrash(
                                nodeCount, 3, firstCrash, CRASH_COUNT)));
            }
        }

        writeManifest(options, runs);
        writeRuns(options.outputDirectory().resolve("runs.csv"), runs);
        writeTargets(options.outputDirectory().resolve("targets.csv"), runs);
        writeDetectorEvents(
                options.outputDirectory().resolve("detector-events.jsonl"), runs);
        writeMembershipEvents(
                options.outputDirectory().resolve("membership-events.jsonl"), runs);
        writeSummary(options.outputDirectory().resolve("summary.md"), runs);

        for (String profile : profiles()) {
            List<RunRow> selected = byProfile(runs, profile);
            long complete = selected.stream().filter(RunRow::allDetected).count();
            long detected = selected.stream().mapToLong(RunRow::detectedDead).sum();
            long targets = selected.stream().mapToLong(RunRow::targetCount).sum();
            System.out.printf(Locale.ROOT,
                    "%s: completeRuns=%d/%d, detectedTargets=%d/%d%n",
                    profile, complete, selected.size(), detected, targets);
        }
        System.out.println("output=" + options.outputDirectory());
    }

    private static RunRow staticRow(
            String profile,
            String mode,
            int nodeCount,
            int neighborCount,
            int firstCrash,
            RingMultiCrashExperiment.Result result
    ) {
        List<TargetRow> targets = result.outcomes().stream()
                .map(outcome -> new TargetRow(
                        outcome.target(),
                        outcome.suspectLatencyNanos(),
                        outcome.deadLatencyNanos(),
                        outcome.liveObservers().isEmpty() ? null : 0L,
                        outcome.liveObservers().isEmpty()
                ))
                .toList();
        Long lastDead = result.allDetectedDead()
                ? targets.stream().map(TargetRow::deadLatencyNanos)
                        .max(Long::compareTo).orElse(null)
                : null;
        return new RunRow(
                profile + "-n" + nodeCount + "-p" + firstCrash,
                profile,
                mode,
                nodeCount,
                neighborCount,
                result.crashedTargets(),
                result.detectedDeadCount(),
                result.outcomes().size(),
                result.allDetectedDead(),
                lastDead,
                null,
                normalMessages(nodeCount, neighborCount),
                result.protocolMessages(),
                result.protocolBytes(),
                0,
                0,
                targets,
                result.events(),
                List.of()
        );
    }

    private static RunRow reconfiguredRow(
            int nodeCount,
            int firstCrash,
            RingMembershipReconfigurationExperiment.Result result
    ) {
        List<TargetRow> targets = result.outcomes().stream()
                .map(outcome -> new TargetRow(
                        outcome.target(),
                        outcome.suspectLatencyNanos(),
                        outcome.deadLatencyNanos(),
                        outcome.firstLiveObserverLatencyNanos(),
                        outcome.firstLiveObserverLatencyNanos() == null
                ))
                .toList();
        return new RunRow(
                "ring-2-immediate-reconfiguration-n" + nodeCount + "-p" + firstCrash,
                "ring-2-immediate-reconfiguration",
                "immediate-shared-view",
                nodeCount,
                result.neighborCount(),
                result.crashedTargets(),
                result.detectedDeadCount(),
                result.outcomes().size(),
                result.allDetectedDead(),
                result.lastDeadLatencyNanos(),
                result.membershipCompleteLatencyNanos(),
                normalMessages(nodeCount, result.neighborCount()),
                result.heartbeatMessages(),
                result.heartbeatBytes(),
                result.membershipControlMessages(),
                result.membershipControlBytes(),
                targets,
                result.detectorEvents(),
                result.membershipEvents()
        );
    }

    private static long normalMessages(int nodeCount, int neighborCount) {
        return NORMAL_TICKS * nodeCount * Math.min(neighborCount, nodeCount - 1L);
    }

    private static void writeManifest(Options options, List<RunRow> runs) throws IOException {
        try (BufferedWriter writer = writer(options.outputDirectory().resolve("manifest.json"))) {
            writer.write("{\n");
            writer.write("  \"experiment\": \"ring-2-membership-reconfiguration\",\n");
            writer.write("  \"generatedAt\": \"" + Instant.now() + "\",\n");
            writer.write("  \"nodeCounts\": \"" + join(options.nodeCounts()) + "\",\n");
            writer.write("  \"crashCount\": 3,\n");
            writer.write("  \"faultStartSeconds\": 20,\n");
            writer.write("  \"durationSeconds\": 40,\n");
            writer.write("  \"profiles\": \"" + String.join(",", profiles()) + "\",\n");
            writer.write("  \"reconfigurationDecisionSource\": \"detector-dead-events\",\n");
            writer.write("  \"injectedCrashSetUsedForDecision\": false,\n");
            writer.write("  \"sharedViewConsistencyDelayNanos\": 0,\n");
            writer.write("  \"membershipControlTrafficModeled\": false,\n");
            writer.write("  \"latencySource\": \"simulation-observed\",\n");
            writer.write("  \"trafficSource\": \"model-estimated\",\n");
            writer.write("  \"runs\": " + runs.size() + "\n");
            writer.write("}\n");
        }
    }

    private static void writeRuns(Path path, List<RunRow> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,profile,reconfiguration_mode,node_count,neighbor_count,");
            writer.write("crashed_targets,detected_dead,target_count,all_detected,");
            writer.write("last_dead_latency_ms,membership_complete_latency_ms,");
            writer.write("normal_heartbeat_messages_40s,fault_heartbeat_messages,");
            writer.write("fault_heartbeat_bytes,membership_control_messages,");
            writer.write("membership_control_bytes,total_fault_messages,total_fault_bytes,");
            writer.write("latency_source,traffic_source\n");
            for (RunRow run : runs) {
                writer.write(String.join(",",
                        csv(run.runId()),
                        csv(run.profile()),
                        csv(run.reconfigurationMode()),
                        Integer.toString(run.nodeCount()),
                        Integer.toString(run.neighborCount()),
                        csv(String.join("|", run.crashedTargets())),
                        Long.toString(run.detectedDead()),
                        Integer.toString(run.targetCount()),
                        Boolean.toString(run.allDetected()),
                        nullableMillis(run.lastDeadLatencyNanos()),
                        nullableMillis(run.membershipCompleteLatencyNanos()),
                        Long.toString(run.normalHeartbeatMessages()),
                        Long.toString(run.faultHeartbeatMessages()),
                        Long.toString(run.faultHeartbeatBytes()),
                        Long.toString(run.membershipControlMessages()),
                        Long.toString(run.membershipControlBytes()),
                        Long.toString(run.faultHeartbeatMessages()
                                + run.membershipControlMessages()),
                        Long.toString(run.faultHeartbeatBytes()
                                + run.membershipControlBytes()),
                        csv("simulation-observed"),
                        csv("model-estimated")
                ));
                writer.newLine();
            }
        }
    }

    private static void writeTargets(Path path, List<RunRow> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("run_id,profile,target,suspect_latency_ms,dead_latency_ms,");
            writer.write("first_live_observer_latency_ms,unobserved_censored,detected_dead\n");
            for (RunRow run : runs) {
                for (TargetRow target : run.targets()) {
                    writer.write(String.join(",",
                            csv(run.runId()),
                            csv(run.profile()),
                            csv(target.target()),
                            nullableMillis(target.suspectLatencyNanos()),
                            nullableMillis(target.deadLatencyNanos()),
                            nullableMillis(target.firstLiveObserverLatencyNanos()),
                            Boolean.toString(target.unobservedCensored()),
                            Boolean.toString(target.deadLatencyNanos() != null)
                    ));
                    writer.newLine();
                }
            }
        }
    }

    private static void writeDetectorEvents(Path path, List<RunRow> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (RunRow run : runs) {
                for (DetectorEvent event : run.detectorEvents()) {
                    writer.write("{\"runId\":\"" + json(run.runId()) + "\""
                            + ",\"profile\":\"" + json(run.profile()) + "\""
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

    private static void writeMembershipEvents(Path path, List<RunRow> runs)
            throws IOException {
        try (BufferedWriter writer = writer(path)) {
            for (RunRow run : runs) {
                for (RingMembershipReconfigurationExperiment.MembershipViewEvent event
                        : run.membershipEvents()) {
                    writer.write("{\"runId\":\"" + json(run.runId()) + "\""
                            + ",\"atNanos\":" + event.atNanos()
                            + ",\"viewVersion\":" + event.viewVersion()
                            + ",\"removedMembers\":" + jsonArray(event.removedMembers())
                            + ",\"membersBefore\":" + jsonArray(event.membersBefore())
                            + ",\"membersAfter\":" + jsonArray(event.membersAfter())
                            + ",\"detectorEvidence\":" + jsonArray(event.detectorEvidence())
                            + ",\"source\":\"" + json(event.source()) + "\"}");
                    writer.newLine();
                }
            }
        }
    }

    private static void writeSummary(Path path, List<RunRow> runs) throws IOException {
        try (BufferedWriter writer = writer(path)) {
            writer.write("# 실험 8 — Ring-2 Membership 재구성\n\n");
            writer.write("- 연속 3노드 동시 Crash, 5·8노드 모든 시작 위치 13개\n");
            writer.write("- 재구성 판단 입력: Detector가 발생시킨 DEAD 이벤트만 사용\n");
            writer.write("- 즉시 재구성은 지연 0의 공유 View이며 제어 메시지 비용은 미모델링\n\n");
            writer.write("| 프로필 | 전체 탐지 실행 | 탐지 노드 | 마지막 DEAD |\n");
            writer.write("|---|---:|---:|---:|\n");
            for (String profile : profiles()) {
                List<RunRow> selected = byProfile(runs, profile);
                long complete = selected.stream().filter(RunRow::allDetected).count();
                long detected = selected.stream().mapToLong(RunRow::detectedDead).sum();
                long targets = selected.stream().mapToLong(RunRow::targetCount).sum();
                Long lastDead = selected.stream().allMatch(RunRow::allDetected)
                        ? selected.stream().map(RunRow::lastDeadLatencyNanos)
                                .max(Long::compareTo).orElse(null)
                        : null;
                writer.write("| " + profile + " | " + complete + "/" + selected.size()
                        + " | " + detected + "/" + targets + " | "
                        + displayMillis(lastDead) + " |\n");
            }
            writer.write("\n| 프로필 | 5노드 fault messages/run | 8노드 fault messages/run |\n");
            writer.write("|---|---:|---:|\n");
            for (String profile : profiles()) {
                writer.write("| " + profile + " | " + messagesFor(runs, profile, 5)
                        + " | " + messagesFor(runs, profile, 8) + " |\n");
            }
            writer.write("\n탐지 시간은 `simulation-observed`, Heartbeat와 byte는 "
                    + "`model-estimated`다. 즉시 공유 View의 membership control 메시지는 "
                    + "0으로 기록했으므로 실제 분산 재구성 비용을 뜻하지 않는다.\n");
        }
    }

    private static long messagesFor(List<RunRow> runs, String profile, int nodeCount) {
        return runs.stream()
                .filter(run -> run.profile().equals(profile) && run.nodeCount() == nodeCount)
                .findFirst()
                .map(run -> run.faultHeartbeatMessages() + run.membershipControlMessages())
                .orElseThrow();
    }

    private static List<String> profiles() {
        return List.of(
                "ring-2-static",
                "ring-2-immediate-reconfiguration",
                "ring-3-static"
        );
    }

    private static List<RunRow> byProfile(List<RunRow> runs, String profile) {
        return runs.stream().filter(run -> run.profile().equals(profile)).toList();
    }

    private static BufferedWriter writer(Path path) throws IOException {
        return Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    }

    private static String join(List<Integer> values) {
        return values.stream().map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(","));
    }

    private static String nullableMillis(Long nanos) {
        return nanos == null ? "" : String.format(Locale.ROOT, "%.3f", nanos / 1_000_000.0);
    }

    private static String displayMillis(Long nanos) {
        return nanos == null ? "미완료" : nullableMillis(nanos) + "ms";
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String jsonArray(List<String> values) {
        return values.stream()
                .map(value -> "\"" + json(value) + "\"")
                .collect(java.util.stream.Collectors.joining(",", "[", "]"));
    }

    private record TargetRow(
            String target,
            Long suspectLatencyNanos,
            Long deadLatencyNanos,
            Long firstLiveObserverLatencyNanos,
            boolean unobservedCensored
    ) {
    }

    private record RunRow(
            String runId,
            String profile,
            String reconfigurationMode,
            int nodeCount,
            int neighborCount,
            List<String> crashedTargets,
            long detectedDead,
            int targetCount,
            boolean allDetected,
            Long lastDeadLatencyNanos,
            Long membershipCompleteLatencyNanos,
            long normalHeartbeatMessages,
            long faultHeartbeatMessages,
            long faultHeartbeatBytes,
            long membershipControlMessages,
            long membershipControlBytes,
            List<TargetRow> targets,
            List<DetectorEvent> detectorEvents,
            List<RingMembershipReconfigurationExperiment.MembershipViewEvent> membershipEvents
    ) {
        private RunRow {
            crashedTargets = List.copyOf(crashedTargets);
            targets = List.copyOf(targets);
            detectorEvents = List.copyOf(detectorEvents);
            membershipEvents = List.copyOf(membershipEvents);
        }
    }

    private record Options(List<Integer> nodeCounts, Path outputDirectory) {
        private static Options parse(String[] args) {
            List<Integer> nodeCounts = List.of(5, 8);
            Path output = Path.of("results-ring-membership-reconfiguration");
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--nodes" -> nodeCounts = Arrays.stream(args[++index].split(","))
                            .map(String::trim)
                            .map(Integer::parseInt)
                            .toList();
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
