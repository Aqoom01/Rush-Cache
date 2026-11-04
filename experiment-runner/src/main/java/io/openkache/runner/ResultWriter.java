package io.openkache.runner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link ExperimentResult}를 JSON(실행 설정·결과의 원본)과 CSV(비교용 요약, 1행 append)로 기록한다.
 *
 * <p>외부 JSON 라이브러리를 쓰지 않는다. 필드가 고정돼 있어 범용 직렬화가 필요하지 않다. {@code resultsDir/<study>/}
 * 아래에 {@code <executionId>.json}과 누적되는 {@code summary.csv}를 쓴다.
 */
final class ResultWriter {

    private ResultWriter() {}

    static Path writeJson(ExperimentResult result, Path resultsDir) {
        try {
            Path studyDir = resultsDir.resolve(sanitize(result.study()));
            Files.createDirectories(studyDir);
            Path file = studyDir.resolve(result.executionId() + ".json");
            Files.writeString(file, toJson(result), StandardCharsets.UTF_8);
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write result JSON", e);
        }
    }

    static Path appendCsv(ExperimentResult result, Path resultsDir) {
        try {
            Path studyDir = resultsDir.resolve(sanitize(result.study()));
            Files.createDirectories(studyDir);
            Path file = studyDir.resolve("summary.csv");
            boolean writeHeader = Files.notExists(file) || Files.size(file) == 0;
            if (!writeHeader) {
                try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                    if (!csvHeader().equals(reader.readLine())) throw new IOException("기존 CSV 열 구성이 다릅니다. 새 결과 폴더를 사용하세요: " + file);
                }
            }
            try (var writer =
                    Files.newBufferedWriter(
                            file, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                if (writeHeader) {
                    writer.write(csvHeader());
                    writer.write("\n");
                }
                writer.write(csvRow(result));
                writer.write("\n");
            }
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append result CSV", e);
        }
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static String csvHeader() {
        return String.join(
                ",",
                List.of(
                        "executionId", "startedAtEpochMillis", "study", "variant", "scenario", "seed", "runNumber", "repeatCount",
                        "nodeCount", "concurrency", "serverWorkerCountHint", "storageLayer",
                        "warmupRequests", "warmupElapsedNanos", "measuredRequests", "measuredElapsedNanos",
                        "throughputPerSecond", "p50Nanos", "p99Nanos", "latencySampleCount",
                        "latencyTotalObserved", "hit", "miss", "writeOk", "busy", "timeout", "unavailable",
                        "serverError", "clientFailure", "gitCommit",
                        "resultSchemaVersion"));
    }

    private static String csvRow(ExperimentResult r) {
        Map<Outcome, Long> counts = r.outcomeCounts();
        return String.join(
                ",",
                List.of(
                        r.executionId(),
                        Long.toString(r.startedAtEpochMillis()),
                        csvField(r.study()),
                        csvField(r.variant()),
                        csvField(r.scenario()),
                        Long.toString(r.seed()),
                        Integer.toString(r.runNumber()),
                        Integer.toString(r.repeatCount()),
                        Integer.toString(r.nodeCount()),
                        Integer.toString(r.concurrency()),
                        Integer.toString(r.serverWorkerCountHint()),
                        csvField(r.storageLayer()),
                        Long.toString(r.warmupRequests()),
                        Long.toString(r.warmupElapsedNanos()),
                        Long.toString(r.measuredRequests()),
                        Long.toString(r.measuredElapsedNanos()),
                        String.format(Locale.ROOT, "%.3f", r.throughputPerSecond()),
                        Long.toString(r.p50Nanos()),
                        Long.toString(r.p99Nanos()),
                        Integer.toString(r.latencySampleCount()),
                        Long.toString(r.latencyTotalObserved()),
                        Long.toString(counts.getOrDefault(Outcome.HIT, 0L)),
                        Long.toString(counts.getOrDefault(Outcome.MISS, 0L)),
                        Long.toString(counts.getOrDefault(Outcome.WRITE_OK, 0L)),
                        Long.toString(counts.getOrDefault(Outcome.BUSY, 0L)),
                        Long.toString(counts.getOrDefault(Outcome.TIMEOUT, 0L)),
                        Long.toString(counts.getOrDefault(Outcome.UNAVAILABLE, 0L)),
                        Long.toString(counts.getOrDefault(Outcome.SERVER_ERROR, 0L)),
                        Long.toString(counts.getOrDefault(Outcome.CLIENT_FAILURE, 0L)),
                        csvField(r.gitCommit().orElse("")),
                        csvField(r.resultSchemaVersion())));
    }

    private static String csvField(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private static String toJson(ExperimentResult r) {
        List<String> entries = new ArrayList<>();
        entries.add(entry("executionId", r.executionId()));
        entries.add(entry("resultSchemaVersion", r.resultSchemaVersion()));
        entries.add(entry("startedAtEpochMillis", r.startedAtEpochMillis()));
        entries.add(entry("study", r.study()));
        entries.add(entry("variant", r.variant()));
        entries.add(entry("scenario", r.scenario()));
        entries.add(entry("seed", r.seed()));
        entries.add(entry("runNumber", r.runNumber()));
        entries.add(entry("repeatCount", r.repeatCount()));
        entries.add(entryStringMap("configSnapshot", r.configSnapshot()));
        entries.add(entryInvocation("invocation", r.invocation().orElse(null)));
        entries.add(entry("nodeCount", r.nodeCount()));
        entries.add(entry("concurrency", r.concurrency()));
        entries.add(
                entryNullableLong(
                        "serverWorkerCountHint", r.serverWorkerCountHint() < 0 ? null : (long) r.serverWorkerCountHint()));
        entries.add(entry("storageLayer", r.storageLayer()));
        entries.add(entry("jdkVendor", r.jdkVendor()));
        entries.add(entry("jdkVersion", r.jdkVersion()));
        entries.add(entryStringArray("runnerJvmArgs", r.runnerJvmArgs()));
        entries.add(entryNullableString("serverJvmOptsNote", r.serverJvmOptsNote().isBlank() ? null : r.serverJvmOptsNote()));
        entries.add(entry("runnerOsName", r.osName()));
        entries.add(entry("runnerOsArch", r.osArch()));
        entries.add(entry("runnerAvailableProcessors", r.availableProcessors()));
        entries.add(entry("warmupRequests", r.warmupRequests()));
        entries.add(entry("warmupElapsedNanos", r.warmupElapsedNanos()));
        entries.add(entry("measuredRequests", r.measuredRequests()));
        entries.add(entry("measuredElapsedNanos", r.measuredElapsedNanos()));
        entries.add(entry("throughputPerSecond", r.throughputPerSecond()));
        entries.add(entry("p50Nanos", r.p50Nanos()));
        entries.add(entry("p99Nanos", r.p99Nanos()));
        entries.add(entry("latencySampleCount", r.latencySampleCount()));
        entries.add(entry("latencyTotalObserved", r.latencyTotalObserved()));
        entries.add(entryOutcomeCounts(r.outcomeCounts()));
        entries.add(entry("faultScenario", r.faultScenario()));
        entries.add(entryNullableString("gitCommit", r.gitCommit().orElse(null)));
        entries.add(entryTargets(r.targets()));

        return "{\n  " + String.join(",\n  ", entries) + "\n}\n";
    }

    private static String entryOutcomeCounts(Map<Outcome, Long> counts) {
        List<String> parts = new ArrayList<>();
        for (Outcome outcome : Outcome.values()) {
            parts.add(quote(outcome.name()) + ": " + counts.getOrDefault(outcome, 0L));
        }
        return quote("outcomeCounts") + ": {" + String.join(", ", parts) + "}";
    }

    /** 서버별 측정 결과. 단일 서버 실행도 원소 하나로 쓴다. */
    private static String entryTargets(List<TargetResult> targets) {
        List<String> objects = new ArrayList<>();
        for (TargetResult t : targets) {
            objects.add(
                    "{"
                            + String.join(
                                    ", ",
                                    entry("address", t.address()),
                                    entry("hashRange", t.hashRange()),
                                    entry("requests", t.requests()),
                                    entry("p50Nanos", t.p50Nanos()),
                                    entry("p99Nanos", t.p99Nanos()),
                                    entryOutcomeCounts(t.outcomeCounts()))
                            + "}");
        }
        return quote("targets") + ": [" + String.join(", ", objects) + "]";
    }

    private static String entryStringArray(String key, List<String> values) {
        List<String> quoted = new ArrayList<>();
        for (String value : values) {
            quoted.add(quote(value));
        }
        return quote(key) + ": [" + String.join(", ", quoted) + "]";
    }

    private static String entryStringMap(String key, Map<String, String> values) {
        List<String> entries = new ArrayList<>();
        for (Map.Entry<String, String> entry : new TreeMap<>(values).entrySet()) {
            entries.add(quote(entry.getKey()) + ": " + quote(entry.getValue()));
        }
        return quote(key) + ": {" + String.join(", ", entries) + "}";
    }

    private static String entryInvocation(String key, RunnerInvocation invocation) {
        if (invocation == null) {
            return quote(key) + ": null";
        }
        return quote(key)
                + ": {"
                + quote("command")
                + ": "
                + quote(invocation.command())
                + ", "
                + quote("configPath")
                + ": "
                + quote(invocation.configPath())
                + ", "
                + entryStringArray("arguments", invocation.arguments())
                + "}";
    }

    private static String entry(String key, String value) {
        return quote(key) + ": " + quote(value);
    }

    private static String entry(String key, long value) {
        return quote(key) + ": " + value;
    }

    private static String entry(String key, double value) {
        return quote(key) + ": " + value;
    }

    private static String entryNullableLong(String key, Long value) {
        return quote(key) + ": " + (value == null ? "null" : value);
    }

    private static String entryNullableString(String key, String value) {
        return quote(key) + ": " + (value == null ? "null" : quote(value));
    }

    private static String quote(String value) {
        StringBuilder escaped = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.append('"').toString();
    }
}
