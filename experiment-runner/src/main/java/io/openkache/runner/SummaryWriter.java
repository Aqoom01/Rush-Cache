package io.openkache.runner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/** 반복 실행의 성공·실패와 공통 통계를 JSON 한 파일로 기록한다. */
final class SummaryWriter {

    private SummaryWriter() {}

    static Path write(
            RunnerConfig config,
            List<RunAttempt> attempts,
            ExperimentSummary summary,
            Map<Integer, Path> resultFiles) {
        try {
            Path studyDir = config.resultsDir().resolve(config.study().replaceAll("[^a-zA-Z0-9._-]", "_"));
            Files.createDirectories(studyDir);
            Path file = studyDir.resolve(summaryFileName(config));
            Files.writeString(file, json(config, attempts, summary, resultFiles), StandardCharsets.UTF_8);
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write run summary", e);
        }
    }

    private static String summaryFileName(RunnerConfig config) {
        return "run-summary-"
                + sanitize(config.variant())
                + "-"
                + sanitize(config.scenario())
                + "-"
                + Instant.now().toEpochMilli()
                + "-"
                + UUID.randomUUID()
                + ".json";
    }

    private static String json(
            RunnerConfig config,
            List<RunAttempt> attempts,
            ExperimentSummary summary,
            Map<Integer, Path> resultFiles) {
        List<String> fields = new ArrayList<>();
        fields.add(field("study", config.study()));
        fields.add(field("variant", config.variant()));
        fields.add(field("scenario", config.scenario()));
        fields.add(field("requestedRuns", summary.requestedRuns()));
        fields.add(field("successfulRuns", summary.successfulRuns()));
        fields.add(field("failedRuns", summary.failedRuns()));
        fields.add(field("timeoutRuns", summary.timeoutRuns()));
        fields.add(field("skippedRuns", summary.skippedRuns()));
        fields.add(metric("throughputPerSecond", summary.throughputPerSecond()));
        fields.add(metric("p50Nanos", summary.p50Nanos()));
        fields.add(metric("p99Nanos", summary.p99Nanos()));
        fields.add(attempts(config.resultsDir(), attempts, resultFiles));
        return "{\n  " + String.join(",\n  ", fields) + "\n}\n";
    }

    private static String attempts(Path resultsDir, List<RunAttempt> attempts, Map<Integer, Path> resultFiles) {
        List<String> rows = new ArrayList<>();
        for (RunAttempt attempt : attempts) {
            String error = attempt.error().map(value -> ", \"error\": " + quote(value)).orElse("");
            String result = resultFields(resultsDir, attempt, resultFiles.get(attempt.runNumber()));
            rows.add(
                    "{\"runNumber\": "
                            + attempt.runNumber()
                            + ", \"status\": "
                            + quote(attempt.status().name())
                            + result
                            + error
                            + "}");
        }
        return "\"attempts\": [" + String.join(", ", rows) + "]";
    }

    private static String resultFields(Path resultsDir, RunAttempt attempt, Path resultFile) {
        if (attempt.result().isEmpty()) {
            return ", \"executionId\": null, \"resultFile\": null";
        }
        String resultPath =
                resultFile == null
                        ? "null"
                        : quote(
                                resultsDir
                                        .toAbsolutePath()
                                        .normalize()
                                        .relativize(resultFile.toAbsolutePath().normalize())
                                        .toString()
                                        .replace('\\', '/'));
        return ", \"executionId\": "
                + quote(attempt.result().orElseThrow().executionId())
                + ", \"resultFile\": "
                + resultPath;
    }

    private static String metric(String name, ExperimentSummary.MetricSummary metric) {
        if (!metric.measured()) {
            return quote(name) + ": null";
        }
        return quote(name)
                + ": {\"average\": "
                + number(metric.average())
                + ", \"minimum\": "
                + number(metric.minimum())
                + ", \"maximum\": "
                + number(metric.maximum())
                + "}";
    }

    private static String field(String name, int value) {
        return quote(name) + ": " + value;
    }

    private static String field(String name, String value) {
        return quote(name) + ": " + quote(value);
    }

    private static String number(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String quote(String value) {
        return "\""
                + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r")
                + "\"";
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
