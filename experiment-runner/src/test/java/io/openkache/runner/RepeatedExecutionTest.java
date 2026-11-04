package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RepeatedExecutionTest {

    @TempDir Path resultsDir;

    @Test
    void omitsWorkingTreeStatusAndRejectsAppendingToTheOldCsvFormat() throws Exception {
        var result = result(1, 10.0);
        Path json = ResultWriter.writeJson(result, resultsDir);
        Path csv = ResultWriter.appendCsv(result, resultsDir);
        assertFalse(Files.readString(json).contains("workingTreeDirty"));
        assertFalse(Files.readString(csv).contains("workingTreeDirty"));
        ResultWriter.appendCsv(result(2, 20.0), resultsDir);
        var rows = Files.readAllLines(csv);
        assertEquals(3, rows.size());
        assertEquals(rows.getFirst().split(",", -1).length, rows.get(1).split(",", -1).length);
        String legacy = Files.readString(csv).replace("gitCommit,", "gitCommit,workingTreeDirty,");
        Files.writeString(csv, legacy);
        assertThrows(java.io.UncheckedIOException.class, () -> ResultWriter.appendCsv(result, resultsDir));
        assertEquals(legacy, Files.readString(csv));
    }

    @Test
    void continuesAfterFailureAndSummarizesSuccessfulRuns() throws Exception {
        RunnerConfig config = config(3, 1_000);
        AtomicInteger calls = new AtomicInteger();

        List<RunAttempt> attempts =
                Main.execute(
                        config,
                        (ignored, runNumber, timeoutMs) -> {
                            calls.incrementAndGet();
                            if (runNumber == 1) {
                                return RunAttempt.failed(runNumber, "connection refused");
                            }
                            return RunAttempt.success(runNumber, result(runNumber, runNumber * 10.0));
                        });

        ExperimentSummary summary = ExperimentSummary.from(config.repeatCount(), attempts);
        Path firstResult = config.resultsDir().resolve("repeat-it/run-2.json");
        Path secondResult = config.resultsDir().resolve("repeat-it/run-3.json");
        Path summaryFile =
                SummaryWriter.write(config, attempts, summary, Map.of(2, firstResult, 3, secondResult));

        assertEquals(3, calls.get());
        assertEquals(2, summary.successfulRuns());
        assertEquals(1, summary.failedRuns());
        assertEquals(0, summary.timeoutRuns());
        assertEquals(0, summary.skippedRuns());
        assertEquals(25.0, summary.throughputPerSecond().average());
        assertEquals(20.0, summary.throughputPerSecond().minimum());
        assertEquals(30.0, summary.throughputPerSecond().maximum());

        String json = Files.readString(summaryFile);
        assertTrue(json.contains("\"successfulRuns\": 2"));
        assertTrue(json.contains("\"failedRuns\": 1"));
        assertTrue(json.contains("\"average\": 25.000"));
        assertTrue(json.contains("connection refused"));
        assertTrue(json.contains("\"executionId\": null"));
        assertTrue(json.contains("\"resultFile\": null"));
        assertTrue(json.contains("\"executionId\": \"run-2\""));
        assertTrue(json.contains("\"resultFile\": \"repeat-it/run-2.json\""));
    }

    @Test
    void stopsStartingNewRunsWhenTotalTimeoutExpires() {
        RunnerConfig config = config(3, 1_000, 10);
        AtomicInteger calls = new AtomicInteger();

        List<RunAttempt> attempts =
                Main.execute(
                        config,
                        (ignored, runNumber, timeoutMs) -> {
                            calls.incrementAndGet();
                            try {
                                Thread.sleep(30);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return RunAttempt.success(runNumber, result(runNumber, 10.0));
                        });

        ExperimentSummary summary = ExperimentSummary.from(config.repeatCount(), attempts);
        assertEquals(1, calls.get());
        assertEquals(3, attempts.size());
        assertEquals(1, summary.successfulRuns());
        assertEquals(0, summary.timeoutRuns());
        assertEquals(2, summary.skippedRuns());
        assertEquals(RunStatus.SKIPPED, attempts.get(1).status());
        assertEquals(RunStatus.SKIPPED, attempts.get(2).status());
    }

    @Test
    void marksSlowSingleRunAsTimedOut() {
        AtomicInteger cleanupCount = new AtomicInteger();
        RunAttempt attempt =
                Main.runOnce(
                        1,
                        10,
                        () -> {
                            try {
                                Thread.sleep(200);
                                return result(1, 10.0);
                            } finally {
                                cleanupCount.incrementAndGet();
                            }
                        });

        assertEquals(RunStatus.TIMEOUT, attempt.status());
        assertEquals(1, cleanupCount.get());
    }

    @Test
    void usesTheSmallerOfRunAndOverallTimeout() {
        RunnerConfig config = config(1, 20, 1_000);
        AtomicInteger receivedTimeoutMs = new AtomicInteger();

        Main.execute(
                config,
                (ignored, runNumber, timeoutMs) -> {
                    receivedTimeoutMs.set((int) timeoutMs);
                    return RunAttempt.success(runNumber, result(runNumber, 10.0));
                });

        assertEquals(20, receivedTimeoutMs.get());
    }

    @Test
    void preservesEarlierSummaryFiles() {
        RunnerConfig config = config(1, 1_000);
        List<RunAttempt> attempts = List.of(RunAttempt.success(1, result(1, 10.0)));
        ExperimentSummary summary = ExperimentSummary.from(1, attempts);

        Path resultFile = config.resultsDir().resolve("repeat-it/run-1.json");
        Path first = SummaryWriter.write(config, attempts, summary, Map.of(1, resultFile));
        Path second = SummaryWriter.write(config, attempts, summary, Map.of(1, resultFile));

        assertTrue(Files.exists(first));
        assertTrue(Files.exists(second));
        assertTrue(!first.equals(second));
    }

    private RunnerConfig config(int repeatCount, long executionTimeoutMs) {
        return config(repeatCount, 600_000, executionTimeoutMs);
    }

    private RunnerConfig config(int repeatCount, long runTimeoutMs, long executionTimeoutMs) {
        Properties properties = new Properties();
        properties.setProperty("target.host", "127.0.0.1");
        properties.setProperty("target.port", "7000");
        properties.setProperty("study", "repeat-it");
        properties.setProperty("variant", "baseline");
        properties.setProperty("scenario", "none");
        properties.setProperty("seed", "1");
        properties.setProperty("key.count", "10");
        properties.setProperty("key.length", "16");
        properties.setProperty("value.size", "8");
        properties.setProperty("mix.get", "100");
        properties.setProperty("mix.set", "0");
        properties.setProperty("mix.delete", "0");
        properties.setProperty("distribution", "uniform");
        properties.setProperty("concurrency", "1");
        properties.setProperty("warmup.requests", "0");
        properties.setProperty("measured.requests", "1");
        properties.setProperty("request.timeout.ms", "100");
        properties.setProperty("repeat.count", String.valueOf(repeatCount));
        properties.setProperty("run.timeout.ms", String.valueOf(runTimeoutMs));
        properties.setProperty("execution.timeout.ms", String.valueOf(executionTimeoutMs));
        properties.setProperty("results.dir", resultsDir.toString());
        return RunnerConfig.from(properties);
    }

    private static ExperimentResult result(int runNumber, double throughput) {
        return new ExperimentResult(
                "run-" + runNumber,
                "experiment-result-v1",
                0,
                "repeat-it",
                "baseline",
                "none",
                1,
                runNumber,
                3,
                Map.of(),
                Optional.empty(),
                1,
                1,
                -1,
                "memory",
                "test",
                "test",
                List.of(),
                "",
                "test",
                "test",
                1,
                0,
                0,
                1,
                1,
                throughput,
                100,
                200,
                1,
                1,
                new EnumMap<>(Outcome.class),
                "none",
                Optional.empty(),
                List.of());
    }
}
