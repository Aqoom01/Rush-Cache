package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultWriterTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void writesRequiredArtifactsAndMeasurementSourcesWithoutOverwriting() throws Exception {
        List<ExperimentResult> results = new ExperimentRunner().runAllProfiles(
                ScenarioConfig.defaults(ScenarioType.CRASH),
                2
        );
        Path output = temporaryDirectory.resolve("results");
        ResultWriter writer = new ResultWriter();

        writer.write(output, results);

        assertTrue(Files.exists(output.resolve("run-manifest.json")));
        assertTrue(Files.exists(output.resolve("events.jsonl")));
        assertTrue(Files.exists(output.resolve("runs.csv")));
        assertTrue(Files.exists(output.resolve("summary.csv")));
        assertTrue(Files.exists(output.resolve("module-contributions.csv")));
        assertTrue(Files.exists(output.resolve("compatibility.csv")));
        String runs = Files.readString(output.resolve("runs.csv"));
        String summary = Files.readString(output.resolve("summary.csv"));
        String events = Files.readString(output.resolve("events.jsonl"));
        String manifest = Files.readString(output.resolve("run-manifest.json"));
        String contributions = Files.readString(output.resolve("module-contributions.csv"));
        String compatibility = Files.readString(output.resolve("compatibility.csv"));
        assertTrue(runs.contains("latency_source,protocol_cost_source"));
        assertTrue(runs.contains("simulation-observed"));
        assertTrue(runs.contains("model-estimated"));
        assertTrue(summary.contains("missing_n,mean,p50,p95,p99,ci95_low,ci95_high"));
        assertTrue(summary.contains("dead_detection_rate"));
        assertTrue(events.contains("\"eventType\":\"DIRECT_TIMEOUT\""));
        assertTrue(events.contains("\"eventType\":\"SUSPICION_DEADLINE_SCHEDULED\""));
        assertTrue(events.contains("\"eventType\":\"SIMULATION_ENDED\""));
        assertTrue(manifest.contains("\"modules\""));
        assertTrue(contributions.contains("A:F0-vs-F4"));
        assertTrue(contributions.contains("C:F5-vs-F6"));
        assertTrue(compatibility.contains("PHI_ACCRUAL"));
        assertTrue(compatibility.contains("false"));
        assertThrows(FileAlreadyExistsException.class, () -> writer.write(output, results));
    }
}
