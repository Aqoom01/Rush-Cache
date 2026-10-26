package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SensitivityExperimentTest {
    @Test
    void fullMatrixCoversEveryPlannedSettingWithoutDuplicates() {
        var cases = ComparisonExperiment.cases("sensitivity-full");
        assertEquals(45, cases.size());
        assertEquals(45, new HashSet<>(cases.stream().map(ComparisonExperiment.Case::id).toList()).size());
        assertEquals(java.util.Set.of(65_536L, 262_144L, 1_048_576L),
                new HashSet<>(cases.stream().map(ComparisonExperiment.Case::threshold).toList()));
        assertEquals(java.util.Set.of(3, 10, 30), new HashSet<>(cases.stream().map(ComparisonExperiment.Case::maxRounds).toList()));
        assertTrue(cases.stream().allMatch(c -> c.keys() == 100_000 && c.budget() == 65_536));
        assertThrows(IllegalArgumentException.class, () -> ComparisonExperiment.cases("unknown"));
    }

    @Test
    void changingProtocolSettingsPreservesWorkloadAndBaseline() {
        var a = new ComparisonExperiment.Case("a", "S3-sensitivity", 1_000, 512, .5, 30, 0, 64, 1);
        var b = new ComparisonExperiment.Case("b", "S3-sensitivity", 1_000, 512, .5, 30, 0, 1_048_576, 30);
        var first = new ComparisonExperiment.Trace(a, 1);
        var second = new ComparisonExperiment.Trace(b, 1);
        assertEquals(first.initial(), second.initial());
        for (int tick = 0; tick < 100; tick++) assertEquals(first.at(tick), second.at(tick));
        assertEquals(ComparisonExperiment.workloadHash(a, 1), ComparisonExperiment.workloadHash(b, 1));
        assertNotEquals(ComparisonExperiment.workloadHash(a, 1), ComparisonExperiment.workloadHash(a, 2));
        for (String variant : java.util.List.of("stop-and-copy", "post-copy")) {
            assertEquals(ComparisonExperiment.run(a, 1, variant).metrics(), ComparisonExperiment.run(b, 1, variant).metrics());
        }
        var limited = ComparisonExperiment.run(a, 1, "pre-copy");
        var threshold = ComparisonExperiment.run(b, 1, "pre-copy");
        assertEquals("ROUND_LIMIT", limited.stopReason());
        assertEquals(1, ((Number) BaselineJson.object(limited.metrics().get("live_delta_rounds")).get("value")).intValue());
        assertEquals("THRESHOLD", threshold.stopReason());
        assertTrue(ComparisonResearchMain.metric(Map.of("metrics", limited.metrics()), "total_ticks")
                > ComparisonResearchMain.metric(Map.of("metrics", threshold.metrics()), "total_ticks"));
    }

    @Test
    void sensitivityReportRebuildsAndRejectsChangedSettingsWorkloadAndMissingRuns(@TempDir Path folder) throws Exception {
        Files.writeString(folder.resolve("launch.properties"),
                "suiteExecutionId=sensitivity-test\nrepeat.count=1\nbackend.option.profile=sensitivity-quick\n", StandardCharsets.UTF_8);
        ComparisonResearchMain.main(new String[] {folder.toString(), "sensitivity-quick", "1", "1"});
        java.util.List<Path> originals;
        try (var paths = Files.list(folder.resolve("results"))) { originals = paths.sorted().toList(); }
        assertEquals(675, originals.size());
        String csv = Files.readString(folder.resolve("summary.csv"));
        String markdown = Files.readString(folder.resolve("results.md"));
        assertEquals(136, csv.lines().count());
        assertTrue(markdown.contains("675/675"));
        ComparisonResearchMain.main(new String[] {"--rebuild", folder.toString()});
        assertEquals(csv, Files.readString(folder.resolve("summary.csv")));
        assertEquals(markdown, Files.readString(folder.resolve("results.md")));
        Path file = originals.stream().filter(p -> p.getFileName().toString().endsWith("-pre-copy.json")).findFirst().orElseThrow();
        String original = Files.readString(file);
        for (String field : java.util.List.of("candidateConfig", "workloadHash", "stopPhaseReason")) {
            var root = BaselineJson.object(BaselineJson.decode(original));
            root.put(field, field.equals("candidateConfig") ? Map.of() : "tampered");
            Files.writeString(file, BaselineJson.encode(root));
            assertThrows(IllegalStateException.class, () -> ComparisonResearchMain.regenerate(folder, "sensitivity-quick"));
        }
        Files.writeString(file, original);
        Files.move(file, file.resolveSibling("missing.txt"));
        assertThrows(IllegalStateException.class, () -> ComparisonResearchMain.regenerate(folder, "sensitivity-quick"));
    }
}
