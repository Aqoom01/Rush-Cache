package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InterruptionExperimentTest {
    @Test
    void matrixSeparatesApplicableAndUnsupportedContracts() {
        int supported = 0;
        for (var boundary : InterruptionExperiment.Boundary.values()) for (String variant : ComparisonExperiment.VARIANTS) {
            if (boundary.supported(variant)) supported++; else assertFalse(boundary.reason().isBlank());
        }
        assertEquals(18, supported);
        assertEquals(135, InterruptionResearchMain.expected("full"));
        assertEquals(135, InterruptionResearchMain.expected("quick"));
        assertEquals(150, InterruptionResearchMain.expected("extended-full"));
        assertEquals(150, InterruptionResearchMain.expected("extended-quick"));
        assertEquals(10_000, InterruptionExperiment.condition("full").keys());
        assertEquals(10_000, InterruptionExperiment.config(InterruptionExperiment.condition("full")).get("maxTicks"));
        assertThrows(IllegalArgumentException.class, () -> InterruptionExperiment.condition("unknown"));
        assertThrows(IllegalStateException.class, () -> InterruptionExperiment.run(InterruptionExperiment.condition("quick"),
                InterruptionExperiment.Boundary.CATCHING_UP_ABORT, 1, "stop-and-copy"));
    }

    @Test
    void partialDeltaIsAppliedAndAcknowledgedBeforeAbortForEverySeed() {
        for (String profile : List.of("extended-quick", "extended-full")) for (int seed = 1; seed <= 5; seed++) {
            var c = InterruptionExperiment.condition(profile);
            var run = InterruptionExperiment.run(c, InterruptionExperiment.Boundary.CATCHING_UP_PARTIAL_ABORT, seed, "pre-copy");
            assertEquals("ABORTED", run.terminal());
            assertEquals("CATCHING_UP", run.injectionState());
            assertEquals(1, metric(run, "source_exact_comparisons"));
            assertEquals(0, metric(run, "ownership_commits"));
            assertTrue(metric(run, "delta_bytes_before_abort") > 0);
            assertTrue(metric(run, "delta_records_acknowledged_before_abort") > 0);
            assertEquals(metric(run, "sent_bytes"), metric(run, "snapshot_bytes_before_delta") + metric(run, "delta_bytes_before_abort"));
            var injection = run.events().stream().filter(e -> ("inject:" + InterruptionExperiment.Boundary.CATCHING_UP_PARTIAL_ABORT).equals(e.get("action"))).findFirst().orElseThrow();
            assertEquals(true, injection.get("partialLiveDelta"));
            assertEquals(metric(run, "delta_records_acknowledged_before_abort"), ((Number) injection.get("acknowledgedSequence")).doubleValue());
        }
    }

    @Test
    void extendedSuiteRecordsPartialTransferAndRejectsMissingEvidence(@TempDir Path folder) throws Exception {
        Files.writeString(folder.resolve("launch.properties"), "suiteExecutionId=s6-extended-test\nrepeat.count=1\nbackend.option.profile=extended-quick\n", StandardCharsets.UTF_8);
        InterruptionResearchMain.main(new String[] {folder.toString(), "extended-quick", "1", "1"});
        List<Path> files;
        try (var paths = Files.list(folder.resolve("results"))) { files = paths.sorted().toList(); }
        assertEquals(150, files.size());
        assertTrue(Files.readString(folder.resolve("results.md")).contains("실제 실행 90회, 미지원 60행"));
        Path partial = folder.resolve("results/S6-CATCHING_UP_PARTIAL_ABORT-seed1-pre-copy.json");
        String original = Files.readString(partial);
        var root = BaselineJson.object(BaselineJson.decode(original));
        var metrics = BaselineJson.object(root.get("metrics"));
        metrics.put("delta_records_acknowledged_before_abort", Map.of("value", 0, "unit", "count"));
        root.put("metrics", metrics);
        Files.writeString(partial, BaselineJson.encode(root));
        assertThrows(IllegalStateException.class, () -> InterruptionResearchMain.regenerate(folder, "extended-quick"));
        root = BaselineJson.object(BaselineJson.decode(original));
        @SuppressWarnings("unchecked")
        var events = (List<Object>) root.get("phaseEvents");
        for (int i = 0; i < events.size(); i++) {
            var event = BaselineJson.object(events.get(i));
            if ("inject:CATCHING_UP_PARTIAL_ABORT".equals(event.get("action"))) {
                event.put("partialLiveDelta", false);
                events.set(i, event);
            }
        }
        Files.writeString(partial, BaselineJson.encode(root));
        assertThrows(IllegalStateException.class, () -> InterruptionResearchMain.regenerate(folder, "extended-quick"));
        Files.writeString(partial, original);
        String csv = Files.readString(folder.resolve("summary.csv"));
        InterruptionResearchMain.main(new String[] {"--rebuild", folder.toString()});
        assertEquals(csv, Files.readString(folder.resolve("summary.csv")));
    }

    @Test
    void everyApplicableBoundaryIsReachedAndCheckedInBothProfiles() {
        for (String profile : List.of("quick", "full")) {
            var c = InterruptionExperiment.condition(profile);
            String workload = InterruptionExperiment.workloadHash(c, 1);
            for (var boundary : InterruptionExperiment.Boundary.values()) for (String variant : ComparisonExperiment.VARIANTS) {
                if (!boundary.supported(variant)) continue;
                var run = InterruptionExperiment.run(c, boundary, 1, variant);
                assertEquals(boundary.terminal(), run.terminal(), boundary + "/" + variant);
                assertEquals(workload, run.workloadHash());
                assertEquals(0, metric(run, "invariant_violations"));
                assertEquals(1, metric(run, "source_exact_comparisons") + metric(run, "destination_exact_comparisons")
                        + metric(run, "retained_state_exact_comparisons"));
                assertEquals(boundary.cancelsBeforeCommit() ? 0 : 1, metric(run, "ownership_commits"));
                if (boundary == InterruptionExperiment.Boundary.PARTIAL_CLEANUP_ABORT) assertEquals(3, metric(run, "cleanup_calls"));
                if (boundary.sourceFailure()) {
                    assertTrue(metric(run, "reads_unavailable") > 0);
                    assertTrue(metric(run, "writes_accepted") >= 2);
                    assertEquals(1, metric(run, "abort_refused"));
                }
            }
        }
    }

    @Test
    void heldFinalAckAndNeverRecoveredSourceCannotBecomeSuccessfulCommits() {
        var c = InterruptionExperiment.condition("quick");
        for (String variant : List.of("stop-and-copy", "pre-copy")) {
            var run = InterruptionExperiment.run(c, InterruptionExperiment.Boundary.FINAL_ACK_WAIT_ABORT, 2, variant);
            assertEquals("ABORTED", run.terminal());
            assertEquals(0, metric(run, "ownership_commits"));
            assertEquals(variant.equals("stop-and-copy") ? "COPYING" : "SWITCHING", run.injectionState());
        }
        var failed = InterruptionExperiment.run(c, InterruptionExperiment.Boundary.SOURCE_DOWN_UNRECOVERED, 2, "post-copy");
        assertEquals("FAILED", failed.terminal());
        assertEquals(0, metric(failed, "cleanup_calls"));
        assertEquals(0, metric(failed, "destination_exact_comparisons"));
        assertEquals(1, metric(failed, "retained_state_exact_comparisons"));
        var timeout = InterruptionExperiment.run(c, InterruptionExperiment.Boundary.PULL_TIMEOUT_RESUME, 2, "post-copy");
        assertEquals("DONE", timeout.terminal());
        assertEquals(3, metric(timeout, "timeout_wait_ticks"));
        assertEquals(3, metric(timeout, "failed_observation_ticks"));
    }

    @Test
    void suiteRebuildsAndRejectsMissingTamperedAndFabricatedUnsupportedRuns(@TempDir Path folder) throws Exception {
        Files.writeString(folder.resolve("launch.properties"), "suiteExecutionId=s6-test\nrepeat.count=1\nbackend.option.profile=quick\n", StandardCharsets.UTF_8);
        InterruptionResearchMain.main(new String[] {folder.toString(), "quick", "1", "1"});
        List<Path> files;
        try (var paths = Files.list(folder.resolve("results"))) { files = paths.sorted().toList(); }
        assertEquals(135, files.size());
        String csv = Files.readString(folder.resolve("summary.csv"));
        String report = Files.readString(folder.resolve("results.md"));
        assertEquals(28, csv.lines().count());
        assertTrue(csv.lines().allMatch(line -> line.split(",", -1).length == 15));
        assertTrue(report.contains("실제 실행 85회, 미지원 50행"));
        assertTrue(report.contains("ABORTED 40회, 이동 DONE 40회, source 미복구 FAILED 5회"));
        InterruptionResearchMain.main(new String[] {"--rebuild", folder.toString()});
        assertEquals(csv, Files.readString(folder.resolve("summary.csv")));
        assertEquals(report, Files.readString(folder.resolve("results.md")));
        Path supported = files.stream().filter(p -> p.getFileName().toString().equals("S6-FINAL_ACK_WAIT_ABORT-seed1-pre-copy.json")).findFirst().orElseThrow();
        String original = Files.readString(supported);
        for (String name : List.of("protocolOutcome", "inputHash", "workloadHash", "injectionState", "phaseEvents")) {
            var changed = BaselineJson.object(BaselineJson.decode(original));
            changed.put(name, name.equals("phaseEvents") ? List.of() : "tampered");
            Files.writeString(supported, BaselineJson.encode(changed));
            assertThrows(IllegalStateException.class, () -> InterruptionResearchMain.regenerate(folder, "quick"));
        }
        Files.writeString(supported, original);
        Path unsupported = files.stream().filter(p -> p.getFileName().toString().equals("S6-CATCHING_UP_ABORT-seed1-stop-and-copy.json")).findFirst().orElseThrow();
        String unsupportedOriginal = Files.readString(unsupported);
        for (String field : List.of("metrics", "protocolOutcome", "injectionState", "terminationReason")) {
            var fabricated = BaselineJson.object(BaselineJson.decode(unsupportedOriginal));
            fabricated.put(field, field.equals("metrics") ? Map.of("invariant_violations", Map.of("value", 0, "unit", "count")) : "DONE");
            Files.writeString(unsupported, BaselineJson.encode(fabricated));
            assertThrows(IllegalStateException.class, () -> InterruptionResearchMain.regenerate(folder, "quick"));
        }
        Files.writeString(unsupported, unsupportedOriginal);
        Path duplicate = supported.resolveSibling("duplicate.json");
        Files.copy(supported, duplicate);
        assertThrows(IllegalStateException.class, () -> InterruptionResearchMain.regenerate(folder, "quick"));
        Files.delete(duplicate);
        Files.move(supported, supported.resolveSibling("missing.txt"));
        assertThrows(IllegalStateException.class, () -> InterruptionResearchMain.regenerate(folder, "quick"));
    }

    private static double metric(InterruptionExperiment.Run run, String name) {
        return InterruptionResearchMain.metric(Map.of("metrics", run.metrics()), name);
    }
}
