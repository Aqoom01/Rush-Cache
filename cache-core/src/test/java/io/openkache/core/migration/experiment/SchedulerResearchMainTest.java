package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SchedulerResearchMainTest {
    @TempDir Path folder;
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mutable(Object value) { BaselineJson.object(value); return (Map<String, Object>) value; }
    private static Map<String, Object> first(Object value) { return mutable(((List<?>) value).getFirst()); }
    private static Map<String, Object> row(SchedulerComparisonExperiment.Condition c, String method, String variant) {
        return BaselineJson.object(BaselineJson.decode(BaselineJson.encode(SchedulerResearchMain.record(c, method, variant, 1))));
    }
    private static void resign(Map<String, Object> row) { row.put("evidenceHash", SchedulerResearchMain.evidenceHash(row)); }

    @Test void jsonRoundTripRetainsEvidenceForAllCopyVariants() {
        var c = SchedulerComparisonExperiment.condition("heterogeneous", "quick");
        for (String variant : MovementExperiment.VARIANTS) {
            var row = row(c, "mongodb", variant);
            assertDoesNotThrow(() -> SchedulerResearchMain.validate(row, c, "mongodb", variant, 1));
        }
        assertEquals(855, SchedulerResearchMain.expected("full"));
        assertEquals(342, SchedulerResearchMain.expected("quick"));
    }
    @Test void repeatEvidenceIgnoresMeasuredPlannerTime() {
        var c = SchedulerComparisonExperiment.condition("heterogeneous", "quick");
        var first = row(c, "vmbuddies", "pre-copy");
        var second = row(c, "vmbuddies", "pre-copy");
        assertEquals(first.get("evidenceHash"), second.get("evidenceHash"));
        assertEquals(first.get("metrics"), second.get("metrics"));
        first.put("plannerWallNanos", 1); assertEquals(first.get("evidenceHash"), SchedulerResearchMain.evidenceHash(first));
    }
    @Test void aggregateForgeryIsRejectedAfterChecksumUpdate() {
        var c = SchedulerComparisonExperiment.condition("heterogeneous", "quick");
        var row = row(c, "fifo", "stop-and-copy");
        mutable(row.get("metrics")).put("sent_bytes", 1); resign(row);
        assertThrows(IllegalStateException.class, () -> SchedulerResearchMain.validate(row, c, "fifo", "stop-and-copy", 1));
    }
    @Test void changedPlanBudgetAndMissingJobAreRejected() {
        var c = SchedulerComparisonExperiment.condition("conflict", "quick");
        var row = row(c, "iter-gwin", "pre-copy");
        first(first(row.get("budgets")).get("grants")).put("allocatedBytes", 999999); resign(row);
        assertThrows(IllegalStateException.class, () -> SchedulerResearchMain.validate(row, c, "iter-gwin", "pre-copy", 1));
        var incomplete = row(c, "iter-gwin", "pre-copy");
        incomplete.put("jobs", ((List<?>) incomplete.get("jobs")).subList(0, 11)); resign(incomplete);
        assertThrows(IllegalStateException.class, () -> SchedulerResearchMain.validate(incomplete, c, "iter-gwin", "pre-copy", 1));
        var altered = row(c, "iter-gwin", "pre-copy");
        first(altered.get("plan")).put("destination", 999); altered.put("planHash", MovementExperiment.hash(altered.get("plan"))); resign(altered);
        assertThrows(IllegalStateException.class, () -> SchedulerResearchMain.validate(altered, c, "iter-gwin", "pre-copy", 1));
    }
    @Test void camigUsesSameCandidateInputAndValidatesActualSelectedPlan() {
        var c = SchedulerComparisonExperiment.condition("candidate-plan", "quick");
        var row = row(c, "camig-selector", "post-copy");
        assertDoesNotThrow(() -> SchedulerResearchMain.validate(row, c, "camig-selector", "post-copy", 1));
        assertEquals("candidate-plan", row.get("stage"));
        assertEquals("mongodb", row.get("executionMethod"));
    }
    @Test void missingAndDuplicateOriginalsNeverCreateCompletionReport() throws IOException {
        var originals = Files.createDirectory(folder.resolve("results"));
        var c = SchedulerComparisonExperiment.condition("heterogeneous", "quick");
        var row = row(c, "fifo", "stop-and-copy"); row.put("executionId", "test");
        for (String key : List.of("suiteExecutionId", "sourceHash", "backendHash", "runnerHash", "runNumber")) row.put(key, "test");
        String json = BaselineJson.encode(row);
        Files.writeString(originals.resolve("one.json"), json, StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> SchedulerResearchMain.regenerate(folder, "quick"));
        Files.writeString(originals.resolve("duplicate.json"), json, StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> SchedulerResearchMain.regenerate(folder, "quick"));
        assertFalse(Files.exists(folder.resolve("summary.csv")));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
