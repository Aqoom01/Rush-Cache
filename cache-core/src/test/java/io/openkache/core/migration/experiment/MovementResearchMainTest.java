package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MovementResearchMainTest {
    @TempDir Path folder;

    private static MovementExperiment.Case condition(String variant) {
        return new MovementExperiment.Case("test", "M5", 3, 3, 3, "shared-source", 32, 2048, 1024, 1024, 1024, 256, .5, variant);
    }
    private static Map<String, Object> decoded(MovementExperiment.Case c) {
        return BaselineJson.object(BaselineJson.decode(BaselineJson.encode(MovementResearchMain.result(c, 1, MovementExperiment.run(c, 1)))));
    }
    private static void resign(Map<String, Object> row) {
        var evidence = new LinkedHashMap<String, Object>();
        for (String key : List.of("condition", "changes", "seed", "metrics", "jobs", "budgets", "phaseEvents")) evidence.put(key, row.get(key));
        row.put("evidenceHash", MovementExperiment.hash(evidence));
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mutable(Object value) {
        BaselineJson.object(value); // 문자열 key인 JSON map인지 먼저 검증한다.
        return (Map<String, Object>) value;
    }
    private static Map<String, Object> first(Object value) { return mutable(((List<?>) value).getFirst()); }

    @Test void serializedEvidenceValidatesForAllVariants() {
        for (String variant : MovementExperiment.VARIANTS) {
            var c = condition(variant);
            assertDoesNotThrow(() -> MovementResearchMain.validate(decoded(c), c, 1), variant);
        }
    }

    @Test void derivedMetricsAndBudgetViolationsAreRejectedEvenWithUpdatedChecksum() {
        var c = condition("pre-copy"); var row = decoded(c);
        mutable(row.get("metrics")).put("sent_bytes", Map.of("value", 1, "unit", "bytes"));
        resign(row);
        assertThrows(IllegalStateException.class, () -> MovementResearchMain.validate(row, c, 1));
        var budgetRow = decoded(c);
        var first = first(budgetRow.get("budgets"));
        first(first.get("grants")).put("allocatedBytes", 999999);
        resign(budgetRow);
        assertThrows(IllegalStateException.class, () -> MovementResearchMain.validate(budgetRow, c, 1));
    }

    @Test void missingJobsAndIncorrectOwnershipPlanAreRejected() {
        var c = condition("post-copy"); var row = decoded(c);
        row.put("jobs", MovementResearchMain.objects(row.get("jobs")).subList(0, 2)); resign(row);
        assertThrows(IllegalStateException.class, () -> MovementResearchMain.validate(row, c, 1));
        var changed = decoded(c);
        first(changed.get("jobs")).put("source", 999); resign(changed);
        assertThrows(IllegalStateException.class, () -> MovementResearchMain.validate(changed, c, 1));
    }

    @Test void missingOrDuplicateOriginalsNeverProduceACompleteReport() throws IOException {
        var originals = Files.createDirectory(folder.resolve("results"));
        var c = MovementExperiment.cases("quick").getFirst();
        var row = MovementResearchMain.result(c, 1, MovementExperiment.run(c, 1));
        row.put("executionId", "one");
        for (String key : List.of("sourceHash", "backendHash", "runnerHash", "suiteExecutionId", "runNumber")) row.put(key, "test");
        String json = BaselineJson.encode(row);
        Files.writeString(originals.resolve("one.json"), json, StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> MovementResearchMain.regenerate(folder, "quick"));
        assertFalse(Files.exists(folder.resolve("summary.csv")));
        Files.writeString(originals.resolve("duplicate.json"), json, StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> MovementResearchMain.regenerate(folder, "quick"));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
