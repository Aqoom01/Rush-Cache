package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExecutionSafetyExperimentTest {
    private static Map<String, Object> row(ExecutionSafetyExperiment.Case c) {
        var raw = ExecutionSafetyExperiment.run(c);
        raw.put("resultSchemaVersion", ExecutionSafetyResearchMain.SCHEMA);
        return BaselineJson.object(BaselineJson.decode(BaselineJson.encode(raw)));
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mutable(Object value) { return (Map<String, Object>) value; }

    @Test void everyQuickCaseExecutesAllThreeProtocolsAndValidatesEvidence() {
        assertEquals(162, ExecutionSafetyExperiment.cases("quick").size());
        assertEquals(435, ExecutionSafetyExperiment.cases("full").size());
        for (var c : ExecutionSafetyExperiment.cases("quick"))
            assertDoesNotThrow(() -> ExecutionSafetyResearchMain.validate(row(c), c), c.toString());
    }

    @Test void unrecoveredSourceDoesNotRollbackPublishedOwnershipOrRemoveNode() {
        for (String variant : MovementExperiment.VARIANTS) {
            var c = new ExecutionSafetyExperiment.Case("pd-operator", "unrecovered-source", variant, 48, 1);
            var r = row(c); var metrics = BaselineJson.object(r.get("metrics"));
            assertEquals("HELD", r.get("outcome"));
            assertEquals(variant.equals("post-copy") ? 4L : 3L, MovementResearchMain.integer(metrics.get("ownership_commits")));
            assertEquals(0L, MovementResearchMain.integer(metrics.get("node_removed")));
            ExecutionSafetyResearchMain.validate(r, c);
        }
    }

    @Test void seededFaultsHaveIdenticalTimelinesAndOutcomesOnReplay() {
        for (String variant : MovementExperiment.VARIANTS) for (int seed = 1; seed <= 4; seed++) {
            var c = new ExecutionSafetyExperiment.Case("foundationdb-fault", "seeded-fault", variant, 48, seed);
            var a = row(c); var b = row(c);
            assertEquals(a.get("evidenceHash"), b.get("evidenceHash"));
            assertEquals(a.get("events"), b.get("events"));
        }
    }

    @Test void aNodeWithRemainingTabletsCannotBeForgedAsRemovedEvenAfterResigning() {
        var c = new ExecutionSafetyExperiment.Case("mongodb-drain", "jumbo", "pre-copy", 48, 1);
        var r = row(c); assertEquals("BLOCKED", r.get("outcome"));
        r.put("outcome", "REMOVED"); mutable(r.get("metrics")).put("node_removed", 1);
        r.put("evidenceHash", MovementExperiment.hash(ExecutionSafetyResearchMain.evidence(r)));
        assertThrows(IllegalStateException.class, () -> ExecutionSafetyResearchMain.validate(r, c));
    }

    @Test void preflightRejectionAndPartialDrainAreDifferentOutcomes() {
        for (String method : List.of("scylla-drain", "mongodb-drain")) {
            var c = new ExecutionSafetyExperiment.Case(method, "no-capacity", "stop-and-copy", 48, 2);
            var r = row(c);
            assertEquals(method.equals("scylla-drain") ? "REJECTED" : "BLOCKED", r.get("outcome"));
            ExecutionSafetyResearchMain.validate(r, c);
        }
    }
}
