package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class PreCopyRoundTraceTest {
    private static final ComparisonExperiment.Case CASE =
            new ComparisonExperiment.Case("trace", "S3", 100, 256, .5, 30, 0, 0, 10);

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> copy(ComparisonExperiment.Run run) {
        return (List<Map<String, Object>>) BaselineJson.decode(BaselineJson.encode(run.dirtyRounds()));
    }

    private static long metric(ComparisonExperiment.Run run, String name) {
        return ((Number) BaselineJson.object(run.metrics().get(name)).get("value")).longValue();
    }

    private static void validate(ComparisonExperiment.Run run, Object rows) {
        PreCopyRoundTrace.validate(rows, true, CASE.threshold(), CASE.maxRounds(), metric(run, "live_delta_rounds"),
                run.stopReason(), metric(run, "sent_bytes"), 0, metric(run, "commit_tick"));
    }

    @Test void missingMetadataDecisionSequenceAndByteForgeriesAreRejected() {
        var run = ComparisonExperiment.run(CASE, 1, "pre-copy");
        assertDoesNotThrow(() -> validate(run, copy(run)));
        List<Consumer<List<Map<String, Object>>>> changes = List.of(
                rows -> rows.getFirst().remove("previousDirtyBytes"),
                rows -> rows.removeLast(),
                rows -> rows.getFirst().put("stopReason", "THRESHOLD"),
                rows -> rows.get(1).put("lastSequence", 99999),
                rows -> rows.getLast().put("roundSentBytes", 0),
                rows -> rows.getFirst().put("dirtyKeys", -1),
                rows -> rows.getFirst().put("tick", metric(run, "commit_tick") + 1));
        for (var change : changes) {
            var rows = copy(run); change.accept(rows);
            assertThrows(IllegalStateException.class, () -> validate(run, rows));
        }
    }

    @Test void otherCopyVariantsHaveNoFabricatedDirtyObservations() {
        for (String variant : List.of("stop-and-copy", "post-copy")) {
            var run = ComparisonExperiment.run(CASE, 1, variant);
            assertTrue(run.dirtyRounds().isEmpty());
            assertDoesNotThrow(() -> PreCopyRoundTrace.validate(run.dirtyRounds(), false, 0, 10, 0,
                    null, metric(run, "sent_bytes"), 0, metric(run, "commit_tick")));
        }
    }
}
