package io.openkache.core.replication.experiment;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicationExperimentTest {
    @TempDir Path folder;

    @Test void slowBackupDeterminesSuccessLatency() {
        var m = ReplicationExperiment.run(new ReplicationExperiment.Condition(5, 20, 200), 3);
        assertEquals(3, m.ok());
        assertEquals(30, m.successMs());
        assertEquals(9, m.verifiedCopies());
    }

    @Test void deadlineEqualityTimesOutAndLeavesPartialWrites() {
        var m = ReplicationExperiment.run(new ReplicationExperiment.Condition(5, 40, 50), 2);
        assertEquals(2, m.timeouts());
        assertEquals(50, m.responseMs());
        assertEquals(-1, m.successMs());
        assertEquals(2, m.timeoutPartialWrites());
    }

    @Test void fixedInputsReplayExactly() {
        for (var c : ReplicationExperiment.conditions())
            assertEquals(ReplicationExperiment.run(c, 2), ReplicationExperiment.run(c, 2));
    }

    @Test void equalTimeEventsKeepInsertionOrder() {
        var s = new ReplicationExperiment.Scheduler();
        var seen = new StringBuilder();
        s.schedule(2, () -> seen.append('b'));
        s.schedule(1, () -> seen.append('a'));
        s.schedule(2, () -> seen.append('c'));
        s.drain();
        assertEquals("abc", seen.toString());
    }

    private void generate() throws Exception {
        Files.writeString(folder.resolve("launch.properties"), "test=true\n");
        ReplicationResearchMain.main(new String[] {folder.toString(), "quick"});
    }

    @Test void regenerateIsIdenticalAndMissingResultInvalidatesCompletion() throws Exception {
        generate();
        String csv = Files.readString(folder.resolve("summary.csv"));
        String report = Files.readString(folder.resolve("results.md"));
        ReplicationResearchMain.regenerate(folder);
        assertEquals(csv, Files.readString(folder.resolve("summary.csv")));
        assertEquals(report, Files.readString(folder.resolve("results.md")));
        Files.delete(folder.resolve("results/d1-a0-t50.json"));
        assertThrows(IllegalStateException.class, () -> ReplicationResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }

    @Test void changedMetricIsRejected() throws Exception {
        generate();
        Path json = folder.resolve("results/d1-a0-t50.json");
        Files.writeString(json, Files.readString(json).replace("\"responseMs\": 2", "\"responseMs\": 3"));
        assertThrows(IllegalStateException.class, () -> ReplicationResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }

    @Test void duplicateOrChangedSchemaIsRejected() {
        String json = ReplicationResearchMain.encode(ReplicationExperiment.run(new ReplicationExperiment.Condition(1, 0, 50), 10));
        assertThrows(IllegalStateException.class, () -> ReplicationResearchMain.decode(json.replace("a1-replication-json-1", "unknown")));
        assertThrows(IllegalStateException.class, () -> ReplicationResearchMain.decode(json.replace("\"ok\": 10,", "\"ok\": 10,\n\"ok\": 10,")));
    }
}
