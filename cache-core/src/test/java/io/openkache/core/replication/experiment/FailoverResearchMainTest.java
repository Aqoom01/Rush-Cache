package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class FailoverResearchMainTest {
    @TempDir Path folder;
    @Test void originalsRebuildAndMissingCaseInvalidatesCompletion() throws Exception {
        Files.writeString(folder.resolve("launch.properties"), "suiteExecutionId=test\n");
        FailoverResearchMain.main(new String[] {folder.toString()});
        String csv = Files.readString(folder.resolve("summary.csv"));
        FailoverResearchMain.regenerate(folder);
        assertEquals(csv, Files.readString(folder.resolve("summary.csv")));
        Files.delete(folder.resolve("results/d1-t50-r1.json"));
        assertThrows(IllegalStateException.class, () -> FailoverResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
    @Test void duplicateSchemaAndInvalidMeasurementsAreRejected() throws Exception {
        var m = FailoverExperiment.run(1, 50, 1);
        String json = FailoverResearchMain.encode(m);
        assertEquals(m, FailoverResearchMain.decode(json));
        assertThrows(IllegalStateException.class, () -> FailoverResearchMain.decode(json.replace("a1-crash-json-1", "other")));
        assertThrows(IllegalStateException.class, () -> FailoverResearchMain.decode(json.replace("\"delay\": 1", "\"delay\": 1, \"delay\": 1")));
        Files.writeString(folder.resolve("launch.properties"), "suiteExecutionId=test\n");
        FailoverResearchMain.main(new String[] {folder.toString()});
        Files.writeString(folder.resolve("results/d1-t50-r1.json"), json.replace("\"preserved\": 10", "\"preserved\": 9"));
        assertThrows(IllegalStateException.class, () -> FailoverResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
