package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class CraqComparisonTest {
    @TempDir Path folder;
    @Test void allConditionsValidateAndReplayIdentically() {
        assertEquals(32,CraqComparison.conditions(false).size());
        for(var c:CraqComparison.conditions(false)) {
            var m=CraqComparison.run(c); assertEquals(m,CraqComparison.run(c));
            assertEquals(m,CraqResearchMain.decode(CraqResearchMain.encode(m)));
        }
    }
    @Test void schemaAndSemanticTamperingAreRejected() {
        String json=CraqResearchMain.encode(CraqComparison.run(CraqComparison.conditions(true).getFirst()));
        assertThrows(IllegalStateException.class,()->CraqResearchMain.decode(json.replace("a1-craq-json-1","wrong")));
        assertThrows(IllegalStateException.class,()->CraqResearchMain.decode(json.replace("\"correct\": 90","\"correct\": 89")));
        assertThrows(IllegalStateException.class,()->CraqResearchMain.decode(json.replace("\"reads\": 90","\"reads\": 90,\n  \"extra\": 0")));
    }
    @Test void rebuildRequiresFullCoverageAndReproducesSummary() throws Exception {
        Path originals=Files.createDirectory(folder.resolve("results"));
        for(var c:CraqComparison.conditions(true)) Files.writeString(originals.resolve(c.id()+".json"),CraqResearchMain.encode(CraqComparison.run(c)));
        CraqResearchMain.regenerate(folder); String csv=Files.readString(folder.resolve("summary.csv"));
        assertEquals(9,csv.lines().count()); CraqResearchMain.regenerate(folder); assertEquals(csv,Files.readString(folder.resolve("summary.csv")));
        Files.delete(originals.resolve(CraqComparison.conditions(true).getFirst().id()+".json"));
        assertThrows(IllegalStateException.class,()->CraqResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
