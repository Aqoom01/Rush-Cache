package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NetworkCostResearchMainTest {
    @TempDir Path folder;
    private void run() throws Exception {
        Files.writeString(folder.resolve("launch.properties"),"suiteExecutionId=test\n");
        NetworkCostResearchMain.main(new String[]{folder.toString(),"quick"});
    }
    @Test void quickMatrixOriginalsRegenerateAndMissingCaseInvalidatesCompletion() throws Exception {
        run(); String csv=Files.readString(folder.resolve("summary.csv"));
        NetworkCostResearchMain.regenerate(folder); assertEquals(csv,Files.readString(folder.resolve("summary.csv")));
        Files.delete(folder.resolve("results/VR-CRASH-h64-v8192-b1-t100.json"));
        assertThrows(IllegalStateException.class,()->NetworkCostResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
    @Test void bytesTamperingAndUnknownSchemaAreRejected() throws Exception {
        run(); Path path=folder.resolve("results/PB-NORMAL-h1-v64-b1-t100.json"); String json=Files.readString(path);
        var m=NetworkCostResearchMain.decode(json); assertEquals(json,NetworkCostResearchMain.encode(m));
        assertThrows(IllegalStateException.class,()->NetworkCostResearchMain.decode(json.replace("a1-network-cost-json-1","other-schema")));
        assertThrows(IllegalStateException.class,()->NetworkCostResearchMain.decode(json.replace("\"history\": 1","\"history\": 1, \"history\": 1")));
        Files.writeString(path,json.replace("\"deliveredBytes\": "+m.network().deliveredBytes(),"\"deliveredBytes\": 0"));
        assertThrows(IllegalStateException.class,()->NetworkCostResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
    @Test void intermediateFullOnlyConditionAndCoverageAreValid() {
        assertEquals(64,NetworkCostExperiment.conditions(true).size());
        assertEquals(216,NetworkCostExperiment.conditions(false).size());
        var m=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("VR","CRASH",16,1024,10,100));
        NetworkCostResearchMain.validate(m); assertEquals(m,NetworkCostResearchMain.decode(NetworkCostResearchMain.encode(m)));
    }
}
