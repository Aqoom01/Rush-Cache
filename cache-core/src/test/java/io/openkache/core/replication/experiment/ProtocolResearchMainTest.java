package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolResearchMainTest {
    @TempDir Path folder;
    private void run() throws Exception {
        Files.writeString(folder.resolve("launch.properties"),"suiteExecutionId=test\n");
        ProtocolResearchMain.main(new String[]{folder.toString(),"quick"});
    }
    @Test void originalsRebuildAndMissingCaseInvalidatesCompletion() throws Exception {
        run(); String csv=Files.readString(folder.resolve("summary.csv"));
        ProtocolResearchMain.regenerate(folder); assertEquals(csv,Files.readString(folder.resolve("summary.csv")));
        Files.delete(folder.resolve("results/VR-CRASH-d1-s1.json"));
        assertThrows(IllegalStateException.class,()->ProtocolResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
    @Test void originalMetricAndSchemaTamperingIsRejected() throws Exception {
        run(); Path original=folder.resolve("results/VR-PARTIAL_CRASH-d1-s1.json"); String json=Files.readString(original);
        assertThrows(IllegalStateException.class,()->ProtocolResearchMain.decode(json.replace("a1-protocol-json-1","bad-schema")));
        assertThrows(IllegalStateException.class,()->ProtocolResearchMain.decode(json.replace("\"delay\": 1","\"delay\": 1, \"delay\": 1")));
        Files.writeString(original,json.replace("\"divergent\": 0","\"divergent\": 1"));
        assertThrows(IllegalStateException.class,()->ProtocolResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
    @Test void normalAndFullMatrixOutcomesPassIndependentMetricChecks() {
        for(String variant:new String[]{"PB","VR"}) for(var scenario:ProtocolComparison.Scenario.values())
            for(int d:new int[]{1,5}) for(int seed=1;seed<=5;seed++) {
                var m=ProtocolComparison.run(variant,scenario,d,seed);
                ProtocolResearchMain.validate(m); assertEquals(m,ProtocolResearchMain.decode(ProtocolResearchMain.encode(m)));
            }
    }
}
