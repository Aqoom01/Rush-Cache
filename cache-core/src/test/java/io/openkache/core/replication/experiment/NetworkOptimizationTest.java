package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NetworkOptimizationTest {
    @TempDir Path folder;
    @Test void formerlyCensoredLargeLogCasesFinishWithinSameFiveSecondLimit() {
        for(int timeout:new int[]{100,2000}) for(int[] sizes:new int[][]{{16,8192,1},{64,1024,1},{64,8192,1},{64,8192,10}}) {
            var m=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("VR_INCREMENTAL","CRASH",sizes[0],sizes[1],sizes[2],timeout));
            NetworkCostResearchMain.validate(m);
            assertEquals("OK",m.outcome(),m.condition().id()); assertTrue(m.recoveryNs()<ProtocolCluster.ms(5000));
            assertEquals(sizes[0]*2,m.preservedCopies());
        }
    }
    @Test void optimizedQuickOriginalsRebuildIdenticallyAndCannotMixModes() throws Exception {
        Files.writeString(folder.resolve("launch.properties"),"suiteExecutionId=test\n");
        NetworkCostResearchMain.main(new String[]{folder.toString(),"quick","optimized"});
        String csv=Files.readString(folder.resolve("summary.csv")); NetworkCostResearchMain.regenerate(folder);
        assertEquals(csv,Files.readString(folder.resolve("summary.csv")));
        Path original=folder.resolve("results/VR_INCREMENTAL-NORMAL-h1-v64-b1-t100.json");
        String json=Files.readString(original); Files.delete(original);
        Files.writeString(folder.resolve("results/VR-NORMAL-h1-v64-b1-t100.json"),json.replace("VR_INCREMENTAL","VR"));
        assertThrows(IllegalStateException.class,()->NetworkCostResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
