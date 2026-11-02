package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ChainComparisonTest {
    @Test void sharedChainMatrixSeparatesPartitionAvailabilityFromPreservation() {
        for(var condition:ChainResearchMain.conditions(true)) if(condition.variant().equals("CHAIN")) {
            var m=ElectionComparison.run(condition); var row=ElectionResearchMain.row(m);
            ElectionResearchMain.validate(row); assertEquals(row,ElectionResearchMain.decode(ElectionResearchMain.encode(row)));
            int expected=switch(condition.scenario()) { case PARTITION -> 0; case FOLLOWER_REJOIN -> 12; default -> 16; };
            assertEquals(expected,m.postOk(),condition.toString()); assertEquals(1,m.converged(),condition.toString());
        }
    }
    @Test void positionMatrixReplaysAndOriginalsRoundTrip() {
        for(var c:ChainPositionExperiment.conditions()) {
            var m=ChainPositionExperiment.run(c); assertEquals(m,ChainPositionExperiment.run(c));
            assertEquals(m,ChainPositionExperiment.decode(ChainPositionExperiment.encode(m)));
        }
        String json=ChainPositionExperiment.encode(ChainPositionExperiment.run(new ChainPositionExperiment.Condition(1,1,64,1)));
        assertThrows(IllegalStateException.class,()->ChainPositionExperiment.decode(json.replace("\"preKvCopies\": 32","\"preKvCopies\": 31")));
        assertThrows(IllegalStateException.class,()->ChainPositionExperiment.decode(json.replace("a1-chain-position-json-1","unknown")));
    }
    @Test void positionRebuildAndIncompleteRun(@TempDir Path folder) throws Exception {
        ChainPositionExperiment.writeOriginals(folder); ChainPositionExperiment.regenerate(folder);
        String before=Files.readString(folder.resolve("position-summary.csv")); ChainPositionExperiment.regenerate(folder);
        assertEquals(before,Files.readString(folder.resolve("position-summary.csv")));
        Files.delete(folder.resolve("position-results/node1-pending1-v64-b1.json"));
        Files.writeString(folder.resolve("completion.properties"),"status=complete");
        assertThrows(IllegalStateException.class,()->ChainResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
