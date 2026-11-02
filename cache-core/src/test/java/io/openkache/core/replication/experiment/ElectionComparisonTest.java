package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ElectionComparisonTest {
    @Test void quickMatrixPreservesAcknowledgedWritesAndOriginalsRoundTrip() {
        for(var condition:ElectionComparison.conditions(true)) {
            var row=ElectionResearchMain.row(ElectionComparison.run(condition));
            ElectionResearchMain.validate(row);
            assertEquals(row,ElectionResearchMain.decode(ElectionResearchMain.encode(row)));
            if(condition.variant().equals("RAFT")) assertEquals(16L,row.values().get("postOk"),condition.toString());
        }
    }
    @Test void replayAndTamperedOriginals() {
        var c=new ElectionComparison.Condition("RAFT",ElectionComparison.Scenario.PARTITION,2,8192,1);
        var a=ElectionComparison.run(c); assertEquals(a,ElectionComparison.run(c));
        String json=ElectionResearchMain.encode(ElectionResearchMain.row(a));
        assertThrows(IllegalStateException.class,()->ElectionResearchMain.decode(json.replace("a1-election-json-1","unknown")));
        assertThrows(IllegalStateException.class,()->ElectionResearchMain.decode(json.replace("\"retainedPre\": 64","\"retainedPre\": 63")));
        assertThrows(IllegalStateException.class,()->ElectionResearchMain.decode(json.replace("\"minorityOk\": 0","\"minorityOk\": 1")));
    }
    @Test void incompleteRunCannotRetainCompletion(@TempDir Path folder) throws Exception {
        Files.createDirectory(folder.resolve("results")); Files.writeString(folder.resolve("completion.properties"),"status=complete");
        assertThrows(IllegalStateException.class,()->ElectionResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
