package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RoseExperimentTest {
    @Test void quickMatrixAuditsLossWindowsAndEveryOutcome() {
        for(var c:RoseExperiment.conditions(true)) {
            var m=RoseExperiment.run(c); RoseResearchMain.validate(m);
            assertEquals(m,RoseResearchMain.decode(RoseResearchMain.encode(m)));
            assertEquals(1L,m.values().get("converged"),c.toString());
        }
    }
    @Test void replayAndTamperedOriginalsAreRejected() {
        var c=new RoseExperiment.Condition(10,RoseExperiment.Scenario.ISOLATED_CRASH,1,8192,1);
        var m=RoseExperiment.run(c); assertEquals(m,RoseExperiment.run(c));
        String json=RoseResearchMain.encode(m);
        assertThrows(IllegalStateException.class,()->RoseResearchMain.decode(json.replace("a1-rose-json-1","unknown")));
        assertThrows(IllegalStateException.class,()->RoseResearchMain.decode(json.replace("\"lostPreCrash\": "+m.values().get("lostPreCrash"),"\"lostPreCrash\": 11")));
        assertThrows(IllegalStateException.class,()->RoseResearchMain.decode(json.replace("\"peakLag\": 10","\"peakLag\": 11")));
    }
    @Test void incompleteOriginalsCannotKeepCompletion(@TempDir Path folder) throws Exception {
        Files.createDirectory(folder.resolve("results")); Files.writeString(folder.resolve("completion.properties"),"status=complete");
        assertThrows(IllegalStateException.class,()->RoseResearchMain.regenerate(folder)); assertFalse(Files.exists(folder.resolve("completion.properties")));
    }
}
