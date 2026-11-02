package io.openkache.core.replication.experiment;

import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SustainedLoadResearchMainTest {
    @TempDir Path folder;
    @Test void fullDurationOriginalRoundTripsAndCountersValidate() {
        var m=SustainedLoadExperiment.run(new SustainedLoadExperiment.Condition("PB","NORMAL",20,64,1));
        SustainedLoadResearchMain.validate(m);
        assertEquals(m,SustainedLoadResearchMain.decode(SustainedLoadResearchMain.encode(m)));
        String json=SustainedLoadResearchMain.encode(m);
        assertThrows(IllegalStateException.class,()->SustainedLoadResearchMain.decode(json.replace("a1-sustained-json-1","unknown-schema")));
        assertThrows(IllegalStateException.class,()->SustainedLoadResearchMain.decode(json.replace("\"loadMs\": 30000","\"loadMs\": 30000, \"loadMs\": 30000")));
        String broken=json.replaceFirst("\\[0,500,", "[0,499,");
        assertNotEquals(json,broken);
        assertThrows(IllegalStateException.class,()->SustainedLoadResearchMain.validate(SustainedLoadResearchMain.decode(broken)));
    }
    @Test void incompleteOriginalSetRemovesOldCompletionMarker() throws Exception {
        Files.createDirectory(folder.resolve("results")); Files.writeString(folder.resolve("completion.properties"),"status=complete\n");
        assertThrows(IllegalStateException.class,()->SustainedLoadResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
        assertEquals(16,SustainedLoadExperiment.conditions(true).size()); assertEquals(48,SustainedLoadExperiment.conditions(false).size());
    }
}
