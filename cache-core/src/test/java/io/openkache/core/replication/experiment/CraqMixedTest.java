package io.openkache.core.replication.experiment;

import io.openkache.core.model.Bytes;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class CraqMixedTest {
    @TempDir Path folder;
    @Test void quickConditionsReplayAndRoundTrip() {
        assertEquals(288,CraqMixedExperiment.conditions(false).size()); assertEquals(24,CraqMixedExperiment.conditions(true).size());
        for(var c:CraqMixedExperiment.conditions(true)) {
            var row=CraqMixedExperiment.run(c); assertEquals(row,CraqMixedExperiment.run(c));
            assertEquals(row,CraqMixedResearchMain.decode(CraqMixedResearchMain.encode(row)));
        }
    }
    @Test void intervalCheckerRejectsUncommittedAndObsoleteValues() {
        var trace=new CraqMixedExperiment.Trace(); Bytes key=Bytes.utf8("k"),old=Bytes.utf8("old"),next=Bytes.utf8("next");
        trace.histories.put(key,java.util.List.of(new CraqMixedExperiment.Commit(1,old),new CraqMixedExperiment.Commit(4,next)));
        assertTrue(trace.valid(key,old,2,5)); assertTrue(trace.valid(key,next,2,5));
        assertFalse(trace.valid(key,next,2,3)); assertFalse(trace.valid(key,old,5,6));
        assertFalse(trace.valid(key,Bytes.utf8("never-applied"),2,5));
    }
    @Test void rebuildAndTamperingChecks() throws Exception {
        Path originals=Files.createDirectory(folder.resolve("results"));
        for(var c:CraqMixedExperiment.conditions(true)) Files.writeString(originals.resolve(c.id()+".json"),CraqMixedResearchMain.encode(CraqMixedExperiment.run(c)));
        CraqMixedResearchMain.regenerate(folder); String csv=Files.readString(folder.resolve("summary.csv"));
        CraqMixedResearchMain.regenerate(folder); assertEquals(csv,Files.readString(folder.resolve("summary.csv")));
        Path first=originals.resolve(CraqMixedExperiment.conditions(true).getFirst().id()+".json"); String json=Files.readString(first);
        assertThrows(IllegalStateException.class,()->CraqMixedResearchMain.decode(json.replace("a1-craq-mixed-json-1","wrong")));
        Files.writeString(first,json.replace("\"invalidReads\": 0","\"invalidReads\": 1"));
        assertThrows(IllegalStateException.class,()->CraqMixedResearchMain.regenerate(folder));
        assertFalse(Files.exists(folder.resolve("completion.properties")));
        Files.delete(first); assertThrows(IllegalStateException.class,()->CraqMixedResearchMain.regenerate(folder));
    }
}
