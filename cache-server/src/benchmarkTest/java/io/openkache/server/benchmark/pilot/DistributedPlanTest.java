package io.openkache.server.benchmark.pilot;

import io.openkache.runner.benchmark.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class DistributedPlanTest {
    @TempDir Path root;
    static PilotFiles.Plan plan(){return new PilotFiles.Plan(new TraceConfig(101,16,24,100,80,15,5,2,TraceConfig.Distribution.UNIFORM,0),7,49,2,120000,30000,5000,1500);}
    @Test void partitionsCoverEveryOriginalRequestOnceIncludingOddCounts()throws Exception{
        var p=plan();var g=new TraceGenerator(p.trace());for(var phase:TracePhase.values()){
            long total=DistributedPlan.count(p,phase);Set<Long> seen=new HashSet<>();long count=0;
            for(int slot=0;slot<2;slot++){var expected=DistributedPlan.expected(p,phase,slot);assertEquals(DistributedPlan.partitionCount(total,slot),expected.requests());count+=expected.requests();
                var iterator=g.stream(phase,total,slot);while(iterator.hasNext()){var r=iterator.next();assertTrue(seen.add(r.globalIndex()));assertEquals(g.at(phase,r.globalIndex()),r);assertEquals(slot,r.streamId());}}
            assertEquals(total,count);assertEquals(total,seen.size());
        }
    }
    @Test void slotAndWireIdentitiesAreDisjointAndInvalidSlotsRejected(){
        Set<Long> wire=new HashSet<>();for(int slot=0;slot<2;slot++)for(int client=0;client<6;client++)assertTrue(wire.add(DistributedPlan.wireBase(slot)+client));
        assertThrows(IllegalArgumentException.class,()->DistributedPlan.slot(-1));assertThrows(IllegalArgumentException.class,()->DistributedPlan.slot(2));
    }
    @Test void partitionDigestsAreRepeatableAndSlotPhaseSensitive()throws Exception{
        var p=plan();assertEquals(DistributedPlan.expected(p,TracePhase.MEASURE,0),DistributedPlan.expected(p,TracePhase.MEASURE,0));
        assertNotEquals(DistributedPlan.expected(p,TracePhase.MEASURE,0).sha256(),DistributedPlan.expected(p,TracePhase.MEASURE,1).sha256());assertNotEquals(DistributedPlan.expected(p,TracePhase.MEASURE,0).sha256(),DistributedPlan.expected(p,TracePhase.WARMUP,0).sha256());
    }
    @Test void distributedProfileRejectsAccidentalConcurrencyOrStreamChanges(){
        var p=plan();DistributedPlan.requireProfile(p);assertThrows(IllegalArgumentException.class,()->DistributedPlan.requireProfile(new PilotFiles.Plan(p.trace(),p.warm(),p.measured(),1,p.workMs(),p.barrierMs(),p.operationMs(),p.socketMs())));
        assertThrows(IllegalArgumentException.class,()->DistributedPlan.endpoint("127.0.0.1:0"));
    }
    @Test void legacyRunRefusesDistributedInputsBeforeOpeningAnyTransport()throws Exception{
        Files.writeString(root.resolve("distributed.properties"),"schema=test");var e=assertThrows(java.io.IOException.class,()->PilotMain.run(root.resolve("no-bundle"),root));assertTrue(e.getMessage().contains("DistributedPilotMain"));assertFalse(Files.exists(root.resolve("CLIENT_STARTED")));
    }
}
