package io.openkache.server.benchmark.pilot;

import io.openkache.runner.benchmark.TracePhase;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.runner.benchmark.measurement.*;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedReportsTest {
    static DistributedReports.Phase phase(int slot,int count,long latency){
        List<DistributedReports.Series> series=new ArrayList<>();for(var kind:LatencyCollector.Kind.values()){var h=new LatencyHistogram(DistributedReports.RANGE);for(int i=0;i<count;i++)h.record(latency);series.add(new DistributedReports.Series(ClientOutcome.Operation.GET,kind,h.snapshot()));}
        return new DistributedReports.Phase(slot,TracePhase.MEASURE,new DistributedPlan.Partition(count,count,0,0,"a".repeat(64)),count,0,100,series);
    }
    @Test void frameRoundTripPreservesAllBinsAndCounts()throws Exception{
        var p=phase(0,100,500);assertEquals(p,DistributedReports.read(DistributedReports.packet(p)));
    }
    @Test void mergeUsesBucketCountsNotMeanOfPercentiles()throws Exception{
        var a=phase(0,100,1);var b=phase(1,1,1000000);var merged=DistributedReports.merge(a,b);var h=merged.getFirst().histogram();assertEquals(101,h.count());assertEquals(1L,h.quantile(99,100).upperBoundNanos());assertEquals(1000100L,h.sumNanos().longValueExact());assertFalse(h.quantile(999,1000).comparisonEligible());
    }
    @Test void duplicateSlotsAndChangedPartitionAreRejected(){
        var p=phase(0,2,10);assertThrows(IOException.class,()->DistributedReports.merge(p,p));assertThrows(IOException.class,()->DistributedReports.validate(p,new DistributedPlan.Partition(3,3,0,0,"a".repeat(64))));
    }
    @Test void missingHistogramPopulationIsRejected(){
        var p=phase(0,2,10);var bad=new DistributedReports.Phase(p.slot(),p.phase(),p.partition(),p.attempts(),p.retried(),p.durationNanos(),p.series().subList(0,2));assertThrows(IOException.class,()->DistributedReports.validate(bad,p.partition()));
    }
    @Test void alteredCountVersionAndDuplicateBinsAreRejected(){
        for(String field:List.of("h.0.count","h.0.version","h.0.buckets")){
            var packet=DistributedReports.packet(phase(0,2,10));packet.setProperty(field,field.endsWith("count")?"3":field.endsWith("version")?"bad":"10:1,10:1");assertThrows(IOException.class,()->DistributedReports.read(packet));
        }
    }
    @Test void impossibleAttemptsAndNoncanonicalBucketAreRejected(){
        var p=DistributedReports.packet(phase(0,2,10000));p.setProperty("attempts","1");assertThrows(IOException.class,()->DistributedReports.read(p));var bad=DistributedReports.packet(phase(0,2,10000));bad.setProperty("h.0.buckets","123456789:2");assertThrows(IOException.class,()->DistributedReports.read(bad));
    }
}
