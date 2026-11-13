package io.openkache.server.integration;

import io.openkache.core.migration.strategy.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Find implemented policy/scheme classes in the shared source tree and compare actual factories. */
class MethodInventoryTest {
    private Set<String> implementations(String folder,String contract)throws Exception{
        Path root=Path.of("../cache-core/src/main/java/io/openkache/core").toAbsolutePath().normalize().resolve(folder);
        var found=new TreeSet<String>();var pattern=Pattern.compile("public\\s+(?:final\\s+)?class\\s+(\\w+)\\s+implements\\s+"+contract+"\\b");
        try(var files=Files.walk(root)){for(Path file:files.filter(p->p.toString().endsWith(".java")).toList()){
            var match=pattern.matcher(Files.readString(file));if(match.find())found.add(match.group(1));
        }}return found;
    }
    @Test void everyImplementedB1B2AndScalingPolicyHasAnExecutableFactory()throws Exception{
        var p=IntegratedCacheTest.properties("O00");var distributions=new TreeSet<String>();
        for(String name:IntegrationConfig.DISTRIBUTIONS){p.setProperty("distribution",name);distributions.add(new IntegrationConfig(p).scheme().getClass().getSimpleName());}
        assertEquals(implementations("tablet","DistributionScheme"),distributions);
        p.setProperty("distribution","RANGE");var splits=new TreeSet<String>();
        for(String name:List.of("MIDPOINT","HEAT","ESTORE")){p.setProperty("split",name);splits.add(new IntegrationConfig(p).splitter().getClass().getSimpleName());}
        assertEquals(implementations("balance/split","SplitPolicy"),splits);p.setProperty("split","NONE");var placements=new TreeSet<String>();
        for(String name:IntegrationConfig.PLACEMENTS)if(!Set.of("NONE","FIXED_LPT").contains(name)){p.setProperty("placement",name);placements.add(new IntegrationConfig(p).placer().getClass().getSimpleName());}
        assertEquals(implementations("balance/placement","PlacementPolicy"),placements);
        p.setProperty("placement","NONE");ScalingConfig config=new IntegrationConfig(p).scalingSettings(16);var model=PerformanceModel.calibrate(config);var scalers=new TreeSet<String>();
        for(String name:ScalingPolicies.METHODS)scalers.add(ScalingPolicies.create(name,model,java.util.stream.IntStream.range(0,512).mapToObj(i->100.0+i%48).toList()).getClass().getSimpleName());
        assertEquals(implementations("migration/strategy","NodeCountPolicy"),scalers);
    }
    @Test void allImplementedA2ReadAndMetaParametersAreUsableAndNotDiscarded(){
        var p=JointCacheTest.properties("VR","V06");p.setProperty("read.selection","round-robin");p.setProperty("read.explore.every","3");p.setProperty("read.logical.limit","19");
        var config=new IntegrationConfig(p);var selection=IoniaProfile.selection(config.layer(false).values());assertEquals("ROUND_ROBIN",selection.mode().name());assertEquals(3,selection.exploreEvery());assertEquals(19,selection.logicalLimit());
        p=JointCacheTest.properties("VR","V07");p.setProperty("meta.batch.bytes","2048");p.setProperty("meta.pending.limit","64");p.setProperty("meta.inflight.limit","2");
        var meta=IoniaProfile.meta(new IntegrationConfig(p).values());assertEquals(2048,meta.maxBytes());assertEquals(64,meta.pendingLimit());assertEquals(2,meta.inFlightLimit());
        p=JointCacheTest.properties("VR","V05");p.setProperty("storage.shards","7");p.setProperty("storage.io.workers","3");var worker=IoniaProfile.worker(new IntegrationConfig(p).values());assertEquals(7,worker.storageShards());assertEquals(3,worker.ioWorkers());
    }
}
