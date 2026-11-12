package io.openkache.core.replication.ionia_sixth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaConfig;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaWorkerSettingsTest {
    @Test void V04B_01_settingsRequireIndependentPositiveShardAndWorkerCounts(){
        Properties p=new Properties();p.setProperty("storage.workers","4");
        assertThrows(IllegalArgumentException.class,()->IoniaWorkerSettings.parse(p));
        p.setProperty(IoniaWorkerSettings.SHARDS,"4");
        for(String value:List.of("0","-1","257","2147483648","+1","1.5"," 1")){
            p.setProperty(IoniaWorkerSettings.WORKERS,value);assertThrows(IllegalArgumentException.class,()->IoniaWorkerSettings.parse(p));
        }
        p.setProperty(IoniaWorkerSettings.WORKERS,"2");assertEquals(new IoniaWorkerSettings(4,2),IoniaWorkerSettings.parse(p));
        assertThrows(IllegalArgumentException.class,()->new IoniaWorkerSettings(0,1));
        assertThrows(IllegalArgumentException.class,()->new IoniaWorkerSettings(257,1));
    }
    @Test void V04B_02_everyShardHasExactlyOneFixedWorkerEvenWithIdleExtraWorkers(){
        for(int shards:new int[]{1,4,7,256})for(int workers:new int[]{1,2,4,8,256}){
            var settings=new IoniaWorkerSettings(shards,workers);
            for(int i=0;i<shards;i++)assertEquals(i%workers,settings.workerForShard(i));
            assertThrows(IllegalArgumentException.class,()->settings.workerForShard(-1));
            assertThrows(IllegalArgumentException.class,()->settings.workerForShard(shards));
        }
    }
    @Test void V04B_01_wireProfilesDistinguishBothConfigurationDimensions(){
        var config=new IoniaConfig(new NodeId(1),new NodeId(1),List.of(new NodeId(1),new NodeId(2),new NodeId(3)),0);
        var settings=new IoniaWorkerSettings(4,2);assertEquals(64,settings.profileId().length());
        assertNotEquals(settings.profileId(),new IoniaWorkerSettings(4,1).profileId());
        assertNotEquals(settings.profileId(),new IoniaWorkerSettings(2,2).profileId());
        assertTrue(settings.configurationId(config).contains("|algorithm=V04b|profile="));
    }
}
