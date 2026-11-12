package io.openkache.core.replication.ionia_fifth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaConfig;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaApplyGateTest {
    @Test void V04_02_limitsNeverExceedDistinctKeysAndSlotsAreReturned() {
        for(int cap:new int[]{1,2,4,65536}) {
            var g=new IoniaApplyGate(cap);for(int i=0;i<8;i++)g.ready(Bytes.utf8("k"+i));
            List<Bytes> running=new ArrayList<>();g.acquire().ifPresent(running::add);
            while(g.acquire().map(running::add).orElse(false)){ }
            assertEquals(Math.min(cap,8),running.size());assertEquals(running.size(),g.snapshot().activeKeys());
            for(Bytes key:running)g.completed(key);
            assertEquals(0,g.snapshot().activeKeys());assertEquals(cap,g.snapshot().available());
        }
    }
    @Test void V04_01_07_equalHashesDoNotMergeIndependentKeys() {
        Bytes a=Bytes.utf8("Aa"),b=Bytes.utf8("BB");assertEquals(a.hashCode(),b.hashCode());assertNotEquals(a,b);
        var g=new IoniaApplyGate(2);g.ready(a);g.ready(b);assertEquals(a,g.acquire().orElseThrow());assertEquals(b,g.acquire().orElseThrow());
        assertEquals(2,g.snapshot().activeKeys());assertThrows(IllegalStateException.class,()->g.ready(a));
    }
    @Test void V04_02_fairKeyRotationPlacesHotKeyBehindWaitingColdKey() {
        var g=new IoniaApplyGate(1);Bytes hot=Bytes.utf8("hot"),cold=Bytes.utf8("cold");
        g.ready(hot);g.ready(cold);g.ready(hot);assertEquals(hot,g.acquire().orElseThrow());
        g.completed(hot);g.ready(hot);assertEquals(cold,g.acquire().orElseThrow());
    }
    @Test void V04_04_duplicateReleaseIsRejectedAndSnapshotsDoNotChange() {
        var g=new IoniaApplyGate(1);Bytes key=Bytes.utf8("k");g.ready(key);g.acquire().orElseThrow();
        var before=g.snapshot();g.completed(key);assertEquals(1,before.activeKeys());assertEquals(0,g.snapshot().activeKeys());
        assertThrows(IllegalStateException.class,()->g.completed(key));
    }
    @Test void V04_05_resetCountersDistinguishDiscardedOldGenerationPermits() {
        var g=new IoniaApplyGate(2);g.ready(Bytes.utf8("a"));g.ready(Bytes.utf8("b"));g.acquire().orElseThrow();
        g.resetAfterDrain();var s=g.snapshot();assertEquals(0,s.activeKeys());assertEquals(0,s.readyKeys());assertEquals(1,s.resetDiscarded());
        assertEquals(s.starts(),s.released()+s.resetDiscarded()+s.activeKeys());
    }
    @Test void V04_08_settingsAreRequiredAndConcurrencyAndLayoutHaveDistinctProfiles() {
        Properties p=new Properties();assertThrows(IllegalArgumentException.class,()->IoniaApplySettings.parse(p));
        p.setProperty(IoniaApplySettings.SHARDS,"2");
        for(String value:List.of("0","-1","65537","2147483648","+1","1.5"," 1")) {
            p.setProperty(IoniaApplySettings.LIMIT,value);assertThrows(IllegalArgumentException.class,()->IoniaApplySettings.parse(p));
        }
        p.setProperty(IoniaApplySettings.LIMIT,"4");var policy=IoniaApplySettings.parse(p);assertEquals(64,policy.profileId().length());
        assertNotEquals(policy.profileId(),new IoniaApplySettings(3,2).profileId());assertNotEquals(policy.profileId(),new IoniaApplySettings(4,3).profileId());
        assertThrows(IllegalArgumentException.class,()->new IoniaApplySettings(1,0));assertThrows(IllegalArgumentException.class,()->new IoniaApplySettings(1,257));
        var nodes=List.of(new NodeId(1),new NodeId(2),new NodeId(3));var config=new IoniaConfig(nodes.getFirst(),nodes.getFirst(),nodes,0);
        assertTrue(policy.configurationId(config).contains("algorithm=V04a"));
    }
}
