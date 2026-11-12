package io.openkache.core.replication.ionia_seventh;
import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaReadSelectionTest {
    static final List<NodeId> IDS=List.of(new NodeId(1),new NodeId(2),new NodeId(3));
    static IoniaReadSelectionSettings settings(IoniaReadSelectionSettings.Mode mode){return new IoniaReadSelectionSettings(mode,64,32,1,1000,250,16);}
    @Test void V05_01_04_requiredSettingsRangesUnknownKeysAndPolicyHash(){
        Properties p=new Properties();assertThrows(IllegalArgumentException.class,()->IoniaReadSelectionSettings.parse(p));
        p.setProperty("read.selection","adaptive");for(String key:IoniaReadSelectionSettings.KEYS)if(!key.equals("read.selection"))p.setProperty(key,"1");
        var first=IoniaReadSelectionSettings.parse(p);assertEquals(64,first.profileId().length());
        for(String bad:List.of("0","-1","65537","2147483648","1.5"," 1","+1")){p.setProperty("read.logical.limit",bad);assertThrows(IllegalArgumentException.class,()->IoniaReadSelectionSettings.parse(p));}
        p.setProperty("read.logical.limit","1");p.setProperty("read.unknown","1");assertThrows(IllegalArgumentException.class,()->IoniaReadSelectionSettings.parse(p));p.remove("read.unknown");
        p.setProperty("read.selection","round-robin");assertNotEquals(first.profileId(),IoniaReadSelectionSettings.parse(p).profileId());
        assertThrows(IllegalArgumentException.class,()->new IoniaReadSelectionSettings(first.mode(),1,1,60001,1,1,1));
        assertDoesNotThrow(()->new IoniaReadSelectionSettings(first.mode(),65536,65536,60000,60000,60000,65536));
    }
    @Test void V05_01_02_leaderExcludedAtEveryRotationAndDeterministicTies(){
        for(int count:new int[]{3,5})for(var mode:IoniaReadSelectionSettings.Mode.values()){
            List<NodeId> nodes=new ArrayList<>();for(int i=1;i<=count;i++)nodes.add(new NodeId(i));
            for(NodeId leader:nodes){var a=new ReplicaSelector(nodes,new ClientId(0),settings(mode));var b=new ReplicaSelector(nodes,new ClientId(0),settings(mode));
                for(int i=0;i<20;i++){var selected=a.choose(leader,0);assertNotEquals(leader,selected.node());assertEquals(selected,b.choose(leader,0));}}
            var a=new ReplicaSelector(nodes,new ClientId(0),settings(mode));var b=new ReplicaSelector(nodes,new ClientId(2),settings(mode));
            assertNotEquals(a.choose(nodes.getFirst(),0).node(),b.choose(nodes.getFirst(),0).node());
        }
    }
    @Test void V05_04_ttlBoundaryColdAndExpiredSamplesAndSaturatingScore(){
        var selector=new ReplicaSelector(IDS,new ClientId(0),settings(IoniaReadSelectionSettings.Mode.ADAPTIVE));var s=selector.states.get(IDS.get(1));
        assertEquals(1_000_000,selector.score(s,0));s.sampled=true;s.ewma=9;s.at=0;
        assertTrue(selector.fresh(s,1_000_000_000));assertFalse(selector.fresh(s,1_000_000_001));assertFalse(selector.fresh(s,-1));
        assertEquals(1_000_000,selector.score(s,1_000_000_001));assertEquals(Long.MAX_VALUE,ReplicaSelector.saturatingMultiply(Long.MAX_VALUE,2));
    }
    @Test void V05_06_explorationVisitsAllContinuouslyEligibleFollowers(){
        for(int period:new int[]{1,2,16}){
            var config=new IoniaReadSelectionSettings(IoniaReadSelectionSettings.Mode.ADAPTIVE,64,32,1,1000,250,period);
            var selector=new ReplicaSelector(IDS,new ClientId(0),config);selector.states.forEach((n,s)->{s.sampled=true;s.at=0;s.ewma=n.equals(IDS.get(1))?1:100;});
            Set<NodeId> explored=new HashSet<>();for(int i=0;i<period*2;i++){var c=selector.choose(IDS.getFirst(),0);if(c.reason().equals("EXPLORE"))explored.add(c.node());}
            assertEquals(Set.of(IDS.get(1),IDS.get(2)),explored);
        }
    }
    @Test void V05_06_allCandidatesFullOrCoolingReturnNoChoice(){
        var selector=new ReplicaSelector(IDS,new ClientId(0),settings(IoniaReadSelectionSettings.Mode.ADAPTIVE));
        selector.states.get(IDS.get(1)).pending=32;var other=selector.states.get(IDS.get(2));other.cooling=true;other.cooldownUntil=10;
        assertNull(selector.choose(IDS.getFirst(),9));assertEquals(IDS.get(2),selector.choose(IDS.getFirst(),10).node());
    }
    @Test void V05_12_sequenceOverflowNeverWraps(){
        var selector=new ReplicaSelector(IDS,new ClientId(0),settings(IoniaReadSelectionSettings.Mode.ADAPTIVE));selector.nextSequence=Long.MAX_VALUE;
        var r=new IoniaClientTransport.Request(0,new Command.Get(Bytes.utf8("k")),new ClientId(0),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,100);
        assertThrows(ArithmeticException.class,()->selector.begin(IDS.get(1),r,0));assertEquals(0,selector.states.get(IDS.get(1)).pending);
    }
}
