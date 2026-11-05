package io.openkache.core.migration.strategy;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class NodeCountIsolationTest {
    static ScalingSnapshot snapshot(ScalingConfig c,double cpu){
        var initial=new ScalingSimulation(c,"fixed_single");double[] reads=new double[c.keys()];Arrays.fill(reads,3);
        var cpus=new TreeMap<Integer,Double>();initial.active().forEach(n->cpus.put(n,cpu));
        return new ScalingSnapshot(c,3,initial.active(),initial.units(),reads,new double[c.keys()],cpus,.05,96,96,new long[c.maxNodes()],-100,"");
    }
    @Test void decisionCannotCarryAnyPlacementAndSameCountUsesSameAdapter(){
        var s=snapshot(ScalingConfig.defaults(),.5);List<PartitionUnit> expected=FixedTabletPlacement.units(s,3);
        for(String name:ScalingPolicies.LITERATURE){var decision=new NodeCountPolicy.Decision(3,name,Map.of());var plan=FixedTabletPlacement.apply(s,decision);assertEquals(expected,plan.units());assertEquals(ScalingSupport.nodes(3),plan.active());}
        assertEquals(List.of("nodes","reason","detail"),Arrays.stream(NodeCountPolicy.Decision.class.getRecordComponents()).map(java.lang.reflect.RecordComponent::getName).toList());
    }
    @Test void unchangedCountCannotRebalanceEvenForHotspot(){
        var s=snapshot(ScalingConfig.defaults(),.5);double[] reads=s.reads();reads[0]=10000;
        var hot=new ScalingSnapshot(s.config(),3,s.active(),s.units(),reads,s.writes(),s.cpu(),1,10000,1,s.hints(),-100,"");
        assertEquals(hot.units(),FixedTabletPlacement.apply(hot,new NodeCountPolicy.Decision(4,"keep",Map.of())).units());
    }
    @Test void estoreWatermarksChangeCountWithoutRefiningTablets(){
        var c=ScalingConfig.defaults();var policy=new EStorePolicy();
        assertEquals(5,policy.decideCount(snapshot(c,.95)).nodes());assertEquals(3,policy.decideCount(snapshot(c,.1)).nodes());assertEquals(4,policy.decideCount(snapshot(c,.5)).nodes());
        var s=snapshot(c,.95);var plan=policy.decide(s);assertEquals(s.units().stream().map(PartitionUnit::keys).toList(),plan.units().stream().map(PartitionUnit::keys).toList());assertTrue(plan.units().stream().allMatch(u->u.holders().size()==1));
    }
    @Test void commonMemoryGuardHoldsRejectedScaleInInsteadOfInventingPlacement(){
        var p=new Properties();p.setProperty("scaling.node.bytes","2048");var s=snapshot(ScalingConfig.from(p),.1);
        var plan=FixedTabletPlacement.apply(s,new NodeCountPolicy.Decision(2,"scale-in",Map.of()));
        assertEquals(s.active(),plan.active());assertEquals(s.units(),plan.units());assertEquals("fixed-placement-rejected",plan.reason());assertEquals(2,plan.detail().get("requested_nodes"));
    }
    @Test void activeRegistryRejectsB2StrategiesAndPaxUsesSameSingleOwnerCopy(){
        assertEquals(7,ScalingPolicies.LITERATURE.size());assertFalse(ScalingPolicies.METHODS.contains("slicer"));assertEquals(List.of(FixedTabletPlacement.NAME),PlacementPlanners.METHODS);
        for(String planner:List.of("camig","slicer","accordion-repack","controller-native"))assertThrows(IllegalArgumentException.class,()->new StrategySelection("accordion",planner,"fifo","pre-copy","preflight-all"));
        var pax=new StrategySelection("pax",FixedTabletPlacement.NAME,"fifo","pre-copy","preflight-all");assertDoesNotThrow(()->pax.require(new StrategySelection.Capabilities(false,false,false)));
        assertThrows(IllegalArgumentException.class,()->new StrategySelection("slicer",FixedTabletPlacement.NAME,"fifo","pre-copy","preflight-all"));
    }
    @Test void allSevenReturnFixedTabletsWithNoNativePlacementMethod() throws NoSuchMethodException {
        var c=ScalingConfig.defaults().profile(true);var model=PerformanceModel.calibrate(c);
        for(String method:ScalingPolicies.LITERATURE){var policy=ScalingPolicies.create(method,model,ScalingSimulation.history("low",1,c));var s=snapshot(c,.5);policy.observe(s.total());var plan=policy.decide(s);
            assertEquals(NodeCountPolicy.class,policy.getClass().getMethod("decide",ScalingSnapshot.class).getDeclaringClass());
            assertEquals(s.units().stream().map(PartitionUnit::keys).toList(),plan.units().stream().map(PartitionUnit::keys).toList());
            assertTrue(plan.units().stream().allMatch(u->u.holders().size()==1));assertEquals(FixedTabletPlacement.VERSION,plan.detail().get("placement_policy"));
        }
    }
}
