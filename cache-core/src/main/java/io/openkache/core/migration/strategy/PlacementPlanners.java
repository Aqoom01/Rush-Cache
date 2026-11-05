package io.openkache.core.migration.strategy;
import java.util.List;
/** B3의 배치는 비교 변수가 아니다. 모든 판단에 같은 adapter만 노출한다. */
public final class PlacementPlanners {
    public static final List<String> METHODS=List.of(FixedTabletPlacement.NAME);
    private PlacementPlanners(){}
    public static PlacementPlanner create(String name,PerformanceModel model){
        if(!METHODS.contains(name))throw new IllegalArgumentException("B3 supports only "+FixedTabletPlacement.NAME);
        return new PlacementPlanner(){
            @Override public String name(){return name;}
            @Override public ScalingPlan plan(ScalingSnapshot before,ScalingPlan desired){
                var units=FixedTabletPlacement.units(before,desired.active().size());
                if(!units.equals(desired.units()))throw new PlanningRejected("non-fixed-placement-plan");
                return desired;
            }
        };
    }
    public static final class PlanningRejected extends RuntimeException {
        private static final long serialVersionUID=1L;
        public PlanningRejected(String reason){super(reason);}
    }
}