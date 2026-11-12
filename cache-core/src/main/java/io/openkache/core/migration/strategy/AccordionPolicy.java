package io.openkache.core.migration.strategy;
import java.util.ArrayList;
import java.util.Map;
/** 서버 수 후보 선택만 유지한다. MILP 배치 대신 모든 방법에 같은 고정 배치를 사용한다. */
public final class AccordionPolicy implements NodeCountPolicy {
    private final PerformanceModel model;private Double previous;
    public AccordionPolicy(PerformanceModel model){this.model=model;}
    @Override public String name(){return "accordion";}
    @Override public Decision decideCount(ScalingSnapshot s){
        int current=s.active().size();boolean decreasing=previous!=null&&s.total()<previous;previous=s.total();
        var counts=new ArrayList<Integer>();if(decreasing){for(int n=s.config().minNodes();n<=current;n++)counts.add(n);}else for(int n=current;n<=s.config().maxNodes();n++)counts.add(n);
        double ratio=java.util.Arrays.stream(s.writes()).sum()/Math.max(1,s.total());
        double capacity=model.limit(ratio)*((1-ratio)*s.config().readService()+ratio*s.config().writeService());
        var checks=new ArrayList<Map<String,Object>>();int desired=current;
        for(int count:counts){boolean fits=false;
            try{var units=FixedTabletPlacement.units(s,count);fits=ScalingSupport.loads(units,ScalingSupport.nodes(count),s.reads(),s.writes(),s.config()).values().stream().allMatch(v->v<=capacity+1e-10);}
            catch(PlacementPlanners.PlanningRejected rejected){ /* 공통 배치 실패도 후보 거부다. */ }
            checks.add(Map.of("nodes",count,"fixed_placement_fits",fits));if(fits){desired=count;break;}
        }
        return NodeCountPolicy.count(s,desired,"server-count-fixed-placement",Map.of("capacity",capacity,"candidates",checks,"native_MILP_used",false));
    }
}