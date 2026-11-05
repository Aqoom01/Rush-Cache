package io.openkache.core.migration.strategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/** SCADS 성능 경계 기반 증설·통합의 count-only 변형. best-fit 이동과 읽기 복제는 제외한다. */
public final class ScadsPolicy implements NodeCountPolicy {
    private final PerformanceModel model;
    public ScadsPolicy(PerformanceModel model){this.model=model;}
    @Override public String name(){return "scads";}
    boolean fits(ScalingSnapshot s,int count) {
        final List<PartitionUnit> units;
        try{units=FixedTabletPlacement.units(s,count);}catch(PlacementPlanners.PlanningRejected rejected){return false;}
        double[] reads=s.reads(),writes=s.writes();
        for(int node:ScalingSupport.nodes(count)){double r=0,w=0;for(var u:units)if(u.holders().contains(node))for(int key:u.keys()){r+=reads[key];w+=writes[key];}if(!model.fits(r,w))return false;}
        return true;
    }
    @Override public Decision decideCount(ScalingSnapshot s){
        int current=s.active().size(),desired=current;var candidates=new ArrayList<Integer>();
        if(!fits(s,current)){
            for(int n=current+1;n<=s.config().maxNodes();n++){candidates.add(n);if(fits(s,n)){desired=n;break;}}
            return NodeCountPolicy.count(s,desired,desired==current?"model-no-feasible-count":"model-scale-out",Map.of("checked_counts",candidates));
        }
        for(int n=current-1;n>=s.config().minNodes();n--){candidates.add(n);if(!fits(s,n))break;desired=n;}
        return NodeCountPolicy.count(s,desired,"model-consolidate-count",Map.of("checked_counts",candidates));
    }
}