package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

/** 모든 판단에 같은 고정 Tablet LPT adapter. 분할·병합·복제 없이 memory를 제한한다. */
public final class FixedTabletPlacement {
    public static final String NAME="fixed-tablet-lpt";
    public static final String VERSION="fixed-tablet-lpt-v1";
    private FixedTabletPlacement(){}
    public static List<PartitionUnit> units(ScalingSnapshot s,int count) {
        if(count<s.config().minNodes()||count>s.config().maxNodes())throw new PlacementPlanners.PlanningRejected("node-count-bound");
        if(s.units().stream().anyMatch(u->u.holders().size()!=1))throw new PlacementPlanners.PlanningRejected("single-owner-required");
        var active=ScalingSupport.nodes(count);
        if(active.equals(s.active()))return s.units();
        var load=new TreeMap<Integer,Double>();var memory=new TreeMap<Integer,Long>();
        active.forEach(n->{load.put(n,0.);memory.put(n,0L);});var out=new ArrayList<PartitionUnit>();
        double[] reads=s.reads(),writes=s.writes();
        var ordered=s.units().stream().sorted(Comparator.<PartitionUnit>comparingDouble(u->-ScalingSupport.work(u,reads,writes,s.config())).thenComparingInt(u->u.keys().getFirst())).toList();
        for(var u:ordered){long bytes=u.keys().size()*s.config().keyBytes();
            int node=active.stream().filter(n->memory.get(n)+bytes<=s.config().nodeBytes())
                    .min(Comparator.<Integer>comparingDouble(load::get).thenComparing(n->!u.holders().contains(n)).thenComparingInt(Integer::intValue))
                    .orElseThrow(()->new PlacementPlanners.PlanningRejected("fixed-placement-memory-blocked"));
            out.add(u.at(node));memory.merge(node,bytes,Long::sum);load.merge(node,ScalingSupport.work(u,reads,writes,s.config()),Double::sum);
        }
        return ScalingSupport.sorted(out);
    }
    public static ScalingPlan apply(ScalingSnapshot s,NodeCountPolicy.Decision decision) {
        var detail=new LinkedHashMap<String,Object>(decision.detail());detail.put("requested_nodes",decision.nodes());detail.put("placement_policy",VERSION);
        try{return new ScalingPlan(ScalingSupport.nodes(decision.nodes()),units(s,decision.nodes()),decision.reason(),detail);}
        catch(PlacementPlanners.PlanningRejected blocked){detail.put("placement_rejection",blocked.getMessage());return new ScalingPlan(s.active(),s.units(),"fixed-placement-rejected",detail);}
    }
}
