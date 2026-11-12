package io.openkache.core.migration.strategy;

import java.util.Map;

/** B3 판단 경계. controller thread에서 노드 수만 반환하며 Tablet 배치를 소유하지 않는다. */
public interface NodeCountPolicy extends ScalingPolicy {
    record Decision(int nodes,String reason,Map<String,Object> detail) {
        public Decision { if(nodes<1)throw new IllegalArgumentException("positive node count required");detail=Map.copyOf(detail); }
    }
    Decision decideCount(ScalingSnapshot snapshot);
    @Override default ScalingPlan decide(ScalingSnapshot snapshot) {
        return FixedTabletPlacement.apply(snapshot,decideCount(snapshot));
    }
    static Decision count(ScalingSnapshot s,int nodes,String reason,Map<String,Object> detail) {
        return new Decision(Math.max(s.config().minNodes(),Math.min(s.config().maxNodes(),nodes)),reason,detail);
    }
}
