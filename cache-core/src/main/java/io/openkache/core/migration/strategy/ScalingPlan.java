package io.openkache.core.migration.strategy;

import java.util.List;
import java.util.Map;

/** 한 번의 판단 결과. active는 준비·복사 이후의 목표 상태이며 즉시 READY라는 뜻이 아니다. */
public record ScalingPlan(List<Integer> active,List<PartitionUnit> units,String reason,Map<String,Object> detail) {
    public ScalingPlan { active=List.copyOf(active); units=List.copyOf(units); detail=Map.copyOf(detail); if(reason==null||reason.isBlank()) throw new IllegalArgumentException("missing plan reason"); }
}
