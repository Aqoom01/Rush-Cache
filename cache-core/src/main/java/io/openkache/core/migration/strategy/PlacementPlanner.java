package io.openkache.core.migration.strategy;

/** controller가 정한 목표 노드 수를 바꾸지 않는 B3 후속 계획 전략. */
public interface PlacementPlanner {
    String name();
    ScalingPlan plan(ScalingSnapshot before,ScalingPlan desired);
}
