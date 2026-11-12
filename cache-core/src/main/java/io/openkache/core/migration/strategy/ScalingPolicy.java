package io.openkache.core.migration.strategy;

/** B3 판단 전략. 인스턴스마다 학습/제어 상태를 가지며 한 controller thread에서만 호출한다. */
public interface ScalingPolicy {
    String name();
    /** 해당 window의 관측을 과거 이력에 추가한다. 미래 요청열은 전달하지 않는다. */
    default void observe(double total) {}
    ScalingPlan decide(ScalingSnapshot snapshot);
}
