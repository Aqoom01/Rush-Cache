package io.openkache.runner.benchmark.execution;

/** 실험 control thread의 선택적 관찰. 기본 경로는 thread/수집기를 만들지 않는다. */
public interface RunLifecycle {
    RunLifecycle NONE = new RunLifecycle() {};
    default void stage(ClosedLoopResult.Stage stage, long monotonicNanos) {}
    /** client/worker cleanup 뒤 호출한다. 수집 자원의 종료와 마지막 관찰을 책임진다. */
    default void finished(long monotonicNanos) {}
}
