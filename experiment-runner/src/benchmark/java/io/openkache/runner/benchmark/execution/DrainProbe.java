package io.openkache.runner.benchmark.execution;

import java.util.Map;
import java.util.Objects;

/**
 * adapter별 비차단 상태 관찰. READY는 client publication/RPC와 모든 replica의 적용 frontier 확인이다.
 * expectedWrites는 빈 격리 그룹에서 이 run이 성공시킨 unique SET/DELETE 누계다.
 * 관측 불가를 READY로 치환하면 안 된다. 원격 관찰은 별도 구현/검증해야 한다.
 */
@FunctionalInterface
public interface DrainProbe {
    Observation observe(long expectedWrites);

    /** client close 이후에도 미완료 raw/apply 상태를 숨기지 않는 종료 관찰. */
    default Observation afterClose(long expectedWrites) { return observe(expectedWrites); }

    enum Status { READY, WAIT, FAILED, UNSUPPORTED }

    record Observation(Status status, String reason, Map<String, Long> evidence) {
        public Observation {
            Objects.requireNonNull(status);
            if (Objects.requireNonNull(reason).isBlank()) { throw new IllegalArgumentException("reason required"); }
            evidence = Map.copyOf(evidence);
        }
    }
}

