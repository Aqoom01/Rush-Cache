package io.openkache.core.api;

import io.openkache.core.spi.TimeSource;
import java.time.Duration;
import java.util.Objects;

/**
 * 요청을 끝내야 하는 시각. 이 JVM의 {@link TimeSource#monotonicNanos()} 기준 절대값이다.
 *
 * <p>wire에서는 절대 시각이 아니라 상대 timeout(밀리초)을 주고받는다. 노드마다 monotonic 시계의 기준점이 다르기 때문이다. server는
 * 요청을 받은 순간 {@link #after(TimeSource, Duration)}로 이 JVM 기준 deadline으로 바꾼다. 다른 노드에 전달할 때는
 * {@link #remaining(TimeSource)}로 남은 시간을 다시 상대값으로 보낸다. 이 값 자체를 다른 노드로 보내지 않는다.
 *
 * @param monotonicNanos deadline 시각의 {@code monotonicNanos()} 값
 */
public record Deadline(long monotonicNanos) {

    /** 지금부터 {@code timeout} 뒤의 deadline. */
    public static Deadline after(TimeSource time, Duration timeout) {
        Objects.requireNonNull(time, "time");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        return new Deadline(time.monotonicNanos() + timeout.toNanos());
    }

    /** 남은 시간. 이미 지났으면 0 이하. */
    public Duration remaining(TimeSource time) {
        // nanoTime 값은 overflow될 수 있으므로 비교는 항상 차이로 한다.
        return Duration.ofNanos(monotonicNanos - time.monotonicNanos());
    }

    public boolean isExpired(TimeSource time) {
        return monotonicNanos - time.monotonicNanos() <= 0;
    }
}
