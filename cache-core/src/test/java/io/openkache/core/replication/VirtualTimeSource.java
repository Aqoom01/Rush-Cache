package io.openkache.core.replication;

import io.openkache.core.spi.TimeSource;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 테스트가 직접 진행시키는 가상 시계. 실제 sleep 없이 deadline 경계를 재현한다.
 *
 * <p>{@link #monotonicNanos()}는 0에서 시작해 {@link #advance}로만 늘어난다. {@link #epochMillis()}는 같은 값을 밀리초로
 * 바꾼 것이며 기록용이다. 가상 시간을 진행해도 timer 이벤트가 자동으로 발생하지는 않는다. 테스트가 만료된 timer를 엔진에 직접
 * 넣어야 한다.
 */
public final class VirtualTimeSource implements TimeSource {

    private final AtomicLong nanos = new AtomicLong();

    /** 시계를 {@code duration}만큼 앞으로 보낸다. 음수는 허용하지 않는다. */
    public void advance(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("duration must not be negative: " + duration);
        }
        nanos.addAndGet(duration.toNanos());
    }

    /** 시계를 {@code targetNanos}로 보낸다. 지금보다 과거면 움직이지 않는다. */
    public void advanceTo(long targetNanos) {
        nanos.accumulateAndGet(targetNanos, Math::max);
    }

    @Override
    public long monotonicNanos() {
        return nanos.get();
    }

    @Override
    public long epochMillis() {
        return nanos.get() / 1_000_000;
    }
}
