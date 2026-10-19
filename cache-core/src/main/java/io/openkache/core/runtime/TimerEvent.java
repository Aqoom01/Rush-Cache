package io.openkache.core.runtime;

import java.util.Objects;

/**
 * 예약한 timer가 만료되었음을 알린다.
 *
 * @param name timer 종류. 코드에 상수로 정의한 고정 이름을 쓴다 (예: {@code "heartbeat"})
 * @param firedAtNanos 만료를 감지한 {@code TimeSource.monotonicNanos()} 값
 */
public record TimerEvent(String name, long firedAtNanos) {

    public TimerEvent {
        Objects.requireNonNull(name, "name");
    }
}
