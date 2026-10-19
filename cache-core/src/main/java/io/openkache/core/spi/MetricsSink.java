package io.openkache.core.spi;

/**
 * core가 지표를 내보내는 최소 계약. 외부 metrics 라이브러리에 의존하지 않는다.
 *
 * <p>{@code name}은 코드에 상수로 정의한 고정 이름만 쓴다. Key, request ID, client ID처럼 값의 종류가 계속 늘어나는 정보를
 * 이름에 넣지 않는다. 이 계약에는 label이 없으며, 필요한 고정 label은 server의 adapter가 붙인다.
 *
 * <p>구현은 여러 thread에서 동시에 호출해도 안전해야 하고, 호출자를 block하지 않아야 한다.
 */
public interface MetricsSink {

    /** 누적 카운터를 {@code delta}만큼 증가시킨다. {@code delta}는 0 이상이다. */
    void increment(String name, long delta);

    /** 소요 시간 한 건을 나노초 단위로 기록한다. */
    void recordNanos(String name, long nanos);

    /** 현재 값을 기록한다 (예: 엔트리 수, mailbox 길이). */
    void gauge(String name, double value);

    /** 아무것도 기록하지 않는 구현. 테스트와 metrics를 끈 실행에 쓴다. */
    static MetricsSink noop() {
        return NoOpMetricsSink.INSTANCE;
    }
}
