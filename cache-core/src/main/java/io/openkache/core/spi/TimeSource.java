package io.openkache.core.spi;

/**
 * 시간 공급원. 시뮬레이션에서는 가상 시간 구현으로 바꿔 끼운다.
 *
 * <p>두 시간은 용도가 다르므로 섞어 계산하지 않는다.
 *
 * <ul>
 *   <li>{@link #monotonicNanos()}: 경과 시간과 deadline 계산용. 값 자체에는 의미가 없고 같은 JVM 안에서 두 값의 차이만
 *       의미가 있다. 다른 노드로 전송하지 않는다.
 *   <li>{@link #epochMillis()}: 로그와 결과 기록용 벽시계 시각. 시스템 시계 조정으로 뒤로 갈 수 있으므로 timeout 계산에
 *       쓰지 않는다.
 * </ul>
 *
 * <p>구현은 여러 thread에서 동시에 호출해도 안전해야 한다.
 */
public interface TimeSource {

    /** 단조 증가하는 나노초. */
    long monotonicNanos();

    /** Unix epoch 기준 밀리초. */
    long epochMillis();

    /** JVM 시계({@link System#nanoTime()}, {@link System#currentTimeMillis()})를 쓰는 구현. */
    static TimeSource system() {
        return SystemTimeSource.INSTANCE;
    }
}
