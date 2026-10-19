/**
 * core가 외부 환경에 요구하는 계약 (Service Provider Interface).
 *
 * <p>전송({@link io.openkache.core.spi.Transport}), 시간({@link io.openkache.core.spi.TimeSource}),
 * 지표({@link io.openkache.core.spi.MetricsSink}), 안전성 메타데이터 저장({@link io.openkache.core.spi.EpochStore})을
 * 인터페이스로만 정의한다. 실제 구현은 cache-server(Netty, Prometheus)나 테스트·시뮬레이션 코드가 제공하므로 core는 외부
 * 라이브러리 없이 테스트할 수 있다.
 */
package io.openkache.core.spi;
