/**
 * 장애 감지와 membership 관측. A3(SWIM / Lifeguard Failure Detection)가 사용한다.
 *
 * <p>책임: FailureDetector(Probe, Indirect Probe, Suspicion, Local Health)와 membership 변화 관측.
 *
 * <p>상태: SWIM baseline의 direct/indirect probe, suspicion, incarnation 반박, membership gossip을 구현한다. server의
 * TCP PING은 연결 확인용이며 이 패키지의 장애 감지가 아니다. PB 실서버의 UDP adapter는 server 모듈에 있으며,
 * SWIM baseline과 Lifeguard local-health/probe/suspicion 보정을 선택할 수 있다. 시간 계산은
 * {@link io.openkache.core.spi.TimeSource#monotonicNanos()}를 쓴다.
 */
package io.openkache.core.failure;
