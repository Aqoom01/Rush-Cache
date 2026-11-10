package io.openkache.runner;

import java.util.Map;

/**
 * 측정 구간에서 대상 서버 하나가 받은 요청의 결과. {@code routing.boundaries}로 여러 서버에 나눠 보낼 때 서버별 부하와 지연을
 * 비교하는 데 쓴다.
 *
 * @param address {@code host:port}
 * @param hashRange 이 서버로 보낸 key 해시 구간 {@code [start, end)}. 부호 없는 10진수, {@code end}가 {@code 2^64}면 끝까지
 * @param requests 이 서버로 보낸 측정 요청 수
 * @param p50Nanos 이 서버 요청의 p50 latency(나노초, reservoir 근사)
 * @param p99Nanos 이 서버 요청의 p99 latency(나노초, reservoir 근사)
 * @param outcomeCounts 이 서버 요청의 결과 종류별 수
 */
record TargetResult(
        String address, String hashRange, long requests, long p50Nanos, long p99Nanos, Map<Outcome, Long> outcomeCounts) {}
