package io.openkache.runner;

import java.util.Map;

/**
 * window 실행({@code schedule.file})에서 window 하나 × 서버 하나의 결과.
 *
 * @param window window 번호
 * @param address 서버 {@code host:port}
 * @param burst burst window인가
 * @param movedTablets 이 window부터 주인이 바뀐 Tablet 수(계획 기준, 서버 전체)
 * @param requests 이 서버로 보낸 요청 수
 * @param outcomeCounts 결과 종류별 수. 옮긴 Tablet이 새 서버에서 비어 있으면 GET이 MISS가 된다
 * @param p50Nanos p50 latency(나노초, reservoir 근사)
 * @param p99Nanos p99 latency(나노초, reservoir 근사)
 * @param elapsedNanos window 전체 소요 시간(모든 서버 공통)
 * @param cpuUtilization 이 window 동안 서버 프로세스 CPU 시간 / window 시간(vCPU 1개 = 1.0). 읽지 못하면 NaN
 * @param refills {@code refill.on.miss}로 이 서버에 다시 채운 SET 수(요청 수·통계에는 넣지 않음)
 */
record WindowResult(
        int window,
        String address,
        boolean burst,
        int movedTablets,
        long requests,
        Map<Outcome, Long> outcomeCounts,
        long p50Nanos,
        long p99Nanos,
        long elapsedNanos,
        double cpuUtilization,
        long refills) {}
