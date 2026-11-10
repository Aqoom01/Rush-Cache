package io.openkache.runner;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 실험 실행 결과. {@link ResultWriter}가 JSON(원본)·CSV(요약)로 기록한다.
 *
 * <p>측정하지 않은 값은 생략하지 않고 {@link Optional#empty()}나 명시적인 "측정하지 않음" 문자열로 남긴다(0으로
 * 채우지 않는다). 예: {@code serverWorkerCountHint}가 -1이면 운영자가 적어 넣지 않았다는 뜻이고, SSD IOPS·byte,
 * 분산 failover 시간은 이 Runner가 전혀 다루지 않으므로 필드 자체가 없다. {@code storageLayer}는 운영자가
 * {@code server.storage.note}로 적은 서버 저장소 종류이고(기본 인메모리), 단일 노드라는 점은 {@code nodeCount}에 명시한다.
 */
record ExperimentResult(
        String executionId,
        String resultSchemaVersion,
        long startedAtEpochMillis,
        String study,
        String variant,
        String scenario,
        long seed,
        int runNumber,
        int repeatCount,
        Map<String, String> configSnapshot,
        Optional<RunnerInvocation> invocation,
        int nodeCount,
        int concurrency,
        int serverWorkerCountHint,
        String storageLayer,
        String jdkVendor,
        String jdkVersion,
        List<String> runnerJvmArgs,
        String serverJvmOptsNote,
        String osName,
        String osArch,
        int availableProcessors,
        long warmupRequests,
        long warmupElapsedNanos,
        long measuredRequests,
        long measuredElapsedNanos,
        double throughputPerSecond,
        long p50Nanos,
        long p99Nanos,
        int latencySampleCount,
        long latencyTotalObserved,
        Map<Outcome, Long> outcomeCounts,
        String faultScenario,
        Optional<String> gitCommit,
        List<TargetResult> targets,
        List<WindowResult> windows) {}
