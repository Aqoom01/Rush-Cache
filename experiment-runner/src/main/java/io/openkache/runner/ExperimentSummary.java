package io.openkache.runner;

import java.util.DoubleSummaryStatistics;
import java.util.List;

/** 같은 설정으로 반복 실행한 결과의 공통 요약. */
record ExperimentSummary(
        int requestedRuns,
        int successfulRuns,
        int failedRuns,
        int timeoutRuns,
        int skippedRuns,
        MetricSummary throughputPerSecond,
        MetricSummary p50Nanos,
        MetricSummary p99Nanos) {

    static ExperimentSummary from(int requestedRuns, List<RunAttempt> attempts) {
        List<ExperimentResult> successes =
                attempts.stream().flatMap(attempt -> attempt.result().stream()).toList();
        return new ExperimentSummary(
                requestedRuns,
                successes.size(),
                (int) attempts.stream().filter(attempt -> attempt.status() == RunStatus.FAILED).count(),
                (int) attempts.stream().filter(attempt -> attempt.status() == RunStatus.TIMEOUT).count(),
                (int) attempts.stream().filter(attempt -> attempt.status() == RunStatus.SKIPPED).count(),
                metric(successes.stream().mapToDouble(ExperimentResult::throughputPerSecond).summaryStatistics()),
                metric(successes.stream().mapToDouble(ExperimentResult::p50Nanos).summaryStatistics()),
                metric(successes.stream().mapToDouble(ExperimentResult::p99Nanos).summaryStatistics()));
    }

    private static MetricSummary metric(DoubleSummaryStatistics statistics) {
        if (statistics.getCount() == 0) {
            return MetricSummary.notMeasured();
        }
        return new MetricSummary(true, statistics.getAverage(), statistics.getMin(), statistics.getMax());
    }

    record MetricSummary(boolean measured, double average, double minimum, double maximum) {
        static MetricSummary notMeasured() {
            return new MetricSummary(false, 0, 0, 0);
        }
    }
}
