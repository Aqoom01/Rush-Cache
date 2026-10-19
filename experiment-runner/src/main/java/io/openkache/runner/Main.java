package io.openkache.runner;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Experiment Runner 진입점.
 *
 * <p>사용법: {@code openkache-experiment-runner <config.properties>}. 설정 형식은 {@link RunnerConfig}를
 * 참고한다. 단일 노드 baseline만 지원한다 — 대상은 {@code cache-server} 하나이고, 복제·장애 주입·multi-node 조율은
 * 하지 않는다.
 */
public final class Main {

    private static final int EXIT_CONFIG_ERROR = 2;
    private static final int EXIT_RUN_ERROR = 1;

    private Main() {}

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: openkache-experiment-runner <config.properties>");
            System.exit(EXIT_CONFIG_ERROR);
            return;
        }

        RunnerConfig config;
        try {
            config = RunnerConfig.load(Path.of(args[0]));
        } catch (ConfigException e) {
            System.err.println(e.getMessage());
            System.exit(EXIT_CONFIG_ERROR);
            return;
        }

        System.out.printf(
                Locale.ROOT,
                "connecting to %s:%d (study=%s variant=%s scenario=%s seed=%d)%n",
                config.targetHost(),
                config.targetPort(),
                config.study(),
                config.variant(),
                config.scenario(),
                config.seed());

        ExperimentResult result;
        try {
            result = new ExperimentRunner(config).run();
        } catch (IOException e) {
            System.err.println("failed to reach target server: " + e.getMessage());
            System.exit(EXIT_RUN_ERROR);
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("interrupted while running experiment");
            System.exit(EXIT_RUN_ERROR);
            return;
        }

        Path jsonFile = ResultWriter.writeJson(result, config.resultsDir());
        Path csvFile = ResultWriter.appendCsv(result, config.resultsDir());

        System.out.printf(Locale.ROOT, "executionId=%s%n", result.executionId());
        System.out.printf(
                Locale.ROOT,
                "measured: requests=%d elapsedMs=%.1f throughputPerSec=%.1f p50us=%.1f p99us=%.1f%n",
                result.measuredRequests(),
                result.measuredElapsedNanos() / 1_000_000.0,
                result.throughputPerSecond(),
                result.p50Nanos() / 1_000.0,
                result.p99Nanos() / 1_000.0);
        System.out.printf(Locale.ROOT, "outcomes: %s%n", result.outcomeCounts());
        System.out.printf(Locale.ROOT, "result json: %s%n", jsonFile);
        System.out.printf(Locale.ROOT, "result csv: %s%n", csvFile);
    }
}
