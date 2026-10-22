package io.openkache.runner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Experiment Runner 진입점.
 *
 * <p>사용법: {@code openkache-experiment-runner <config.properties>}. 설정 형식은 {@link RunnerConfig}를
 * 참고한다. 기본 TCP 실험 또는 {@code execution.mode=research}의 config 지정 JVM backend를 실행한다.
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
            String mode = ResearchConfig.mode(Path.of(args[0]));
            if (mode.equals("research")) {
                int status = ResearchLauncher.run(ResearchConfig.load(Path.of(args[0])));
                if (status != 0) System.exit(status);
                return;
            }
            if (!mode.equals("tcp")) throw new ConfigException("execution.mode는 tcp 또는 research입니다");
            config = RunnerConfig.load(Path.of(args[0]));
        } catch (ConfigException e) {
            System.err.println(e.getMessage());
            System.exit(EXIT_CONFIG_ERROR);
            return;
        } catch (java.io.IOException e) {
            System.err.println("연구 실행·결과 저장 실패: " + e.getMessage());
            System.exit(EXIT_RUN_ERROR);
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

        RunnerInvocation invocation = RunnerInvocation.fromMainArgs(args);
        List<RunAttempt> attempts =
                execute(config, (runConfig, runNumber, timeoutMs) -> runOnce(runConfig, runNumber, timeoutMs, invocation));
        Map<Integer, Path> resultFiles = new HashMap<>();
        for (RunAttempt attempt : attempts) {
            System.out.printf(Locale.ROOT, "run %d/%d%n", attempt.runNumber(), config.repeatCount());
            if (attempt.status() != RunStatus.SUCCESS) {
                System.err.printf(Locale.ROOT, "run %d ended with %s%n", attempt.runNumber(), attempt.status());
                continue;
            }
            ExperimentResult result = attempt.result().orElseThrow();

            Path jsonFile = ResultWriter.writeJson(result, config.resultsDir());
            Path csvFile = ResultWriter.appendCsv(result, config.resultsDir());
            resultFiles.put(attempt.runNumber(), jsonFile);

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

        ExperimentSummary summary = ExperimentSummary.from(config.repeatCount(), attempts);
        Path summaryFile = SummaryWriter.write(config, attempts, summary, resultFiles);
        System.out.printf(
                Locale.ROOT,
                "summary: success=%d failed=%d timeout=%d skipped=%d (%s)%n",
                summary.successfulRuns(),
                summary.failedRuns(),
                summary.timeoutRuns(),
                summary.skippedRuns(),
                summaryFile);
        if (summary.failedRuns() > 0 || summary.timeoutRuns() > 0 || summary.skippedRuns() > 0) {
            System.exit(EXIT_RUN_ERROR);
        }
    }

    static List<RunAttempt> execute(RunnerConfig config, AttemptRunner attemptRunner) {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.executionTimeoutMs());
        List<RunAttempt> attempts = new ArrayList<>();
        for (int runNumber = 1; runNumber <= config.repeatCount(); runNumber++) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                for (int skippedRun = runNumber; skippedRun <= config.repeatCount(); skippedRun++) {
                    attempts.add(RunAttempt.skipped(skippedRun, "overall execution timeout before start"));
                }
                break;
            }
            long remainingMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
            attempts.add(attemptRunner.run(config, runNumber, Math.min(remainingMs, config.runTimeoutMs())));
        }
        return attempts;
    }

    static RunAttempt runOnce(RunnerConfig config, int runNumber, long timeoutMs) {
        return runOnce(runNumber, timeoutMs, () -> new ExperimentRunner(config, runNumber).run());
    }

    private static RunAttempt runOnce(
            RunnerConfig config, int runNumber, long timeoutMs, RunnerInvocation invocation) {
        return runOnce(runNumber, timeoutMs, () -> new ExperimentRunner(config, runNumber, invocation).run());
    }

    static RunAttempt runOnce(int runNumber, long timeoutMs, ExperimentCall call) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<ExperimentResult> future = executor.submit(call::run);
        try {
            return RunAttempt.success(runNumber, future.get(Math.max(1, timeoutMs), TimeUnit.MILLISECONDS));
        } catch (TimeoutException e) {
            future.cancel(true);
            return RunAttempt.timedOut(runNumber);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            return RunAttempt.failed(runNumber, cause == null ? e.toString() : cause.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RunAttempt.failed(runNumber, "interrupted");
        } finally {
            executor.shutdownNow();
            awaitExecutorTermination(executor);
        }
    }

    private static void awaitExecutorTermination(ExecutorService executor) {
        boolean interrupted = false;
        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                interrupted = true;
                executor.shutdownNow();
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    interface AttemptRunner {
        RunAttempt run(RunnerConfig config, int runNumber, long timeoutMs);
    }

    @FunctionalInterface
    interface ExperimentCall {
        ExperimentResult run() throws Exception;
    }
}
