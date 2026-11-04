package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 단일 노드 baseline 실험을 처음부터 끝까지 실행한다: 연결 확인 → warm-up → 측정 → {@link ExperimentResult} 구성.
 *
 * <p>warm-up과 측정은 같은 per-thread seed로 시작한다. 즉 warm-up은 측정 구간과 같은 요청열을 먼저 한 번 실행해
 * JIT·connection 초기화 영향을 줄이는 의도된 설계다(측정 구간에 "이어지는" 새 요청열이 아니다).
 *
 * <p>closed-loop 부하 생성이다: thread마다 하나의 연결로 요청-응답을 순서대로 반복하므로, thread 수(concurrency)가
 * 실질적인 동시성 상한이고 고정 요청률(open-loop)은 지원하지 않는다. 이 제한은 {@link ExperimentResult}에 담지
 * 않으므로 결과를 읽는 사람이 이 문서를 함께 참고해야 한다.
 */
final class ExperimentRunner {

    /** latency percentile 근사에 쓰는 reservoir 크기. 요청 수가 더 많아도 메모리 사용량은 이 값으로 고정된다. */
    private static final int LATENCY_RESERVOIR_CAPACITY = 8192;

    /** 서버의 deadline보다 늦게 소켓을 끊어 서버가 보낸 TIMEOUT 응답을 받을 여유를 준다. */
    private static final long SOCKET_TIMEOUT_MARGIN_MS = 500;

    private final RunnerConfig config;
    private final int runNumber;
    private final Optional<RunnerInvocation> invocation;

    ExperimentRunner(RunnerConfig config) {
        this(config, 1, Optional.empty());
    }

    ExperimentRunner(RunnerConfig config, int runNumber) {
        this(config, runNumber, Optional.empty());
    }

    ExperimentRunner(RunnerConfig config, int runNumber, RunnerInvocation invocation) {
        this(config, runNumber, Optional.of(invocation));
    }

    private ExperimentRunner(RunnerConfig config, int runNumber, Optional<RunnerInvocation> invocation) {
        this.config = config;
        if (runNumber < 1 || runNumber > config.repeatCount()) {
            throw new IllegalArgumentException("runNumber must be between 1 and repeatCount: " + runNumber);
        }
        this.runNumber = runNumber;
        this.invocation = invocation;
    }

    ExperimentResult run() throws IOException, InterruptedException {
        checkConnectivity();

        long warmupStart = System.nanoTime();
        runPhase(config.warmupRequests(), null, null);
        long warmupElapsed = System.nanoTime() - warmupStart;

        ResultAccumulator accumulator = new ResultAccumulator(LATENCY_RESERVOIR_CAPACITY, config.seed());
        ResultAccumulator[] perTarget = new ResultAccumulator[config.routedTargetCount()];
        for (int t = 0; t < perTarget.length; t++) {
            perTarget[t] = new ResultAccumulator(LATENCY_RESERVOIR_CAPACITY, config.seed() + t);
        }
        long measuredStart = System.nanoTime();
        runPhase(config.measuredRequests(), accumulator, perTarget);
        long measuredElapsed = System.nanoTime() - measuredStart;

        return buildResult(accumulator, perTarget, warmupElapsed, measuredElapsed);
    }

    /** 요청을 받을 서버가 모두 살아 있는지 PING으로 먼저 확인한다. 실패하면 thread pool을 띄우지 않고 바로 알린다. */
    private void checkConnectivity() throws IOException {
        for (int t = 0; t < config.routedTargetCount(); t++) {
            RunnerConfig.Target target = config.targets().get(t);
            try (WireClient client = new WireClient(target.host(), target.port(), config.requestTimeoutMs())) {
                client.ping();
            }
        }
    }

    private void runPhase(long totalRequests, ResultAccumulator accumulator, ResultAccumulator[] perTarget)
            throws InterruptedException {
        if (totalRequests == 0) {
            return;
        }
        int concurrency = config.concurrency();
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        PhaseControl control = new PhaseControl();
        List<Future<?>> futures = new ArrayList<>();
        try {
            long share = totalRequests / concurrency;
            long remainder = totalRequests % concurrency;
            for (int i = 0; i < concurrency; i++) {
                long threadRequests = share + (i < remainder ? 1 : 0);
                long threadSeed = config.seed() + i;
                int threadIndex = i;
                futures.add(
                        pool.submit(
                                () ->
                                        runOnThread(
                                                threadIndex,
                                                threadRequests,
                                                threadSeed,
                                                accumulator,
                                                perTarget,
                                                control)));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (ExecutionException e) {
            stopWorkers(pool, futures, control);
            throw new RuntimeException("worker thread failed", e.getCause());
        } catch (InterruptedException e) {
            stopWorkers(pool, futures, control);
            throw e;
        } finally {
            if (!pool.isShutdown()) {
                pool.shutdown();
            }
            awaitWorkers(pool);
        }
    }

    private void runOnThread(
            int threadIndex,
            long requestCount,
            long threadSeed,
            ResultAccumulator accumulator,
            ResultAccumulator[] perTarget,
            PhaseControl control) {
        WorkloadGenerator generator = new WorkloadGenerator(config, new Random(threadSeed));
        long clientId = 1000 + threadIndex;
        long operationId = 0;
        // 서버마다 연결 하나. 처음 그 서버로 보낼 때 연다.
        WireClient[] clients = new WireClient[config.routedTargetCount()];
        try {
            for (long i = 0; i < requestCount && !control.stopRequested(); i++) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                Command command = generator.next();
                operationId++;
                int t = config.targetIndex(KeyHash.of(keyOf(command)));
                if (clients[t] == null) {
                    RunnerConfig.Target target = config.targets().get(t);
                    try {
                        clients[t] =
                                new WireClient(
                                        target.host(),
                                        target.port(),
                                        config.requestTimeoutMs() + SOCKET_TIMEOUT_MARGIN_MS);
                        control.register(clients[t]);
                    } catch (IOException e) {
                        if (accumulator != null && !control.stopRequested()) {
                            accumulator.recordFailure(Outcome.CLIENT_FAILURE);
                            perTarget[t].recordFailure(Outcome.CLIENT_FAILURE);
                        }
                        continue;
                    }
                }
                long startNanos = System.nanoTime();
                try {
                    WireClient.Response response =
                            clients[t].send(command, clientId, operationId, 0, config.requestTimeoutMs());
                    long elapsedNanos = System.nanoTime() - startNanos;
                    if (accumulator != null) {
                        Outcome outcome = outcomeOf(command, response.status());
                        accumulator.recordCompleted(outcome, elapsedNanos);
                        perTarget[t].recordCompleted(outcome, elapsedNanos);
                    }
                } catch (IOException e) {
                    if (accumulator != null && !control.stopRequested()) {
                        accumulator.recordFailure(Outcome.CLIENT_FAILURE);
                        perTarget[t].recordFailure(Outcome.CLIENT_FAILURE);
                    }
                    clients[t].close();
                    control.unregister(clients[t]);
                    clients[t] = null;
                }
            }
        } finally {
            for (WireClient client : clients) {
                if (client != null) {
                    control.unregister(client);
                    client.close();
                }
            }
        }
    }

    private static Bytes keyOf(Command command) {
        return switch (command) {
            case Command.Get get -> get.key();
            case Command.Set set -> set.key();
            case Command.Delete delete -> delete.key();
            case Command.Exists exists -> exists.key();
            case Command.Count count -> throw new IllegalArgumentException("workload does not generate COUNT");
        };
    }

    private static void stopWorkers(ExecutorService pool, List<Future<?>> futures, PhaseControl control) {
        control.stop();
        for (Future<?> future : futures) {
            future.cancel(true);
        }
        pool.shutdownNow();
    }

    private static void awaitWorkers(ExecutorService pool) throws InterruptedException {
        boolean interrupted = false;
        while (!pool.isTerminated()) {
            try {
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class PhaseControl {
        private final java.util.Set<WireClient> clients = ConcurrentHashMap.newKeySet();
        private volatile boolean stopRequested;

        boolean stopRequested() {
            return stopRequested;
        }

        void register(WireClient client) {
            clients.add(client);
            if (stopRequested) {
                client.close();
            }
        }

        void unregister(WireClient client) {
            clients.remove(client);
        }

        void stop() {
            stopRequested = true;
            for (WireClient client : clients) {
                client.close();
            }
        }
    }

    private static Outcome outcomeOf(Command command, int status) {
        return switch (status) {
            case WireProtocol.STATUS_OK -> command instanceof Command.Get ? Outcome.HIT : Outcome.WRITE_OK;
            case WireProtocol.STATUS_MISS -> Outcome.MISS;
            case WireProtocol.STATUS_BUSY -> Outcome.BUSY;
            case WireProtocol.STATUS_TIMEOUT -> Outcome.TIMEOUT;
            case WireProtocol.STATUS_UNAVAILABLE -> Outcome.UNAVAILABLE;
            default -> Outcome.SERVER_ERROR;
        };
    }

    private ExperimentResult buildResult(
            ResultAccumulator accumulator,
            ResultAccumulator[] perTarget,
            long warmupElapsedNanos,
            long measuredElapsedNanos) {
        GitInfo git = GitInfo.capture();
        Runtime runtime = Runtime.getRuntime();
        double seconds = measuredElapsedNanos / 1_000_000_000.0;
        double throughput = seconds > 0 ? config.measuredRequests() / seconds : 0.0;

        Map<Outcome, Long> outcomeCounts = counts(accumulator);
        List<TargetResult> targets = new ArrayList<>();
        for (int t = 0; t < perTarget.length; t++) {
            long start = t == 0 ? 0 : config.routingBoundaries().get(t - 1);
            String end = t == perTarget.length - 1 ? "2^64" : Long.toUnsignedString(config.routingBoundaries().get(t));
            targets.add(
                    new TargetResult(
                            config.targets().get(t).toString(),
                            "[" + Long.toUnsignedString(start) + ", " + end + ")",
                            perTarget[t].totalRequests(),
                            perTarget[t].p50Nanos(),
                            perTarget[t].p99Nanos(),
                            counts(perTarget[t])));
        }

        return new ExperimentResult(
                UUID.randomUUID().toString(),
                "experiment-result-v1",
                Instant.now().toEpochMilli(),
                config.study(),
                config.variant(),
                config.scenario(),
                config.seed(),
                runNumber,
                config.repeatCount(),
                config.configSnapshot(),
                invocation,
                perTarget.length, // nodeCount: 요청을 받은 서버 수
                config.concurrency(),
                config.serverWorkerCountHint(),
                config.serverStorageNote(),
                System.getProperty("java.vendor", "unknown"),
                System.getProperty("java.version", "unknown"),
                ManagementFactory.getRuntimeMXBean().getInputArguments(),
                config.serverJvmOptsNote(),
                System.getProperty("os.name", "unknown"),
                System.getProperty("os.arch", "unknown"),
                runtime.availableProcessors(),
                config.warmupRequests(),
                warmupElapsedNanos,
                config.measuredRequests(),
                measuredElapsedNanos,
                throughput,
                accumulator.p50Nanos(),
                accumulator.p99Nanos(),
                accumulator.latencySampleCount(),
                accumulator.latencyTotalObserved(),
                outcomeCounts,
                "none",
                git.commit(),
                List.copyOf(targets));
    }

    private static Map<Outcome, Long> counts(ResultAccumulator accumulator) {
        Map<Outcome, Long> counts = new EnumMap<>(Outcome.class);
        for (Outcome outcome : Outcome.values()) {
            counts.put(outcome, accumulator.count(outcome));
        }
        return counts;
    }
}
