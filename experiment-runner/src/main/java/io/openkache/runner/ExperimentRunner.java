package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.function.LongToIntFunction;
import java.util.function.Supplier;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

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
        if (config.windowed().isPresent()) {
            return runWindowed(config.windowed().get());
        }
        checkConnectivity(config.routedTargetCount());
        KeySpace keys = KeySpace.of(config);
        LongToIntFunction router = config::targetIndex;

        long warmupStart = System.nanoTime();
        runPhase(split(config.warmupRequests()), generators(keys, config.seed(), 0), router, null, null, null, null);
        long warmupElapsed = System.nanoTime() - warmupStart;

        ResultAccumulator accumulator = new ResultAccumulator(LATENCY_RESERVOIR_CAPACITY, config.seed());
        ResultAccumulator[] perTarget = accumulators(config.routedTargetCount());
        long measuredStart = System.nanoTime();
        runPhase(split(config.measuredRequests()), generators(keys, config.seed(), 0), router, accumulator, perTarget, null, null);
        long measuredElapsed = System.nanoTime() - measuredStart;

        List<TargetResult> targets = new ArrayList<>();
        for (int t = 0; t < perTarget.length; t++) {
            long start = t == 0 ? 0 : config.routingBoundaries().get(t - 1);
            String end = t == perTarget.length - 1 ? "2^64" : Long.toUnsignedString(config.routingBoundaries().get(t));
            targets.add(targetResult(t, "[" + Long.toUnsignedString(start) + ", " + end + ")", perTarget[t]));
        }
        return buildResult(accumulator, targets, List.of(), config.measuredRequests(), warmupElapsed, measuredElapsed);
    }

    /**
     * window 단위 실행: (미리 채우기) → warm-up(window 0) → window마다 그 window의 라우팅·부하로 요청을 보내고 서버별 결과와 서버
     * CPU를 기록한다. window {@code w}의 thread {@code i} seed는 {@code seed + i + w × 1,000,003}이다(계획 생성 도구와 같아야 한다).
     */
    private ExperimentResult runWindowed(RunnerConfig.Windowed windowed) throws IOException, InterruptedException {
        Schedule schedule = windowed.schedule();
        int targetCount = config.targets().size();
        checkConnectivity(targetCount);
        KeySpace keys = KeySpace.of(config);

        long warmupStart = System.nanoTime();
        if (windowed.prefill()) {
            runPhase(prefillCounts(), this::prefillSource, h -> schedule.target(0, h), null, null, null, null);
        }
        runPhase(split(config.warmupRequests()), generators(keys, config.seed(), 0), h -> schedule.target(0, h), null, null, null, null);
        long warmupElapsed = System.nanoTime() - warmupStart;

        ResultAccumulator accumulator = new ResultAccumulator(LATENCY_RESERVOIR_CAPACITY, config.seed());
        ResultAccumulator[] perTargetTotal = accumulators(targetCount);
        List<WindowResult> windows = new ArrayList<>();
        long sent = 0;
        long measuredElapsed = 0;
        for (int w = 0; w < schedule.windows(); w++) {
            int window = w;
            long requests = WorkloadGenerator.requestsIn(config, windowed.windowRequests(), w);
            ResultAccumulator[] perTarget = accumulators(targetCount);
            LongAdder[] refills = windowed.refillOnMiss() ? adders(targetCount) : null;
            double[] cpuBefore = cpuSeconds(windowed.metricsPort());
            long started = System.nanoTime();
            runPhase(split(requests), generators(keys, config.seed() + w * WINDOW_SEED_STRIDE, w),
                    h -> schedule.target(window, h), accumulator, perTarget, perTargetTotal, refills);
            long elapsed = System.nanoTime() - started;
            double[] cpuAfter = cpuSeconds(windowed.metricsPort());
            for (int t = 0; t < targetCount; t++) {
                windows.add(new WindowResult(
                        w, config.targets().get(t).toString(), WorkloadGenerator.isBurst(config, w), schedule.movesAt(w),
                        perTarget[t].totalRequests(), counts(perTarget[t]), perTarget[t].p50Nanos(), perTarget[t].p99Nanos(),
                        elapsed, (cpuAfter[t] - cpuBefore[t]) / (elapsed / 1_000_000_000.0),
                        refills == null ? 0 : refills[t].sum()));
            }
            sent += requests;
            measuredElapsed += elapsed;
        }
        List<TargetResult> targets = new ArrayList<>();
        for (int t = 0; t < targetCount; t++) {
            targets.add(targetResult(t, "schedule", perTargetTotal[t]));
        }
        return buildResult(accumulator, targets, windows, sent, warmupElapsed, measuredElapsed);
    }

    /** window마다 thread seed를 이만큼 띄운다. 계획 생성 도구도 같은 값을 쓴다. */
    static final long WINDOW_SEED_STRIDE = 1_000_003L;

    /** 요청 {@code total}건을 thread에 나눈 수. 앞 thread가 나머지를 하나씩 더 받는다. */
    long[] split(long total) {
        long[] counts = new long[config.concurrency()];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = total / counts.length + (i < total % counts.length ? 1 : 0);
        }
        return counts;
    }

    /** thread {@code i}는 seed {@code seedBase + i}로 window {@code window}의 요청을 만든다. */
    private IntFunction<Supplier<Command>> generators(KeySpace keys, long seedBase, int window) {
        return i -> new WorkloadGenerator(config, keys, new Random(seedBase + i), window)::next;
    }

    /** 미리 채우기: thread {@code i}가 key {@code i, i + concurrency, ...}를 한 번씩 SET한다. */
    private long[] prefillCounts() {
        long[] counts = new long[config.concurrency()];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = (config.keyCount() - i + counts.length - 1) / counts.length;
        }
        return counts;
    }

    private Supplier<Command> prefillSource(int thread) {
        Random random = new Random(config.seed() ^ 0x5DEECE66DL ^ thread);
        byte[] value = new byte[config.valueSize()];
        int[] next = {thread};
        return () -> {
            random.nextBytes(value);
            Command set = new Command.Set(WorkloadGenerator.key(next[0], config.keyLength()), Bytes.copyOf(value));
            next[0] += config.concurrency();
            return set;
        };
    }

    private double[] cpuSeconds(int metricsPort) {
        double[] cpu = new double[config.targets().size()];
        for (int t = 0; t < cpu.length; t++) {
            cpu[t] = NodeMetrics.cpuSeconds(config.targets().get(t).host(), metricsPort);
        }
        return cpu;
    }

    private static LongAdder[] adders(int count) {
        LongAdder[] result = new LongAdder[count];
        for (int t = 0; t < count; t++) {
            result[t] = new LongAdder();
        }
        return result;
    }

    private ResultAccumulator[] accumulators(int count) {
        ResultAccumulator[] result = new ResultAccumulator[count];
        for (int t = 0; t < count; t++) {
            result[t] = new ResultAccumulator(LATENCY_RESERVOIR_CAPACITY, config.seed() + t);
        }
        return result;
    }

    private TargetResult targetResult(int t, String hashRange, ResultAccumulator accumulator) {
        return new TargetResult(config.targets().get(t).toString(), hashRange, accumulator.totalRequests(),
                accumulator.p50Nanos(), accumulator.p99Nanos(), counts(accumulator));
    }

    /** 요청을 받을 서버가 모두 살아 있는지 PING으로 먼저 확인한다. 실패하면 thread pool을 띄우지 않고 바로 알린다. */
    private void checkConnectivity(int targetCount) throws IOException {
        for (int t = 0; t < targetCount; t++) {
            RunnerConfig.Target target = config.targets().get(t);
            try (WireClient client = new WireClient(target.host(), target.port(), config.requestTimeoutMs())) {
                client.ping();
            }
        }
    }

    /**
     * thread {@code i}가 {@code sources.apply(i)}에서 요청 {@code counts[i]}건을 꺼내 {@code router}가 고른 서버로 보낸다.
     *
     * @param accumulator null이면 기록하지 않는다(warm-up, 미리 채우기)
     * @param perTargetTotal null이 아니면 서버별 누적에도 기록한다(window 실행)
     * @param refills null이 아니면 GET MISS 뒤에 같은 key를 SET하고(cache-aside) 서버별로 센다
     */
    private void runPhase(
            long[] counts,
            IntFunction<Supplier<Command>> sources,
            LongToIntFunction router,
            ResultAccumulator accumulator,
            ResultAccumulator[] perTarget,
            ResultAccumulator[] perTargetTotal,
            LongAdder[] refills)
            throws InterruptedException {
        if (Arrays.stream(counts).sum() == 0) {
            return;
        }
        ExecutorService pool = Executors.newFixedThreadPool(counts.length);
        PhaseControl control = new PhaseControl();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < counts.length; i++) {
                int threadIndex = i;
                Supplier<Command> source = sources.apply(i);
                futures.add(pool.submit(() -> runOnThread(
                        threadIndex, counts[threadIndex], source, router, accumulator, perTarget, perTargetTotal, refills, control)));
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
            Supplier<Command> source,
            LongToIntFunction router,
            ResultAccumulator accumulator,
            ResultAccumulator[] perTarget,
            ResultAccumulator[] perTargetTotal,
            LongAdder[] refills,
            PhaseControl control) {
        byte[] refillValue = refills == null ? null : new byte[config.valueSize()];
        Random refillRandom = refills == null ? null : new Random(config.seed() ^ threadIndex);
        long clientId = 1000 + threadIndex;
        long operationId = 0;
        // 서버마다 연결 하나. 처음 그 서버로 보낼 때 연다.
        WireClient[] clients = new WireClient[config.targets().size()];
        try {
            for (long i = 0; i < requestCount && !control.stopRequested(); i++) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                Command command = source.get();
                operationId++;
                int t = router.applyAsInt(KeyHash.of(keyOf(command)));
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
                            if (perTargetTotal != null) {
                                perTargetTotal[t].recordFailure(Outcome.CLIENT_FAILURE);
                            }
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
                        if (perTargetTotal != null) {
                            perTargetTotal[t].recordCompleted(outcome, elapsedNanos);
                        }
                    }
                    if (refills != null && command instanceof Command.Get get && response.status() == WireProtocol.STATUS_MISS) {
                        refillRandom.nextBytes(refillValue);
                        operationId++;
                        clients[t].send(new Command.Set(get.key(), Bytes.copyOf(refillValue)), clientId, operationId, 0,
                                config.requestTimeoutMs());
                        refills[t].increment();
                    }
                } catch (IOException e) {
                    if (accumulator != null && !control.stopRequested()) {
                        accumulator.recordFailure(Outcome.CLIENT_FAILURE);
                        perTarget[t].recordFailure(Outcome.CLIENT_FAILURE);
                        if (perTargetTotal != null) {
                            perTargetTotal[t].recordFailure(Outcome.CLIENT_FAILURE);
                        }
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
            List<TargetResult> targets,
            List<WindowResult> windows,
            long measuredRequests,
            long warmupElapsedNanos,
            long measuredElapsedNanos) {
        GitInfo git = GitInfo.capture();
        Runtime runtime = Runtime.getRuntime();
        double seconds = measuredElapsedNanos / 1_000_000_000.0;
        double throughput = seconds > 0 ? measuredRequests / seconds : 0.0;

        Map<Outcome, Long> outcomeCounts = counts(accumulator);

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
                targets.size(), // nodeCount: 요청을 받은 서버 수
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
                measuredRequests,
                measuredElapsedNanos,
                throughput,
                accumulator.p50Nanos(),
                accumulator.p99Nanos(),
                accumulator.latencySampleCount(),
                accumulator.latencyTotalObserved(),
                outcomeCounts,
                "none",
                git.commit(),
                List.copyOf(targets),
                List.copyOf(windows));
    }

    private static Map<Outcome, Long> counts(ResultAccumulator accumulator) {
        Map<Outcome, Long> counts = new EnumMap<>(Outcome.class);
        for (Outcome outcome : Outcome.values()) {
            counts.put(outcome, accumulator.count(outcome));
        }
        return counts;
    }
}
