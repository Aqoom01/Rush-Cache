package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * B1 라우팅 서버 클러스터 실험({@code cluster.mode=b1}): 표 조회 → (미리 채우기) → warm-up → 측정(open-loop, 이동 지시) →
 * 정합성 검사.
 *
 * <p><b>open-loop:</b> 요청 k의 예정 시각은 {@code 시작 + k / load.rate}초이고 thread {@code k mod concurrency}가 보낸다.
 * thread는 예정 시각까지 기다렸다 보내며, 앞 요청이 늦게 끝나 예정 시각을 넘겼으면 바로 보낸다. 지연은 예정 시각부터 마지막
 * 응답까지(MOVED·BUSY 재시도 포함)라서 freeze로 응답이 밀린 동안 예정됐던 요청의 대기도 지연에 들어간다(coordinated omission
 * 보정). 다만 모든 thread가 막히면 실제 송신률은 떨어진다. 그래서 송신 지연(예정 → 실제 송신)의 p99·최대를 함께 기록한다.
 *
 * <p>결과: 공통 {@link ExperimentResult}(최종 응답 노드별 targets) 외에 {@code cluster-<id>.json}(hop별 지연, BUSY·redirect,
 * 이동 기록, 정합성)과 {@code timeline-<id>.csv}(예정 시각 기준 구간별 요청·결과·지연)를 쓴다. 이동 시각은 측정 시작 기준 ms라
 * timeline과 같은 기준이다.
 */
final class ClusterRunner {

    static final int OP_MIGRATE = 10;
    static final int OP_MIGRATE_STATUS = 11;
    static final int PHASE_DONE = 4;
    static final int PHASE_FAILED = 5;
    static final long MIGRATION_WAIT_MS = 120_000;
    private static final long SOCKET_MARGIN_MS = 500;

    private final RunnerConfig config;
    private final ClusterConfig cluster;
    private final ExperimentRunner base;
    private final ConsistencyOracle oracle;
    private final List<String> violations = new ArrayList<>();
    private BalanceController balancer;
    private String schedulerResult;
    private String drainResult;

    ClusterRunner(RunnerConfig config, ClusterConfig cluster, ExperimentRunner base) {
        this.config = config;
        this.cluster = cluster;
        this.base = base;
        this.oracle = new ConsistencyOracle(config.keyCount());
    }

    /** 이동 한 건의 기록. 시각은 측정 시작 기준 ms(Runner 시계). */
    record MigrationRecord(ClusterConfig.Migration plan, long from, double sentMs, int acceptStatus, String rejection,
                           double finishedMs, int phase, String serverJson, double queuedMs, int admissionRetries) {
        MigrationRecord(ClusterConfig.Migration plan, long from, double sentMs, int acceptStatus, String rejection,
                        double finishedMs, int phase, String serverJson) {
            this(plan, from, sentMs, acceptStatus, rejection, finishedMs, phase, serverJson, -1, 0);
        }

        MigrationRecord queuedAt(double queued) {
            return new MigrationRecord(plan, from, sentMs, acceptStatus, rejection, finishedMs, phase, serverJson, queued, 0);
        }
    }

    /** thread 하나가 측정 구간에 남긴 요청 기록. 끝난 뒤 합친다. */
    /** 측정 구간 요청 기록. 요청 k는 자기 칸 k에만 쓴다(가상 thread끼리 칸을 나누므로 경합이 없다). */
    private static final class Records {
        final int size;
        final long[] scheduled;
        final long[] latency;
        final long[] lag;
        final int[] status;
        final byte[] hops;
        final byte[] node;
        final boolean[] get;
        final int[] busy;
        final int[] tablet;
        final byte[] op;

        Records(int size) {
            this.size = size;
            scheduled = new long[size];
            latency = new long[size];
            lag = new long[size];
            status = new int[size];
            hops = new byte[size];
            node = new byte[size];
            get = new boolean[size];
            busy = new int[size];
            tablet = new int[size];
            op = new byte[size];
        }

        void set(int k, long scheduledOffset, long latencyNanos, long lagNanos, ClusterClient.Result r, int tabletId,
                 byte opCode) {
            scheduled[k] = scheduledOffset;
            latency[k] = latencyNanos;
            lag[k] = lagNanos;
            status[k] = r.status();
            hops[k] = (byte) Math.min(r.hops(), 127);
            node[k] = (byte) r.node();
            get[k] = opCode == 0;
            busy[k] = r.busyRetries();
            tablet[k] = tabletId;
            op[k] = opCode;
        }
    }

    ExperimentResult run() throws IOException, InterruptedException {
        for (RunnerConfig.Target target : config.targets()) {
            try (WireClient client = new WireClient(target.host(), target.port(), config.requestTimeoutMs())) {
                client.ping();
            }
        }
        try (ClusterRouting routing = new ClusterRouting(config.targets(), cluster.peerPort(), config.requestTimeoutMs());
             ClusterClient client = new ClusterClient(routing, cluster, config.requestTimeoutMs(),
                     config.requestTimeoutMs() + SOCKET_MARGIN_MS, config.concurrency())) {
            long tableStart = routing.version();
            long warmupStart = System.nanoTime();
            if (cluster.prefill()) prefill(client);
            openLoop(routing, client, config.warmupRequests(), config.seed() + 7_919, null);
            long warmupElapsed = System.nanoTime() - warmupStart;

            long measuredStart = System.nanoTime() + 50_000_000L;
            long measuredStartEpochMs = System.currentTimeMillis() + 50;
            List<MigrationRecord> migrations = new ArrayList<>();
            boolean scheduled = cluster.scheduler().isPresent() || cluster.drain().isPresent();
            FutureTask<Void> scheduledMigration = new FutureTask<>(() -> {
                migrateScheduled(routing, measuredStart, migrations);
                return null;
            });
            Thread migrator = new Thread(scheduled ? scheduledMigration : () -> migrate(routing, measuredStart, migrations),
                    "cluster-migrator");
            int moveLimit = cluster.drain().map(d -> routing.table().size()).orElse(cluster.migrations().size());
            migrator.start();
            Thread balanceThread = null;
            if (cluster.balance().isPresent()) {
                ClusterConfig.Migration s = cluster.migrationSettings();
                balancer = new BalanceController(config, cluster.balance().get(), routing, (tablet, to, atMs) ->
                        migrateOne(routing, measuredStart, new ClusterConfig.Migration((long) atMs, tablet, to, s.method(),
                                s.rateBytesPerSec(), s.batchBytes(), s.stopThresholdBytes(), s.maxRounds())));
                BalanceController b = balancer;
                balanceThread = new Thread(() -> b.run(measuredStart), "cluster-balance");
                balanceThread.start();
            }
            List<Records> records = openLoop(routing, client, config.measuredRequests(), config.seed(), measuredStart);
            long measuredElapsed = System.nanoTime() - measuredStart;
            if (balanceThread != null) {
                balancer.stop();
                balanceThread.join();
            }
            long waitMs = MIGRATION_WAIT_MS * Math.max(1, moveLimit);
            if (scheduled) {
                long lastAt = cluster.drain().map(ClusterConfig.Drain::atMs)
                        .orElse(cluster.migrations().stream().mapToLong(ClusterConfig.Migration::atMs).max().orElse(0));
                waitMs += Math.min(Long.MAX_VALUE - waitMs, Math.max(0, lastAt - (long) ms(measuredElapsed)));
            }
            migrator.join(waitMs);
            if (migrator.isAlive()) {
                migrator.interrupt();
                migrator.join();
            }
            if (scheduled) {
                try {
                    scheduledMigration.get();
                } catch (ExecutionException e) {
                    throw new IOException("scheduled migration failed", e.getCause());
                }
            }
            routing.refreshNow();
            long[] consistency = cluster.consistencyCheck() ? check(routing, client) : null;

            ResultAccumulator all = new ResultAccumulator(8192, config.seed());
            ResultAccumulator[] perNode = new ResultAccumulator[routing.nodeCount()];
            for (int i = 0; i < perNode.length; i++) perNode[i] = new ResultAccumulator(8192, config.seed() + i);
            for (Records r : records) {
                for (int i = 0; i < r.size; i++) {
                    Outcome outcome = outcomeOf(r.status[i], r.get[i]);
                    if (outcome == Outcome.CLIENT_FAILURE) {
                        all.recordFailure(outcome);
                        perNode[r.node[i]].recordFailure(outcome);
                    } else {
                        all.recordCompleted(outcome, r.latency[i]);
                        perNode[r.node[i]].recordCompleted(outcome, r.latency[i]);
                    }
                }
            }
            List<TargetResult> targets = new ArrayList<>();
            for (int i = 0; i < perNode.length; i++) {
                ResultAccumulator a = perNode[i];
                targets.add(new TargetResult(config.targets().get(i).toString(), "b1-node-" + i + " (final responder)",
                        a.totalRequests(), a.p50Nanos(), a.p99Nanos(), ExperimentRunner.counts(a)));
            }
            ExperimentResult result = base.buildResult(all, targets, List.of(), config.measuredRequests(), warmupElapsed,
                    measuredElapsed);
            Path dir = config.resultsDir().resolve(ResultWriter.sanitize(config.study()));
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("timeline-" + result.executionId() + ".csv"), timeline(records));
            Files.writeString(dir.resolve("cluster-" + result.executionId() + ".json"),
                    clusterJson(records, migrations, consistency, tableStart, routing, measuredElapsed,
                            measuredStartEpochMs));
            return result;
        }
    }

    // ------------------------------------------------------------------ 부하

    /** 모든 key를 한 번씩 SET한다(closed-loop, thread i는 key i, i+N, ...). */
    private void prefill(ClusterClient client) throws InterruptedException {
        parallel(i -> {
            Random random = new Random(config.seed() ^ 0x5DEECE66DL ^ i);
            byte[] value = new byte[config.valueSize()];
            for (int k = i; k < config.keyCount(); k += config.concurrency()) {
                random.nextBytes(value);
                Bytes key = WorkloadGenerator.key(k, config.keyLength());
                write(client, new Command.Set(key, Bytes.copyOf(value)), key);
            }
            return null;
        });
    }

    /**
     * {@code total}건을 load.rate 예정 시각에 보낸다. 발송 thread 하나가 요청열을 순서대로 만들고(seed 재현) 예정 시각마다 요청 하나를
     * 가상 thread로 띄운다. 그래서 freeze로 오래 기다리는 요청이 있어도 다음 요청은 제때 나간다. {@code start}가 null이면 지금부터
     * 보내고 기록하지 않는다(warm-up).
     */
    private List<Records> openLoop(ClusterRouting routing, ClusterClient client, long total, long seed, Long start)
            throws InterruptedException {
        if (total == 0) return List.of();
        if (total > Integer.MAX_VALUE) throw new IllegalArgumentException("too many requests: " + total);
        boolean record = start != null;
        long origin = record ? start : System.nanoTime() + 20_000_000L;
        double intervalNanos = 1_000_000_000.0 / cluster.loadRate();
        WorkloadGenerator generator = new WorkloadGenerator(config, KeySpace.of(config), new Random(seed), 0);
        Records records = record ? new Records((int) total) : null;
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch((int) total);
        java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        for (int k = 0; k < total; k++) {
            long scheduled = origin + (long) (k * intervalNanos);
            Command command = generator.next();
            long now;
            while ((now = System.nanoTime()) < scheduled) LockSupport.parkNanos(scheduled - now);
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            int slot = k;
            Thread.ofVirtual().start(() -> {
                try {
                    Bytes key = keyOf(command);
                    long hash = KeyHash.of(key);
                    int tablet = (int) routing.table().lookup(hash).id().value();
                    ClusterClient.Result r = command instanceof Command.Get ? client.send(command, key) : write(client, command, key);
                    long end = System.nanoTime();
                    if (record && balancer != null) balancer.observe(hash, !(command instanceof Command.Get));
                    if (record) {
                        byte op = (byte) (command instanceof Command.Get ? 0 : command instanceof Command.Set ? 1 : 2);
                        records.set(slot, scheduled - origin, end - scheduled, r.firstSendNanos() - scheduled, r, tablet, op);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            });
        }
        done.await();
        if (failure.get() != null) throw new RuntimeException("cluster request failed", failure.get());
        return record ? List.of(records) : List.of();
    }

    /** 같은 key의 쓰기는 하나씩 보내고 결과를 oracle에 남긴다. */
    private ClusterClient.Result write(ClusterClient client, Command command, Bytes key) {
        int index = ConsistencyOracle.indexOf(key);
        ReentrantLock lock = oracle.lock(index);
        lock.lock();
        try {
            ClusterClient.Result r = client.send(command, key);
            if (oracle.state(index) == ConsistencyOracle.UNCERTAIN) return r;
            if (r.status() == ClusterClient.IO_FAILURE) {
                oracle.uncertain(index);
            } else if (command instanceof Command.Set set && r.status() == WireProtocol.STATUS_OK) {
                oracle.set(index, set.value());
            } else if (command instanceof Command.Delete
                    && (r.status() == WireProtocol.STATUS_OK || r.status() == WireProtocol.STATUS_MISS)) {
                oracle.delete(index);
            }
            return r;
        } finally {
            lock.unlock();
        }
    }

    @FunctionalInterface
    private interface Task<T> {
        T run(int thread) throws Exception;
    }

    private <T> List<T> parallel(Task<T> task) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(config.concurrency());
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < config.concurrency(); i++) {
                int thread = i;
                futures.add(pool.submit(() -> task.run(thread)));
            }
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) results.add(f.get());
            return results;
        } catch (ExecutionException e) {
            throw new RuntimeException("cluster worker failed", e.getCause());
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------ 이동 지시

    private void migrate(ClusterRouting routing, long measuredStart, List<MigrationRecord> out) {
        for (ClusterConfig.Migration m : cluster.migrations()) {
            long at = measuredStart + m.atMs() * 1_000_000L;
            long now;
            while ((now = System.nanoTime()) < at) {
                LockSupport.parkNanos(at - now);
                if (Thread.interrupted()) return;
            }
            MigrationRecord record;
            try {
                record = migrateOne(routing, measuredStart, m);
            } catch (IOException | RuntimeException e) {
                record = new MigrationRecord(m, -1, ms(System.nanoTime() - measuredStart), -1, e.toString(), -1, -1, "null");
            }
            synchronized (out) {
                out.add(record);
            }
            System.out.printf(Locale.ROOT, "migration tablet=%d %d->%d %s: phase=%d sent=%.1fms finished=%.1fms %s%n",
                    m.tablet(), record.from(), m.to(), m.method().label(), record.phase(), record.sentMs(),
                    record.finishedMs(), record.rejection());
        }
    }

    MigrationRecord migrateOne(ClusterRouting routing, long measuredStart, ClusterConfig.Migration m)
            throws IOException {
        RoutingTable table = routing.refreshNow();
        TabletId id = new TabletId(m.tablet());
        if (!table.contains(id)) {
            return new MigrationRecord(m, -1, ms(System.nanoTime() - measuredStart), -1, "unknown tablet", -1, -1, "null");
        }
        int from = (int) table.get(id).owner().value();
        RunnerConfig.Target source = routing.node(from);
        byte[] body = migrationBody(m);
        try (WireClient peer = new WireClient(source.host(), cluster.peerPort(), config.requestTimeoutMs())) {
            double sentMs = ms(System.nanoTime() - measuredStart);
            WireClient.Response accepted = peer.peer(OP_MIGRATE, body);
            // Controller가 막 나눈 Tablet은 source가 새 표를 받기 전이면 모른다고 답한다. 잠깐 기다렸다 다시 보낸다
            for (int retry = 0; retry < 100 && accepted.status() == WireProtocol.STATUS_BAD_REQUEST
                    && accepted.detail().startsWith("unknown tablet"); retry++) {
                LockSupport.parkNanos(10_000_000);
                accepted = peer.peer(OP_MIGRATE, body);
            }
            if (accepted.status() != WireProtocol.STATUS_OK) {
                return new MigrationRecord(m, from, sentMs, accepted.status(), accepted.detail(), -1, -1, "null");
            }
            long jobId = ByteBuffer.wrap(accepted.value()).getLong();
            long deadline = System.nanoTime() + MIGRATION_WAIT_MS * 1_000_000L;
            byte[] query = ByteBuffer.allocate(8).putLong(jobId).array();
            while (true) {
                WireClient.Response status = peer.peer(OP_MIGRATE_STATUS, query);
                int phase = status.value().length == 1 ? status.value()[0] : -1;
                if (status.status() != WireProtocol.STATUS_OK || phase == PHASE_DONE || phase == PHASE_FAILED
                        || System.nanoTime() > deadline || Thread.currentThread().isInterrupted()) {
                    double finishedMs = ms(System.nanoTime() - measuredStart);
                    routing.refreshNow();
                    String json = status.detail().startsWith("{") ? status.detail() : "null";
                    return new MigrationRecord(m, from, sentMs, 0, phase == PHASE_DONE ? "" : "not done",
                            finishedMs, phase, json);
                }
                LockSupport.parkNanos(5_000_000);
            }
        }
    }

    static byte[] migrationBody(ClusterConfig.Migration m) {
        return ByteBuffer.allocate(41).put((byte) m.method().ordinal()).putLong(m.tablet()).putLong(m.to())
                .putLong(m.rateBytesPerSec()).putInt(m.batchBytes()).putLong(m.stopThresholdBytes())
                .putInt(m.maxRounds()).array();
    }

    private void migrateScheduled(ClusterRouting routing, long measuredStart, List<MigrationRecord> out)
            throws IOException {
        ClusterConfig.Drain drain = cluster.drain().orElse(null);
        List<ClusterConfig.Migration> plans = cluster.migrations();
        long firstAt = drain == null ? (plans.isEmpty() ? 0 : plans.getFirst().atMs()) : drain.atMs();
        double now;
        while ((now = ms(System.nanoTime() - measuredStart)) < firstAt) {
            LockSupport.parkNanos((long) ((firstAt - now) * 1_000_000));
            if (Thread.currentThread().isInterrupted()) return;
        }

        RoutingTable table = routing.refreshNow();
        if (drain != null) {
            NodeId leaving = new NodeId(drain.node());
            // B1 uses HashRangeScheme. onNodeRemoved uses only the supplied table, not the initial tablet count.
            plans = table.nodes().contains(leaving) ? new HashRangeScheme(HashRangeScheme.DEFAULT_TABLETS_PER_NODE)
                    .onNodeRemoved(table, leaving).moves().stream().map(c -> drainMove(drain, c)).toList() : List.of();
        }

        if (cluster.scheduler().isPresent()) {
            runAdmissions(routing, measuredStart, table, plans, out);
        } else {
            double queued = ms(System.nanoTime() - measuredStart);
            for (ClusterConfig.Migration plan : plans) {
                if (Thread.currentThread().isInterrupted()) break;
                MigrationRecord record;
                try {
                    record = migrateOne(routing, measuredStart, plan);
                } catch (IOException e) {
                    record = new MigrationRecord(plan, drain.node(), -1, -1, e.toString(), -1, -1, "null");
                }
                out.add(record.queuedAt(queued));
            }
            List<Long> admissionOrder = out.stream().filter(m -> m.acceptStatus() == WireProtocol.STATUS_OK)
                    .map(m -> m.plan().tablet()).toList();
            schedulerResult = schedulerJson("sequential", 1, 1, "not-applied",
                    admissionOrder.isEmpty() ? 0 : 1, admissionOrder, out);
        }

        if (drain != null) {
            long remaining = routing.refreshNow().tablets().stream().filter(t -> t.owner().value() == drain.node()).count();
            boolean completed = remaining == 0 && out.size() == plans.size()
                    && out.stream().allMatch(m -> m.phase() == PHASE_DONE);
            drainResult = String.format(Locale.ROOT, """
                    {"node": %d, "atMs": %d, "planner": "onNodeRemoved", "plannedMoves": %d,                     "completedMs": %s, "remainingTablets": %d, "nodeRemoved": false}""",
                    drain.node(), drain.atMs(), plans.size(),
                    completed ? String.format(Locale.ROOT, "%.3f", ms(System.nanoTime() - measuredStart)) : "null", remaining);
        }
    }

    private ClusterConfig.Migration drainMove(ClusterConfig.Drain drain, Change change) {
        ClusterConfig.Migration m = cluster.migrationSettings();
        return new ClusterConfig.Migration(drain.atMs(), change.unit().value(), change.to().value(), m.method(),
                m.rateBytesPerSec(), m.batchBytes(), m.stopThresholdBytes(), m.maxRounds());
    }

    private void runAdmissions(ClusterRouting routing, long measuredStart, RoutingTable table,
                               List<ClusterConfig.Migration> plans, List<MigrationRecord> out) throws IOException {
        ClusterConfig.Scheduler settings = cluster.scheduler().orElseThrow();
        List<MigrationAdmission.Job> jobs = new ArrayList<>();
        Map<TabletId, MigrationAdmission.Job> lastMove = new HashMap<>();
        for (ClusterConfig.Migration plan : plans) {
            TabletId id = new TabletId(plan.tablet());
            Tablet tablet = table.contains(id) ? table.get(id) : null;
            MigrationAdmission.Job previous = lastMove.get(id);
            NodeId from = previous == null ? (tablet == null ? null : tablet.owner()) : previous.change.to();
            NodeId to = new NodeId(plan.to());
            if (from == null || from.equals(to) || to.value() >= routing.nodeCount()) {
                out.add(new MigrationRecord(plan, from == null ? -1 : from.value(), -1, -1,
                        from == null ? "unknown tablet" : "invalid destination", -1, -1, "null",
                        -1, 0));
                continue;
            }
            MigrationAdmission.Job job = new MigrationAdmission.Job(plan, new Change(id, from, to),
                    BalanceController.estimatedBytes(config, tablet), previous);
            jobs.add(job);
            lastMove.put(id, job);
        }

        Map<Long, WireClient> peers = new HashMap<>();
        MigrationAdmission admission = new MigrationAdmission(settings, jobs, cluster.migrationSettings().rateBytesPerSec(),
                (node, opcode, body) -> {
                    try {
                        WireClient peer = peers.get(node);
                        if (peer == null) {
                            peer = new WireClient(routing.node(Math.toIntExact(node)).host(), cluster.peerPort(),
                                    config.requestTimeoutMs());
                            peers.put(node, peer);
                        }
                        return peer.peer(opcode, body);
                    } catch (IOException e) {
                        WireClient failed = peers.remove(node);
                        if (failed != null) failed.close();
                        throw e;
                    }
                }, () -> ms(System.nanoTime() - measuredStart));
        long tick = 0;
        double deadline = plans.stream().mapToLong(ClusterConfig.Migration::atMs).max().orElse(0)
                + (double) MIGRATION_WAIT_MS * Math.max(1, plans.size());
        try {
            while (!Thread.currentThread().isInterrupted() && ms(System.nanoTime() - measuredStart) < deadline
                    && admission.step(tick)) {
                tick++;
                LockSupport.parkNanos(MigrationAdmission.POLL_MS * 1_000_000);
            }
            admission.stop(tick, "scheduler stopped before terminal status");
        } finally {
            for (WireClient peer : peers.values()) peer.close();
        }
        jobs.stream().map(MigrationAdmission.Job::record).forEach(out::add);

        schedulerResult = schedulerJson(settings.policy(), settings.maxConcurrent(), settings.maxPerNode(), "poll-loop",
                admission.maxObservedConcurrent, admission.admissionOrder, out);
    }

    private static String schedulerJson(String policy, int maxConcurrent, int maxPerNode, String tickUnit,
                                        int maxObservedConcurrent, List<Long> admissionOrder, List<MigrationRecord> out) {
        List<MigrationRecord> completed = out.stream().filter(m -> m.phase() == PHASE_DONE).toList();
        double firstSent = out.stream().filter(m -> m.acceptStatus() == WireProtocol.STATUS_OK)
                .mapToDouble(MigrationRecord::sentMs).min().orElse(0);
        double lastCompleted = completed.stream().mapToDouble(MigrationRecord::finishedMs).max().orElse(firstSent);
        double meanCompletion = completed.stream().mapToDouble(m -> m.finishedMs() - m.queuedMs()).average().orElse(0);
        return String.format(Locale.ROOT,
                "{\"policy\": \"%s\", \"maxConcurrent\": %d, \"maxPerNode\": %d, \"allocation\": \"not-applied\", "
                        + "\"tickUnit\": \"%s\", \"makespanMs\": %.3f, \"meanJobCompletionMs\": %.3f, "
                        + "\"maxObservedConcurrent\": %d, \"admissionOrder\": %s}",
                policy, maxConcurrent, maxPerNode, tickUnit, lastCompleted - firstSent, meanCompletion,
                maxObservedConcurrent, admissionOrder);
    }

    // ------------------------------------------------------------------ 정합성

    /**
     * 끝난 뒤 oracle에 기록된 key를 모두 GET해 대조한다. 결과: checked, lostSet(성공한 SET이 MISS), wrongValue(다른 값),
     * staleHit(성공한 DELETE 뒤 값이 보임), uncertain(제외), unverified(GET이 끝내 실패).
     */
    private long[] check(ClusterRouting routing, ClusterClient client) throws InterruptedException {
        List<long[]> parts = parallel(i -> {
            long[] c = new long[6];
            {
                for (int k = i; k < oracle.keyCount(); k += config.concurrency()) {
                    byte state = oracle.state(k);
                    if (state == ConsistencyOracle.UNKNOWN) continue;
                    if (state == ConsistencyOracle.UNCERTAIN) {
                        c[4]++;
                        continue;
                    }
                    c[0]++;
                    Bytes key = WorkloadGenerator.key(k, config.keyLength());
                    ClusterClient.Result r = client.send(new Command.Get(key), key);
                    String violation = null;
                    if (r.status() == WireProtocol.STATUS_OK) {
                        if (state == ConsistencyOracle.DELETED) {
                            c[3]++;
                            violation = "staleHit";
                        } else if (!oracle.matches(k, r.value())) {
                            c[2]++;
                            violation = "wrongValue";
                        }
                    } else if (r.status() == WireProtocol.STATUS_MISS) {
                        if (state == ConsistencyOracle.PRESENT) {
                            c[1]++;
                            violation = "lostSet";
                        }
                    } else {
                        c[5]++;
                    }
                    if (violation != null) {
                        synchronized (violations) {
                            if (violations.size() < 50) {
                                violations.add(String.format(Locale.ROOT, "{\"key\": %d, \"tablet\": %d, \"node\": %d, \"kind\": \"%s\"}",
                                        k, routing.table().lookup(io.openkache.core.tablet.KeyHash.of(key)).id().value(), r.node(), violation));
                            }
                        }
                    }
                }
            }
            return c;
        });
        long[] total = new long[6];
        for (long[] c : parts) for (int j = 0; j < 6; j++) total[j] += c[j];
        return total;
    }

    // ------------------------------------------------------------------ 결과

    private static Outcome outcomeOf(int status, boolean get) {
        return switch (status) {
            case WireProtocol.STATUS_OK -> get ? Outcome.HIT : Outcome.WRITE_OK;
            case WireProtocol.STATUS_MISS -> Outcome.MISS;
            case WireProtocol.STATUS_BUSY -> Outcome.BUSY;
            case WireProtocol.STATUS_TIMEOUT -> Outcome.TIMEOUT;
            case WireProtocol.STATUS_UNAVAILABLE -> Outcome.UNAVAILABLE;
            case ClusterClient.IO_FAILURE -> Outcome.CLIENT_FAILURE;
            default -> Outcome.SERVER_ERROR;
        };
    }

    private String timeline(List<Records> records) {
        long bucketNanos = cluster.bucketMs() * 1_000_000L;
        int buckets = 0;
        for (Records r : records) {
            for (int i = 0; i < r.size; i++) buckets = Math.max(buckets, (int) (r.scheduled[i] / bucketNanos) + 1);
        }
        long[][] c = new long[buckets][8];
        List<List<Long>> lat = new ArrayList<>();
        for (int b = 0; b < buckets; b++) lat.add(new ArrayList<>());
        for (Records r : records) {
            for (int i = 0; i < r.size; i++) {
                int b = (int) (r.scheduled[i] / bucketNanos);
                long[] x = c[b];
                x[0]++;
                switch (outcomeOf(r.status[i], r.get[i])) {
                    case HIT -> x[1]++;
                    case MISS -> x[2]++;
                    case WRITE_OK -> x[3]++;
                    case BUSY -> x[4]++;
                    default -> x[5]++;
                }
                if (r.hops[i] > 0) x[6]++;
                x[7] += r.busy[i];
                if (outcomeOf(r.status[i], r.get[i]) != Outcome.CLIENT_FAILURE) lat.get(b).add(r.latency[i]);
            }
        }
        StringBuilder s = new StringBuilder(
                "bucket_start_ms,requests,hit,miss,write_ok,busy_final,error,redirected,busy_retries,p50_us,p99_us,max_us\n");
        for (int b = 0; b < buckets; b++) {
            long[] sorted = lat.get(b).stream().mapToLong(Long::longValue).sorted().toArray();
            long[] x = c[b];
            s.append(b * cluster.bucketMs());
            for (long v : x) s.append(',').append(v);
            s.append(String.format(Locale.ROOT, ",%.1f,%.1f,%.1f%n", percentile(sorted, 0.50) / 1e3,
                    percentile(sorted, 0.99) / 1e3, sorted.length == 0 ? 0.0 : sorted[sorted.length - 1] / 1e3));
        }
        return s.toString();
    }

    private String clusterJson(List<Records> records, List<MigrationRecord> migrations, long[] consistency,
                               long tableStart, ClusterRouting routing, long measuredElapsed,
                               long measuredStartEpochMs) {
        List<Long> hop0 = new ArrayList<>(), hop1 = new ArrayList<>(), hop2 = new ArrayList<>(), lag = new ArrayList<>();
        long sent = 0, redirected = 0, exhausted = 0, withBusy = 0, busyRetries = 0, busyFinal = 0;
        for (Records r : records) {
            for (int i = 0; i < r.size; i++) {
                sent++;
                lag.add(r.lag[i]);
                if (r.hops[i] > 0) redirected++;
                if (r.status[i] == ClusterClient.REDIRECT_EXHAUSTED) exhausted++;
                if (r.busy[i] > 0) withBusy++;
                busyRetries += r.busy[i];
                if (r.status[i] == WireProtocol.STATUS_BUSY) busyFinal++;
                if (r.status[i] == ClusterClient.IO_FAILURE) continue;
                (r.hops[i] == 0 ? hop0 : r.hops[i] == 1 ? hop1 : hop2).add(r.latency[i]);
            }
        }
        StringBuilder s = new StringBuilder(1024);
        s.append("{\n  \"schema\": \"cluster-result-v1\",\n");
        s.append("  \"measuredStartEpochMs\": ").append(measuredStartEpochMs).append(",\n");
        s.append(String.format(Locale.ROOT, "  \"loadRatePerSec\": %.1f,%n", cluster.loadRate()));
        s.append("  \"scheduledRequests\": ").append(config.measuredRequests()).append(",\n");
        s.append("  \"completedRequests\": ").append(sent).append(",\n");
        s.append(String.format(Locale.ROOT, "  \"achievedRatePerSec\": %.1f,%n", sent / (measuredElapsed / 1e9)));
        s.append("  \"sendLag\": ").append(stats(lag)).append(",\n");
        s.append("  \"latencyByHops\": {\"0\": ").append(stats(hop0)).append(", \"1\": ").append(stats(hop1))
                .append(", \"2+\": ").append(stats(hop2)).append("},\n");
        s.append("  \"redirectedRequests\": ").append(redirected).append(",\n");
        s.append("  \"redirectExhausted\": ").append(exhausted).append(",\n");
        s.append("  \"requestsWithBusyRetry\": ").append(withBusy).append(",\n");
        s.append("  \"busyRetries\": ").append(busyRetries).append(",\n");
        s.append("  \"busyFinal\": ").append(busyFinal).append(",\n");
        s.append("  \"tableVersionStart\": ").append(tableStart).append(",\n");
        s.append("  \"tableVersionEnd\": ").append(routing.version()).append(",\n");
        s.append("  \"tableRefreshes\": ").append(routing.refreshCount()).append(",\n");
        s.append("  \"tabletLoad\": ").append(tabletLoad(records, measuredElapsed)).append(",\n");
        s.append("  \"consistency\": ");
        if (consistency == null) {
            s.append("null");
        } else {
            s.append("{\"checked\": ").append(consistency[0]).append(", \"lostSet\": ").append(consistency[1])
                    .append(", \"wrongValue\": ").append(consistency[2]).append(", \"staleHit\": ").append(consistency[3])
                    .append(", \"uncertainExcluded\": ").append(consistency[4]).append(", \"unverified\": ")
                    .append(consistency[5]).append(", \"samples\": [").append(String.join(", ", violations)).append("]}");
        }
        s.append(",\n  \"balance\": ").append(balancer == null ? "null" : balancer.json());
        if (schedulerResult != null) s.append(",\n  \"scheduler\": ").append(schedulerResult);
        if (drainResult != null) s.append(",\n  \"drain\": ").append(drainResult);
        s.append(",\n  \"migrations\": [");
        List<String> items = new ArrayList<>();
        for (MigrationRecord m : migrations) {
            ClusterConfig.Migration p = m.plan();
            items.add(String.format(Locale.ROOT,
                    "%n    {\"atMs\": %d, \"tablet\": %d, \"from\": %d, \"to\": %d, \"method\": \"%s\", "
                            + "\"rateBytesPerSec\": %d, \"sentMs\": %.3f, \"acceptStatus\": %d, \"note\": %s, "
                            + "\"finishedMs\": %.3f, \"phase\": %d, \"server\": %s%s}",
                    p.atMs(), p.tablet(), m.from(), p.to(), p.method().label(), p.rateBytesPerSec(), m.sentMs(),
                    m.acceptStatus(), quote(m.rejection()), m.finishedMs(), m.phase(), m.serverJson(),
                    cluster.scheduler().isEmpty() && cluster.drain().isEmpty() ? "" : String.format(Locale.ROOT,
                            ", \"queuedMs\": %.3f, \"admissionRetries\": %d", m.queuedMs(), m.admissionRetries())));
        }
        s.append(String.join(",", items)).append(items.isEmpty() ? "]\n}\n" : "\n  ]\n}\n");
        return s.toString();
    }

    /**
     * 요청을 보낼 때 Runner 표 기준 Tablet별 요청 수·쓰기와 GET/쓰기 지연. setBytesPerSec = SET 수 × value.size ÷ 측정 시간(rho 분자).
     */
    private String tabletLoad(List<Records> records, long measuredElapsed) {
        java.util.TreeMap<Integer, long[]> load = new java.util.TreeMap<>();
        java.util.Map<Integer, List<Long>> getLatency = new java.util.HashMap<>();
        java.util.Map<Integer, List<Long>> writeLatency = new java.util.HashMap<>();
        for (Records r : records) {
            for (int i = 0; i < r.size; i++) {
                load.computeIfAbsent(r.tablet[i], k -> new long[3])[r.op[i]]++;
                if (r.status[i] == ClusterClient.IO_FAILURE) continue;
                (r.op[i] == 0 ? getLatency : writeLatency).computeIfAbsent(r.tablet[i], k -> new ArrayList<>()).add(r.latency[i]);
            }
        }
        List<String> items = new ArrayList<>();
        double seconds = measuredElapsed / 1e9;
        load.forEach((id, c) -> items.add(String.format(Locale.ROOT,
                "%n    {\"tablet\": %d, \"gets\": %d, \"sets\": %d, \"deletes\": %d, \"setBytesPerSec\": %.0f, "
                        + "\"get\": %s, \"write\": %s}",
                id, c[0], c[1], c[2], c[1] * (double) config.valueSize() / seconds,
                stats(getLatency.getOrDefault(id, List.of())), stats(writeLatency.getOrDefault(id, List.of())))));
        return "[" + String.join(",", items) + "\n  ]";
    }

    private static String stats(List<Long> values) {
        long[] sorted = values.stream().mapToLong(Long::longValue).sorted().toArray();
        return String.format(Locale.ROOT, "{\"count\": %d, \"p50Us\": %.1f, \"p99Us\": %.1f, \"p999Us\": %.1f, \"maxUs\": %.1f}",
                sorted.length, percentile(sorted, 0.50) / 1e3, percentile(sorted, 0.99) / 1e3,
                percentile(sorted, 0.999) / 1e3, sorted.length == 0 ? 0.0 : sorted[sorted.length - 1] / 1e3);
    }

    /** nearest-rank percentile. 빈 배열이면 0. */
    static long percentile(long[] sorted, double q) {
        if (sorted.length == 0) return 0;
        int rank = (int) Math.ceil(q * sorted.length);
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank - 1))];
    }

    private static Bytes keyOf(Command command) {
        return switch (command) {
            case Command.Get g -> g.key();
            case Command.Set s -> s.key();
            case Command.Delete d -> d.key();
            default -> throw new IllegalArgumentException("unsupported command " + command);
        };
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }

    private static String quote(String text) {
        return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ") + '"';
    }
}
