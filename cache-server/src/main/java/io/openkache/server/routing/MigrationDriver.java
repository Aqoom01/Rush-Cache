package io.openkache.server.routing;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.MigrationProtocol;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.routing.Controller;
import io.openkache.core.tablet.routing.LocalRouting;
import io.openkache.server.netty.Protocol;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * source 노드에서 B3 이동 job을 실제로 진행한다. 1ms마다 worker에 tick을 넣고, tick이 protocol의 {@code step(예산)}을 부른다.
 * 예산은 설정한 대역(bytes/s) × 경과 시간이다.
 *
 * <p>commit 순서(fencing): protocol이 commit 준비가 되면 (1) {@link LocalRouting#stopServing}으로 이 Tablet의 모든 요청을
 * destination으로 MOVED 시키고 (2) Controller에 MOVE(expectedVersion CAS)를 보낸 뒤 (3) 새 표를 이 노드가 받을 때까지 기다렸다가
 * (4) source 범위를 지우고 freeze를 푼다. 그래서 commit 이후 옛 주인이 쓰기를 받는 틈이 없다.
 * CAS 응답이 오지 않으면(시간 초과) commit 여부를 알 수 없으므로 거절·freeze를 유지한 채 FAILED로 둔다.
 *
 * <p>job 상태는 worker thread에서만 바꾼다. 상태 조회도 worker 작업으로 처리한다.
 */
final class MigrationDriver implements AutoCloseable {

    private static final int MAX_COMMIT_RETRIES = 16;
    private static final int MAX_TABLE_REQUESTS = 16;
    private static final long TABLE_REQUEST_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    enum Phase { RUNNING, COMMITTING, AWAIT_TABLE, CLEANING, DONE, FAILED }

    private final NodeId self;
    private final NodeId controllerId;
    private final NodeStore store;
    private final LocalRouting routing;
    private final Controller controller;
    private final PeerRequests caller;
    private final OwnerWorker worker;
    private final Map<Long, Job> jobs = new LinkedHashMap<>();
    private final ScheduledExecutorService ticker;
    private final AtomicBoolean tickQueued = new AtomicBoolean();
    private volatile boolean active;
    private long nextJob = 1;

    MigrationDriver(NodeId self, NodeId controllerId, NodeStore store, LocalRouting routing, Controller controller,
                    PeerRequests caller, OwnerWorker worker) {
        this.self = self;
        this.controllerId = controllerId;
        this.store = store;
        this.routing = routing;
        this.controller = controller;
        this.caller = caller;
        this.worker = worker;
        this.ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openkache-migration-tick");
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(this::scheduleTick, 1, 1, TimeUnit.MILLISECONDS);
    }

    /** tick이 이미 대기 중이면 더 넣지 않는다(mailbox를 tick으로 채우지 않게). */
    private void scheduleTick() {
        if (!active || !tickQueued.compareAndSet(false, true)) return;
        if (!worker.offer(() -> {
            tickQueued.set(false);
            tick();
        })) {
            tickQueued.set(false);
        }
    }

    /** 이 Tablet의 정리를 이동 job이 맡고 있으면 true(표 변경 시 일반 정리를 건너뛰기 위해). worker thread 전용. */
    boolean cleansUp(TabletId tablet) {
        for (Job j : jobs.values()) {
            if (j.tablet.id().equals(tablet) && j.phase != Phase.DONE && j.phase != Phase.FAILED) return true;
        }
        return false;
    }

    /** MIGRATE 요청을 받아 job을 시작한다. worker thread 전용. 거절하면 이유를 돌려준다. */
    Result start(MigrationWire.Migrate m) {
        RoutingTable table = routing.table();
        TabletId id = new TabletId(m.tabletId());
        if (!table.contains(id)) return Result.rejected("unknown tablet " + m.tabletId());
        Tablet tablet = table.get(id);
        NodeId to = new NodeId(m.to());
        if (!tablet.owner().equals(self)) return Result.rejected("tablet owned by " + tablet.owner());
        if (to.equals(self) || !caller.knows(to)) {
            return Result.rejected("invalid destination " + m.to());
        }
        if (cleansUp(id) || routing.isRefusing(id)) return Result.busy("tablet already moving");
        long jobId = nextJob++;
        Change change = new Change(id, self, to);
        RemoteDestination destination = new RemoteDestination(caller, self, to, jobId);
        MigrationProtocol protocol = switch (m.method()) {
            case STOP_AND_COPY -> new StopAndCopyProtocol(store, destination, table, m.batchBytes());
            case PRE_COPY -> new PreCopyProtocol(store, destination, table,
                    new PreCopyProtocol.Settings(m.batchBytes(), m.stopThresholdBytes(), m.maxRounds()));
            case CUTOVER -> null;
        };
        Job job = new Job(jobId, m, change, tablet, protocol, destination);
        jobs.put(jobId, job);
        job.startNanos = System.nanoTime();
        job.startEpochMs = System.currentTimeMillis();
        job.lastTickNanos = job.startNanos;
        active = true;
        if (protocol == null) {
            job.begun = true;
            beginCommit(job);
            return Result.started(jobId);
        }
        // snapshot은 worker를 막지 않게 background에서 만든다. 그 전에 stop-and-copy는 freeze를, pre-copy는 쓰기 추적을 건다
        boolean reserved = m.method() == MigrationWire.Method.STOP_AND_COPY ? store.reserveFreeze(tablet) : store.preTrack(tablet);
        if (!reserved) {
            fail(job, "tablet frozen or tracked");
            return Result.started(jobId);
        }
        observeFreeze(job);
        store.inBackground(() -> {
            List<io.openkache.core.migration.TransferBatch.Record> snapshot = store.scan(tablet.range());
            if (!worker.offer(() -> beginPrepared(job, snapshot))) {
                worker.offer(() -> {
                    store.cancelPrepared(tablet);
                    fail(job, "worker queue full at begin");
                });
            }
        });
        return Result.started(jobId);
    }

    private void beginPrepared(Job job, List<io.openkache.core.migration.TransferBatch.Record> snapshot) {
        if (job.phase != Phase.RUNNING) {
            store.cancelPrepared(job.tablet);
            return;
        }
        store.prepareSnapshot(job.tablet, snapshot);
        job.snapshotReadyNanos = System.nanoTime();
        job.snapshotRecords = snapshot.size();
        JobState s;
        try {
            s = job.protocol.begin(MigrationJob.pending(job.change, job.protocol.name()));
        } finally {
            store.cancelPrepared(job.tablet);
        }
        job.begun = true;
        job.lastTickNanos = System.nanoTime();
        if (s == JobState.ABORTED) fail(job, "begin aborted (tablet frozen or tracked)");
        observeFreeze(job);
    }

    /** 상태 조회. worker thread 전용. */
    Optional<Job> job(long jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    private void tick() {
        boolean any = false;
        for (Job job : List.copyOf(jobs.values())) {
            try {
                advance(job);
            } catch (RuntimeException e) {
                fail(job, "driver error: " + e);
            }
            any |= job.phase != Phase.DONE && job.phase != Phase.FAILED;
        }
        active = any;
    }

    private void advance(Job job) {
        long now = System.nanoTime();
        long elapsed = now - job.lastTickNanos;
        job.lastTickNanos = now;
        switch (job.phase) {
            case RUNNING -> {
                if (!job.begun) return;
                Optional<String> failure = job.destination.failure();
                if (failure.isPresent()) {
                    job.protocol.abort();
                    fail(job, failure.get());
                    return;
                }
                long budget = budget(job, elapsed);
                long before = job.sentBytes();
                JobState state = job.protocol.step(budget);
                job.allowance = Math.max(0, job.allowance - (job.sentBytes() - before));
                observeFreeze(job);
                if (state == JobState.ABORTED) {
                    fail(job, "protocol aborted");
                } else if (state == JobState.SWITCHING && readyToCommit(job)) {
                    beginCommit(job);
                }
            }
            case COMMITTING -> {
                if (job.rejectedVersion != null) retryRemoteCommit(job, now);
            }
            case AWAIT_TABLE -> {
                RoutingTable table = routing.table();
                if (table.version().value() >= job.publishedVersion
                        && !table.lookup(job.tablet.range().start()).owner().equals(self)) {
                    job.tableSeenNanos = System.nanoTime();
                    job.phase = Phase.CLEANING;
                    advance(job);
                }
            }
            case CLEANING -> {
                if (job.protocol == null) {
                    store.deleteRange(job.tablet.range());
                } else if (job.protocol.step(0) != JobState.DONE) {
                    return;
                }
                job.doneNanos = System.nanoTime();
                job.phase = Phase.DONE;
            }
            default -> { }
        }
    }

    /** 대역 상한이 0이면 무제한. 쉬는 동안 쌓인 예산은 batch 하나 또는 10ms 분량까지만 둔다. */
    private static long budget(Job job, long elapsedNanos) {
        long rate = job.request.rateBytesPerSec();
        if (rate == 0) return Long.MAX_VALUE / 4;
        long cap = Math.max(job.request.batchBytes(), rate / 100);
        job.allowance = Math.min(cap, job.allowance + rate * elapsedNanos / 1_000_000_000L);
        return job.allowance;
    }

    private static boolean readyToCommit(Job job) {
        return switch (job.protocol) {
            case StopAndCopyProtocol p -> p.readyToCommit();
            case PreCopyProtocol p -> p.readyToCommit();
            default -> false;
        };
    }

    private void observeFreeze(Job job) {
        if (job.freezeNanos == 0 && store.frozen(job.tablet)) job.freezeNanos = System.nanoTime();
    }

    private void beginCommit(Job job) {
        RoutingTable before = routing.table();
        if (!before.contains(job.tablet.id()) || !before.get(job.tablet.id()).equals(job.tablet)) {
            if (job.protocol != null) job.protocol.abort();
            fail(job, "moving tablet changed since begin");
            return;
        }
        job.phase = Phase.COMMITTING;
        job.commitNanos = System.nanoTime();
        if (job.freezeNanos == 0) job.freezeNanos = job.commitNanos;
        routing.stopServing(job.tablet.id(), Optional.of(job.change.to()));
        if (controller != null) {
            Optional<RoutingTable> published = controller.move(before.version(), job.change);
            if (published.isPresent()) routing.accept(published.get());
            onCommitReply(job, before, published.isPresent(), "stale expected version");
            return;
        }
        sendRemoteCommit(job, before);
    }

    private void sendRemoteCommit(Job job, RoutingTable before) {
        byte[] body = ByteBuffer.allocate(33).put((byte) 0).putLong(before.version().value())
                .putLong(job.change.unit().value()).putLong(self.value()).putLong(job.change.to().value()).array();
        caller.call(controllerId, B1Wire.CONTROL, body).whenComplete((reply, error) -> {
            boolean offered = worker.offer(() -> {
                if (job.phase != Phase.COMMITTING) return;
                if (error != null) {
                    // CAS가 적용됐는지 모른다. 거절과 freeze를 유지해 두 주인이 쓰기를 받는 일을 막는다.
                    fail(job, "commit outcome unknown: " + error);
                } else if (reply.status() == Protocol.statusCode(ResultStatus.UNAVAILABLE)
                        && B1Wire.STALE_EXPECTED_VERSION.equals(reply.detail())) {
                    if (job.commitRetries >= MAX_COMMIT_RETRIES) {
                        rejectCommit(job, "stale expected version after " + job.commitRetries + " retries");
                    } else {
                        job.rejectedVersion = before.version();
                        job.tableRequests = 0;
                    }
                } else {
                    onCommitReply(job, before, reply.ok(), "control status " + reply.status() + " " + reply.detail());
                }
            });
            if (!offered) worker.offer(() -> fail(job, "commit reply dropped"));
        });
    }

    private void retryRemoteCommit(Job job, long now) {
        RoutingTable current = routing.table();
        if (current.version().isNewerThan(job.rejectedVersion)) {
            if (!current.contains(job.tablet.id()) || !current.get(job.tablet.id()).equals(job.tablet)) {
                rejectCommit(job, "moving tablet changed before commit retry");
                return;
            }
            job.rejectedVersion = null;
            job.commitRetries++;
            sendRemoteCommit(job, current);
            return;
        }
        if (job.tableRequests > 0 && now - job.lastTableRequest < TABLE_REQUEST_INTERVAL_NANOS) return;
        if (job.tableRequests == MAX_TABLE_REQUESTS) {
            rejectCommit(job, "no newer routing table after " + job.tableRequests + " requests and "
                    + job.commitRetries + " commit retries");
            return;
        }
        job.tableRequests++;
        job.lastTableRequest = now;
        // The normal TABLE_REPLY handler installs the table and performs routing-change housekeeping.
        routing.requestTable(controllerId);
    }

    private void rejectCommit(Job job, String reason) {
        job.commitAckNanos = System.nanoTime();
        if (controller == null) {
            routing.resumeServing(job.tablet);
        } else {
            routing.resumeServing(job.tablet.id());
        }
        if (job.protocol != null) job.protocol.abort();
        fail(job, "commit rejected: " + reason);
    }

    private void onCommitReply(Job job, RoutingTable before, boolean ok, String rejection) {
        if (job.phase != Phase.COMMITTING) return;
        job.commitAckNanos = System.nanoTime();
        if (!ok) {
            rejectCommit(job, rejection);
            return;
        }
        RoutingTable published = before.apply(job.change);
        switch (job.protocol) {
            case StopAndCopyProtocol p -> p.committed(before, published);
            case PreCopyProtocol p -> p.committed(before, published);
            case null, default -> { }
        }
        job.publishedVersion = published.version().value();
        job.phase = Phase.AWAIT_TABLE;
        advance(job);
    }

    private void fail(Job job, String reason) {
        if (job.phase == Phase.DONE || job.phase == Phase.FAILED) return;
        job.failure = reason;
        job.doneNanos = System.nanoTime();
        job.phase = Phase.FAILED;
    }

    @Override
    public void close() {
        ticker.shutdownNow();
    }

    /** MIGRATE 처리 결과. */
    record Result(boolean started, boolean busy, long jobId, String reason) {
        static Result started(long jobId) { return new Result(true, false, jobId, ""); }
        static Result rejected(String reason) { return new Result(false, false, 0, reason); }
        static Result busy(String reason) { return new Result(false, true, 0, reason); }
    }

    /** 이동 한 건. worker thread에서만 바꾼다. */
    static final class Job {
        final long id;
        final MigrationWire.Migrate request;
        final Change change;
        final Tablet tablet;
        final MigrationProtocol protocol;
        final RemoteDestination destination;
        Phase phase = Phase.RUNNING;
        String failure;
        long startEpochMs;
        long startNanos;
        long lastTickNanos;
        long allowance;
        long freezeNanos;
        long commitNanos;
        long commitAckNanos;
        long tableSeenNanos;
        long doneNanos;
        long publishedVersion;
        RoutingVersion rejectedVersion;
        long lastTableRequest;
        int tableRequests;
        int commitRetries;
        long snapshotReadyNanos;
        int snapshotRecords = -1;
        boolean begun;

        Job(long id, MigrationWire.Migrate request, Change change, Tablet tablet, MigrationProtocol protocol,
            RemoteDestination destination) {
            this.id = id;
            this.request = request;
            this.change = change;
            this.tablet = tablet;
            this.protocol = protocol;
            this.destination = destination;
        }

        Phase phase() { return phase; }

        long sentBytes() {
            return switch (protocol) {
                case StopAndCopyProtocol p -> p.job() == null ? 0 : p.job().progress().sentBytes();
                case PreCopyProtocol p -> p.job() == null ? 0 : p.job().progress().sentBytes();
                case null, default -> 0;
            };
        }

        private String ms(long nanos) {
            return nanos == 0 ? "null" : String.format(Locale.ROOT, "%.3f", (nanos - startNanos) / 1e6);
        }

        /**
         * 결과 기록용 JSON. 시각은 job 시작 기준 ms(실제 시계, 이 노드 기준)이고 startEpochMs만 절대 시각이다.
         * writeBlockedMs는 freeze(또는 cutover 거절) 시작부터 이 노드가 새 표를 받을 때까지다.
         */
        String json() {
            StringBuilder s = new StringBuilder(256);
            s.append("{\"jobId\":").append(id)
                    .append(",\"method\":\"").append(request.method().name().toLowerCase(Locale.ROOT).replace('_', '-'))
                    .append("\",\"tablet\":").append(tablet.id().value())
                    .append(",\"from\":").append(change.from().value())
                    .append(",\"to\":").append(change.to().value())
                    .append(",\"phase\":\"").append(phase).append('"')
                    .append(",\"failure\":").append(failure == null ? "null" : quote(failure))
                    .append(",\"rateBytesPerSec\":").append(request.rateBytesPerSec())
                    .append(",\"batchBytes\":").append(request.batchBytes())
                    .append(",\"sentBytes\":").append(sentBytes())
                    .append(",\"startEpochMs\":").append(startEpochMs)
                    .append(",\"snapshotRecords\":").append(snapshotRecords)
                    .append(",\"snapshotReadyMs\":").append(ms(snapshotReadyNanos))
                    .append(",\"freezeStartMs\":").append(ms(freezeNanos))
                    .append(",\"commitStartMs\":").append(ms(commitNanos))
                    .append(",\"commitAckMs\":").append(ms(commitAckNanos))
                    .append(",\"tableSeenMs\":").append(ms(tableSeenNanos))
                    .append(",\"doneMs\":").append(ms(doneNanos))
                    .append(",\"writeBlockedMs\":")
                    .append(freezeNanos == 0 || tableSeenNanos == 0 ? "null"
                            : String.format(Locale.ROOT, "%.3f", (tableSeenNanos - freezeNanos) / 1e6));
            if (protocol instanceof PreCopyProtocol p) {
                s.append(",\"rounds\":").append(p.rounds())
                        .append(",\"stopReason\":").append(p.stopReason().map(r -> quote(r.name())).orElse("null"))
                        .append(",\"roundEvents\":[");
                List<String> events = new ArrayList<>();
                for (PreCopyProtocol.RoundEvent e : p.roundEvents()) {
                    events.add("{\"generation\":" + e.generation() + ",\"final\":" + e.finalRound()
                            + ",\"event\":\"" + e.event() + "\",\"dirtyKeys\":" + e.dirtyKeys()
                            + ",\"tombstoneKeys\":" + e.tombstoneKeys() + ",\"dirtyBytes\":" + e.dirtyBytes()
                            + ",\"roundSentBytes\":" + e.roundSentBytes() + "}");
                }
                s.append(String.join(",", events)).append(']');
            }
            return s.append(",\"commitRetries\":").append(commitRetries).append('}').toString();
        }

        private static String quote(String text) {
            return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
        }
    }
}
