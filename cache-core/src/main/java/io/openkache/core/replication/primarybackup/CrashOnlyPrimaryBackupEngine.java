package io.openkache.core.replication.primarybackup;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 3노드에서 최초 Primary의 단일 영구 crash만 처리하는 기준형.
 * 외부는 주기적으로 TICK을 전달한다. 가장 작은 Backup ID가 승계하고 남은 Backup의
 * 구성 적용 확인 뒤 쓰기를 연다. 모든 변경은 owner에서 수행한다.
 * 분단/오판/재시작/두 번째 장애는 지원하지 않으며 epoch만으로 옛 Primary를 fence하지 않는다.
 * 기존 OK 쓰기는 전원 ACK로 보존된다. 미완료·TIMEOUT 쓰기의 복제본 차이는 복구하지 않는다.
 */
public final class CrashOnlyPrimaryBackupEngine implements CacheEngine {
    public static final String TICK = "primary-backup.failover-tick";
    /** 내부 제어 메시지. TCP codec은 아직 없다. */
    public record Control(OwnershipEpoch epoch, Kind kind) implements PeerMessage {}
    public enum Kind { HEARTBEAT, INSTALL, READY }
    private final PrimaryBackupConfig initial;
    private final CacheEngine kv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource time;
    private final long interval, timeout;
    private final NodeId successor, survivor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<CompletableFuture<Result>> pending = ConcurrentHashMap.newKeySet();
    private volatile PrimaryBackupConfig config;
    private PrimaryBackupEngine delegate;
    private boolean transitioning, installed, peerReady, serving;
    private long lastHeartbeat, lastSent = Long.MIN_VALUE;
    private volatile long detectedAt = -1, readyAt = -1;

    /**
     * interval/timeout은 양수 Duration, timeout은 interval보다 커야 한다. 초기 그룹은 정확히 3노드다.
     * kv는 KvEngine처럼 COUNT를 각 worker의 선행 작업 뒤에 실행해야 한다. barrier 실패 시 쓰기를 열지 않는다.
     */
    public CrashOnlyPrimaryBackupEngine(PrimaryBackupConfig config, CacheEngine kv, Mailbox owner,
            Transport transport, TimeSource time, Duration interval, Duration timeout) {
        if (config.backupIds().size() != 2 || interval.isNegative() || interval.isZero()
                || timeout.compareTo(interval) <= 0) throw new IllegalArgumentException("invalid crash-only configuration");
        this.initial = config; this.config = config; this.kv = kv; this.owner = owner;
        this.transport = transport; this.time = time; this.interval = interval.toNanos(); this.timeout = timeout.toNanos();
        var backups = config.backupIds().stream().sorted(Comparator.comparingLong(NodeId::value)).toList();
        successor = backups.get(0); survivor = backups.get(1);
        delegate = new PrimaryBackupEngine(config, kv, owner, transport, time);
        serving = config.isPrimary(); lastHeartbeat = time.monotonicNanos();
    }

    @Override public CompletionStage<Result> execute(Command command, RequestContext context) {
        CompletableFuture<Result> result = new CompletableFuture<>();
        pending.add(result); result.whenComplete((r, e) -> pending.remove(result));
        if (!owner.offer(() -> {
            if (closed.get()) result.complete(status(ResultStatus.UNAVAILABLE));
            else if (context.ownershipEpoch().value() < config.epoch().value()) result.complete(status(ResultStatus.STALE_EPOCH));
            else if (transitioning && !serving && config.nodeId().equals(successor)) result.complete(status(ResultStatus.UNAVAILABLE));
            else delegate.execute(command, context).whenComplete((r, e) -> {
                if (e == null) result.complete(r); else result.completeExceptionally(e);
            });
        })) result.complete(status(ResultStatus.BUSY));
        return result;
    }

    @Override public void onPeerMessage(NodeId from, PeerMessage message) {
        owner.offer(() -> {
            if (closed.get()) return;
            if (!(message instanceof Control control)) { delegate.onPeerMessage(from, message); return; }
            if (control.kind() == Kind.HEARTBEAT && !transitioning && from.equals(initial.primaryId())
                    && control.epoch().equals(initial.epoch())) lastHeartbeat = time.monotonicNanos();
            if (!control.epoch().equals(initial.epoch().next())) return;
            if (control.kind() == Kind.INSTALL && from.equals(successor) && config.nodeId().equals(survivor)) {
                if (!transitioning) install();
                else if (installed) send(successor, Kind.READY);
            }
            if (control.kind() == Kind.READY && transitioning && from.equals(survivor)
                    && config.nodeId().equals(successor)) { peerReady = true; openWrites(); }
        });
    }

    @Override public void onTimer(TimerEvent event) {
        owner.offer(() -> {
            if (closed.get()) return;
            delegate.onTimer(new TimerEvent(PrimaryBackupEngine.REPLICATE_DEADLINE_TIMER, time.monotonicNanos()));
            if (!TICK.equals(event.name())) return;
            long now = time.monotonicNanos();
            if (initial.isPrimary()) {
                if (lastSent == Long.MIN_VALUE || now - lastSent >= interval) {
                    lastSent = now;
                    for (NodeId backup : initial.backupIds()) send(backup, Kind.HEARTBEAT);
                }
            } else if (config.nodeId().equals(successor)) {
                if (!transitioning && now - lastHeartbeat >= timeout) { detectedAt = now; install(); }
                if (transitioning && !serving && (lastSent == Long.MIN_VALUE || now - lastSent >= interval)) {
                    lastSent = now; send(survivor, Kind.INSTALL);
                }
            }
        });
    }

    private void install() {
        transitioning = true;
        // 닫힌 옛 엔진은 늦은 메시지를 거절한다. COUNT는 각 KV worker에서 이미 큐에 들어간 적용 뒤 실행되는 barrier다.
        delegate.close();
        var context = new RequestContext(new ClientId(0), new OperationId(0), RoutingVersion.ZERO,
                initial.epoch(), Deadline.after(time, Duration.ofSeconds(5)));
        kv.execute(new Command.Count(), context).whenComplete((r, e) -> owner.offer(() -> {
            if (closed.get() || e != null || r.status() != ResultStatus.OK) return;
            config = new PrimaryBackupConfig(initial.nodeId(), successor, List.of(survivor), initial.epoch().next());
            delegate = new PrimaryBackupEngine(config, kv, owner, transport, time);
            installed = true;
            if (config.nodeId().equals(survivor)) send(successor, Kind.READY);
            else openWrites();
        }));
    }

    private void openWrites() {
        if (installed && peerReady && !serving) { serving = true; readyAt = time.monotonicNanos(); }
    }
    private void send(NodeId to, Kind kind) {
        OwnershipEpoch epoch = kind == Kind.HEARTBEAT ? initial.epoch() : initial.epoch().next();
        transport.send(new PeerEnvelope(initial.nodeId(), to, new Control(epoch, kind)));
    }
    private Result status(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, config.epoch()); }
    /** 관측용 가상/실제 monotonic nanos. 아직 발생하지 않았으면 -1. */
    public long detectedAtNanos() { return detectedAt; }
    /** 남은 Backup의 준비 확인을 받은 시각, 아직이면 -1. */
    public long readyAtNanos() { return readyAt; }
    @Override public EngineSnapshot snapshot() {
        return new EngineSnapshot(config.nodeId(), time.monotonicNanos(), RoutingVersion.ZERO, config.epoch());
    }
    @Override public void close() {
        closed.set(true);
        pending.forEach(f -> f.complete(status(ResultStatus.UNAVAILABLE)));
        owner.offer(delegate::close);
    }
}
