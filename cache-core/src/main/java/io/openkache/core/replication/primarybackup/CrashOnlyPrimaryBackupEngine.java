package io.openkache.core.replication.primarybackup;

import io.openkache.core.api.*;
import io.openkache.core.failure.MemberStatus;
import io.openkache.core.failure.MembershipEvent;
import io.openkache.core.model.*;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 3노드에서 최초 Primary 한 번의 승계를 처리한다.
 * 외부는 주기적으로 TICK을 전달한다. 가장 작은 Backup ID가 승계하고 남은 Backup의
 * 구성 적용 확인 뒤 쓰기를 연다. 모든 변경은 owner에서 수행한다.
 * 기존 생성자는 crash-only 기준형이다. lease 생성자는 DEAD, 과반수 투표와 lease로 승계를 제한한다.
 * lease 모드는 초기 KV가 비어 있고, 같은 lease 설정과 중단 없이 흐르는 bounded-drift 시계를 사용한다.
 * 재시작·두 번째 승계는 지원하지 않는다. 미완료 쓰기로 두 Backup이 다르면 승계를 보류한다.
 */
public final class CrashOnlyPrimaryBackupEngine implements CacheEngine {
    public static final String TICK = "primary-backup.failover-tick";
    /** PB 구성 전환 제어 메시지. */
    public record Control(OwnershipEpoch epoch, Kind kind) implements PeerMessage {}
    public enum Kind { HEARTBEAT, INSTALL, READY }
    private final PrimaryBackupConfig initial;
    private final CacheEngine kv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource time;
    private final long interval, timeout;
    private final boolean fixedTimeoutDetection;
    private final NodeId successor, survivor;
    private final PrimaryBackupFence fence;
    private final MessageDigest backupHistory;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<MembershipEvent> firstSuspect = new AtomicReference<>();
    private final AtomicReference<MembershipEvent> firstDead = new AtomicReference<>();
    private final AtomicInteger completedBackupApplies = new AtomicInteger();
    private final AtomicBoolean backupApplyFailed = new AtomicBoolean();
    private final AtomicBoolean installBarrierPassed = new AtomicBoolean();
    private final Set<CompletableFuture<Result>> pending = ConcurrentHashMap.newKeySet();
    private volatile PrimaryBackupConfig config;
    private PrimaryBackupEngine delegate;
    private boolean transitioning, installed, peerReady, serving;
    private long lastBackupOp;
    private int backupPending;
    private Bytes frozenDigest;
    private long lastHeartbeat, lastSent = Long.MIN_VALUE;
    private volatile long detectedAt = -1, readyAt = -1;
    private long suspectAt = -1, quorumAt = -1, authorizedAt = -1, installedAt = -1, peerReadyAt = -1;
    private volatile FenceObservation fenceObservation;

    public record FenceObservation(NodeId primary, long suspectAt, long deadAt, long quorumAt, long authorizedAt, long installedAt,
                                   long peerReadyAt, long leaseDeadline, int votes, String state) {}

    /**
     * interval/timeout은 양수 Duration, timeout은 interval보다 커야 한다. 초기 그룹은 정확히 3노드다.
     * kv는 KvEngine처럼 COUNT를 각 worker의 선행 작업 뒤에 실행해야 한다. barrier 실패 시 쓰기를 열지 않는다.
     */
    public CrashOnlyPrimaryBackupEngine(PrimaryBackupConfig config, CacheEngine kv, Mailbox owner,
            Transport transport, TimeSource time, Duration interval, Duration timeout) {
        this(config, kv, owner, transport, time, interval, timeout, true, null);
    }

    /** 외부 failure detector가 승격을 시작할 때 내부 heartbeat timeout 감지를 끈다. */
    public CrashOnlyPrimaryBackupEngine(PrimaryBackupConfig config, CacheEngine kv, Mailbox owner,
            Transport transport, TimeSource time, Duration interval, Duration timeout, boolean fixedTimeoutDetection) {
        this(config, kv, owner, transport, time, interval, timeout, fixedTimeoutDetection, null);
    }

    /** lease를 주면 외부 membership 이벤트와 투표를 사용하는 펜싱 모드다. */
    public CrashOnlyPrimaryBackupEngine(PrimaryBackupConfig config, CacheEngine kv, Mailbox owner,
            Transport transport, TimeSource time, Duration interval, Duration timeout, Duration lease) {
        this(config, kv, owner, transport, time, interval, timeout, false, lease);
    }

    private CrashOnlyPrimaryBackupEngine(PrimaryBackupConfig config, CacheEngine kv, Mailbox owner,
            Transport transport, TimeSource time, Duration interval, Duration timeout,
            boolean fixedTimeoutDetection, Duration lease) {
        if (config.backupIds().size() != 2 || interval.isNegative() || interval.isZero()
                || timeout.compareTo(interval) <= 0 || (lease != null && lease.compareTo(interval) <= 0))
            throw new IllegalArgumentException("invalid primary-backup configuration");
        this.initial = config; this.config = config; this.kv = kv; this.owner = owner;
        this.transport = transport; this.time = time; this.interval = interval.toNanos(); this.timeout = timeout.toNanos();
        this.fixedTimeoutDetection = fixedTimeoutDetection;
        var backups = config.backupIds().stream().sorted(Comparator.comparingLong(NodeId::value)).toList();
        successor = backups.get(0); survivor = backups.get(1);
        fence = lease == null ? null : new PrimaryBackupFence(config, successor, survivor, time, transport, interval, lease);
        backupHistory = fence == null || config.isPrimary() ? null : sha256();
        delegate = newDelegate();
        serving = config.isPrimary(); lastHeartbeat = time.monotonicNanos();
        publishFence();
    }

    /** Latch A3 events for the owner tick, even if its mailbox is temporarily full. */
    public void onMembershipChanged(MembershipEvent event) {
        if (!event.member().equals(initial.primaryId())) return;
        if (event.current() == MemberStatus.SUSPECT) firstSuspect.compareAndSet(null, event);
        if (event.current() == MemberStatus.DEAD) firstDead.compareAndSet(null, event);
    }

    /** SWIM 어댑터가 DEAD 시각만 제공하는 기존 호출부를 위한 편의 진입점. */
    public void onPrimaryFailure(NodeId failed, long observedAtNanos) {
        if (!failed.equals(initial.primaryId()) || observedAtNanos < 0) return;
        onMembershipChanged(new MembershipEvent(failed, MemberStatus.SUSPECT, MemberStatus.DEAD, 0, observedAtNanos));
    }

    private void observeMembership() {
        if (!config.epoch().equals(initial.epoch())) return;
        MembershipEvent suspect = firstSuspect.get();
        if (suspect != null && suspectAt == -1) suspectAt = suspect.observedAtNanos();
        MembershipEvent dead = firstDead.get();
        if (dead != null && detectedAt == -1) {
            detectedAt = dead.observedAtNanos();
            if (fence != null) fence.observeDead(dead.member());
            else if (!fixedTimeoutDetection && config.nodeId().equals(successor) && !transitioning) install();
        }
    }

    @Override public CompletionStage<Result> execute(Command command, RequestContext context) {
        CompletableFuture<Result> result = new CompletableFuture<>();
        pending.add(result); result.whenComplete((r, e) -> pending.remove(result));
        if (!owner.offer(() -> {
            if (closed.get()) result.complete(status(ResultStatus.UNAVAILABLE));
            else if (context.ownershipEpoch().value() < config.epoch().value()) result.complete(status(ResultStatus.STALE_EPOCH));
            else if ((transitioning && !serving && config.nodeId().equals(successor))
                    || (fence != null && config.isPrimary() && !mayServe())) result.complete(status(ResultStatus.UNAVAILABLE));
            else {
                var authority = fence != null && config.isPrimary() ? fence.authority() : null;
                var admittedEpoch = config.epoch();
                delegate.execute(command, context).whenComplete((r, e) -> {
                    if (authority == null) complete(result, r, e);
                    else if (!owner.offer(() -> {
                        if (!mayServe() || authority.isExpired(time) || !config.epoch().equals(admittedEpoch))
                            result.complete(status(ResultStatus.UNAVAILABLE));
                        else complete(result, r, e);
                    })) result.complete(status(ResultStatus.UNAVAILABLE));
                });
            }
        })) result.complete(status(ResultStatus.BUSY));
        return result;
    }

    @Override public void onPeerMessage(NodeId from, PeerMessage message) {
        owner.offer(() -> {
            if (closed.get()) return;
            if (message instanceof PrimaryBackupFence.Message fenced) {
                if (fence != null) { fence.receive(from, fenced); publishFence(); }
                return;
            }
            if (!(message instanceof Control control)) {
                if (fence != null && message instanceof PrimaryBackupMessage.Replicate replicate) {
                    applyFencedBackup(from, replicate);
                } else delegate.onPeerMessage(from, message);
                return;
            }
            if (control.kind() == Kind.HEARTBEAT && !transitioning && from.equals(initial.primaryId())
                    && control.epoch().equals(initial.epoch())) lastHeartbeat = time.monotonicNanos();
            if (!control.epoch().equals(initial.epoch().next())) return;
            if (control.kind() == Kind.INSTALL && from.equals(successor) && config.nodeId().equals(survivor)) {
                if (fence != null && (!fence.voted() || frozenDigest == null)) return;
                if (!transitioning) install();
                else if (installed) send(successor, Kind.READY);
            }
            if (control.kind() == Kind.READY && transitioning && from.equals(survivor)
                    && config.nodeId().equals(successor)) {
                if (!peerReady) peerReadyAt = time.monotonicNanos();
                peerReady = true; openWrites();
            }
            publishFence();
        });
    }

    @Override public void onTimer(TimerEvent event) {
        owner.offer(() -> {
            if (closed.get()) return;
            delegate.onTimer(new TimerEvent(PrimaryBackupEngine.REPLICATE_DEADLINE_TIMER, time.monotonicNanos()));
            if (!TICK.equals(event.name())) return;
            long now = time.monotonicNanos();
            observeMembership();
            if (fence != null) {
                backupPending -= completedBackupApplies.getAndSet(0);
                if (transitioning && !installed && installBarrierPassed.get()) finishInstall();
                if (fence.voted() && frozenDigest == null && backupPending == 0 && !backupApplyFailed.get() && backupHistory != null) {
                    frozenDigest = Bytes.copyOf(backupHistory.digest());
                }
                boolean authorized = fence.tick(serving, backupPending == 0, frozenDigest);
                if (quorumAt == -1 && fence.voted() && fence.peerVoted()) quorumAt = now;
                if (!transitioning && authorized) {
                    authorizedAt = now;
                    install();
                }
                if (config.nodeId().equals(successor) && transitioning && !peerReady
                        && (lastSent == Long.MIN_VALUE || now - lastSent >= interval)) {
                    lastSent = now; send(survivor, Kind.INSTALL);
                }
                markFencedReady();
                publishFence();
                return;
            }
            if (initial.isPrimary()) {
                if (lastSent == Long.MIN_VALUE || now - lastSent >= interval) {
                    lastSent = now;
                    for (NodeId backup : initial.backupIds()) send(backup, Kind.HEARTBEAT);
                }
            } else if (config.nodeId().equals(successor)) {
                if (fixedTimeoutDetection && !transitioning && now - lastHeartbeat >= timeout) { detectedAt = now; install(); }
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
        kv.execute(new Command.Count(), context).whenComplete((r, e) -> {
            if (e != null || r.status() != ResultStatus.OK) return;
            if (fence != null) installBarrierPassed.set(true);
            else owner.offer(this::finishInstall);
        });
    }

    private void finishInstall() {
        if (closed.get()) return;
        config = new PrimaryBackupConfig(initial.nodeId(), successor, List.of(survivor), initial.epoch().next());
        if (fence != null) {
            fence.advanceEpoch();
            lastBackupOp = 0;
        }
        delegate = newDelegate();
        installed = true;
        installedAt = time.monotonicNanos();
        if (config.nodeId().equals(survivor)) send(successor, Kind.READY);
        else openWrites();
    }

    private void openWrites() {
        if (installed && peerReady && !serving) {
            serving = true;
            if (fence == null) readyAt = time.monotonicNanos();
        }
    }

    private void markFencedReady() {
        if (transitioning && readyAt == -1 && mayServe()) readyAt = time.monotonicNanos();
    }

    private PrimaryBackupEngine newDelegate() {
        return new PrimaryBackupEngine(config, kv, owner, transport, time, this::mayServe);
    }

    private boolean mayServe() {
        return !closed.get() && (fence == null || (serving && fence.hasAuthority()));
    }

    private static void complete(CompletableFuture<Result> result, Result outcome, Throwable error) {
        if (error == null) result.complete(outcome); else result.completeExceptionally(error);
    }

    private void applyFencedBackup(NodeId from, PrimaryBackupMessage.Replicate replicate) {
        if (config.isPrimary() || !from.equals(config.primaryId()) || !replicate.epoch().equals(config.epoch())) return;
        if (replicate.opNumber() <= lastBackupOp) return;
        if (!config.epoch().equals(initial.epoch())) {
            lastBackupOp = replicate.opNumber();
            delegate.applyBackup(from, replicate, ignored -> {});
            return;
        }
        if (fence.voted()) return;
        lastBackupOp = replicate.opNumber();
        var command = replicate.command();
        backupHistory.update(ByteBuffer.allocate(16).putLong(lastBackupOp)
                .putInt(command.key().size()).putInt(command.value().size()).array());
        backupHistory.update(command.key().toByteArray());
        backupHistory.update(command.value().toByteArray());
        backupPending++;
        delegate.applyBackup(from, replicate, ok -> {
            if (!ok) backupApplyFailed.set(true);
            completedBackupApplies.incrementAndGet();
        });
    }

    private void publishFence() {
        if (fence == null) return;
        markFencedReady();
        String state;
        if (fence.originalFenced()) state = "FENCED";
        else if (config.isPrimary()) state = mayServe() ? "SERVING" : (transitioning && !peerReady ? "WAIT_READY" : "NO_LEASE");
        else if (transitioning) state = installed ? "INSTALLED" : "INSTALLING";
        else if (fence.voted()) state = backupApplyFailed.get() || fence.peerVoted() ? "HISTORY_NOT_READY" : "WAIT_VOTE";
        else state = fence.dead() ? "WAIT_OLD_LEASE" : "BACKUP";
        fenceObservation = new FenceObservation(config.primaryId(), suspectAt, detectedAt, quorumAt, authorizedAt, installedAt, peerReadyAt,
                fence.authority() == null ? -1 : fence.authority().monotonicNanos(),
                (fence.voted() ? 1 : 0) + (fence.peerVoted() ? 1 : 0), state);
    }

    public FenceObservation fenceObservation() { return fenceObservation; }

    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
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
