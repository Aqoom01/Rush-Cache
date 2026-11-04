package io.openkache.core.replication.primarybackup;

import io.openkache.core.api.CacheEngine;
import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.EngineSnapshot;
import io.openkache.core.api.Redirect;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage.Replicate;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage.ReplicateAck;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.spi.Transport;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 고정 Primary 1대 + 고정 Backup들의 <b>동기</b> Primary-Backup 복제 엔진.
 *
 * <p>저장은 주입받은 로컬 KV({@link CacheEngine}, 보통 {@code KvEngine})에 맡기고, 이 엔진은 노드 사이의 복제와 클라이언트에
 * 성공을 알리는 시점만 관리한다. 참고 문헌은 Budhiraja et al. (1993)이며 설계는
 * {@code experiments/a1-active-passive-failover/design.md} §3 조각 1·2·5를 따른다.
 *
 * <h2>SET 처리 (Primary)</h2>
 *
 * <ol>
 *   <li>op-number를 부여하고 진행 중 요청으로 등록한다. 진행 중인 SET이 이미 있으면 {@link ResultStatus#BUSY}다(이번 단계는
 *       동시에 1건만 진행한다).
 *   <li>모든 Backup에 {@link Replicate}를 보낸다. {@code Transport.send} 완료는 ACK가 아니다.
 *   <li>지정된 Backup이 보낸, 같은 epoch·op-number의 {@link ReplicateAck}를 Backup마다 한 번씩 센다.
 *   <li>모든 Backup의 ACK가 모이면 로컬 KV에 SET을 적용하고, 적용 결과가 OK일 때만 클라이언트에 OK를 돌려준다.
 *   <li>{@code RequestContext.deadline()}까지 ACK가 모이지 않으면 {@link ResultStatus#TIMEOUT}을 한 번만 돌려준다. 늦게 온
 *       ACK는 무시한다. TIMEOUT은 취소나 롤백이 아니다. 일부 Backup에는 이미 적용됐을 수 있다.
 * </ol>
 *
 * <p>GET·EXISTS·COUNT는 Primary의 로컬 KV에 바로 위임한다. DELETE 복제는 미구현이라 {@link ResultStatus#NOT_IMPLEMENTED}다.
 * Backup으로 온 클라이언트 요청은 모두 {@link ResultStatus#NOT_PRIMARY} + {@link Redirect}(Primary)다.
 *
 * <h2>Replicate 처리 (Backup)</h2>
 *
 * <p>지정된 Primary가 보낸 현재 epoch의 {@link Replicate}만 받아 로컬 KV에 적용하고, 적용 결과가 OK일 때만
 * {@link ReplicateAck}를 보낸다. 다른 노드나 다른 epoch의 메시지는 무시한다(아직 {@code STALE_EPOCH} 거부·fencing은 없다).
 * 재전송·순서 역전·복귀 후 따라잡기는 미구현이며 정상 순서 전달을 전제한다.
 *
 * <h2>timer</h2>
 *
 * <p>이 엔진은 timer를 스스로 예약하지 않는다. 외부(server 또는 테스트 harness)가 {@link #REPLICATE_DEADLINE_TIMER} 이름의
 * {@link TimerEvent}를 {@link #onTimer}로 넣어 주면 진행 중 요청의 deadline을 확인한다. ACK를 처리할 때도 deadline을 다시
 * 확인하므로 timer가 늦어도 늦은 ACK로 OK가 되지는 않는다.
 *
 * <h2>thread 규칙</h2>
 *
 * <ul>
 *   <li>진행 중 요청, op-number 등 모든 상태는 생성자에 받은 {@code owner} mailbox의 thread에서만 바꾼다. 어떤 메서드도 호출
 *       thread를 block하지 않는다.
 *   <li>로컬 KV의 완료 callback은 KV worker thread에서 돌 수 있으므로, 상태를 바꾸는 후속 작업은 다시 {@code owner}에 넘긴다.
 *       Backup의 ACK 전송은 상태를 바꾸지 않으므로 callback에서 바로 보낸다.
 *   <li>mailbox가 가득 차면 클라이언트 요청은 BUSY로 끝내고, peer 메시지와 timer는 버린다(버려진 ACK는 Primary의 TIMEOUT으로
 *       드러난다).
 * </ul>
 *
 * <p>주입받은 KV, mailbox, transport의 생명주기는 외부 조립부가 관리한다. {@link #close()}는 이 엔진의 진행 중 요청만
 * {@link ResultStatus#UNAVAILABLE}로 끝낸다.
 */
public final class PrimaryBackupEngine implements CacheEngine {

    /** 진행 중 SET의 deadline 확인용 timer 이름. 외부가 이 이름으로 {@link TimerEvent}를 넣는다. */
    public static final String REPLICATE_DEADLINE_TIMER = "primary-backup.replicate-deadline";

    private static final System.Logger LOG = System.getLogger(PrimaryBackupEngine.class.getName());

    /**
     * Backup이 전달받은 SET을 로컬 KV에 적용할 때 쓰는 제한 시간. 클라이언트 deadline은 다른 JVM의 monotonic 값이라 그대로 쓸 수
     * 없고({@link Deadline} 참고), 이번 단계에서는 남은 시간을 wire로 보내지 않는다.
     */
    private static final Duration BACKUP_APPLY_TIMEOUT = Duration.ofSeconds(5);

    private final PrimaryBackupConfig config;
    private final CacheEngine localKv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource timeSource;
    private final Set<CompletableFuture<Result>> pending = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    // 아래 두 필드는 owner thread에서만 읽고 쓴다.
    private long nextOpNumber = 1;
    private InFlight inFlight;

    /** Primary가 진행 중인 SET 하나의 상태. owner thread에서만 바꾼다. */
    private static final class InFlight {
        final long opNumber;
        final Command.Set command;
        final RequestContext context;
        final CompletableFuture<Result> future;
        final Set<NodeId> acked = new HashSet<>();

        InFlight(long opNumber, Command.Set command, RequestContext context, CompletableFuture<Result> future) {
            this.opNumber = opNumber;
            this.command = command;
            this.context = context;
            this.future = future;
        }
    }

    /**
     * 노드별 구성 요소를 연결한다. 아직 메시지를 보내거나 작업을 예약하지 않는다.
     *
     * @param config 이 노드와 고정 복제 그룹의 설정
     * @param localKv 같은 노드의 로컬 KV. Backup은 Replicate를, Primary는 모든 ACK가 모인 SET을 여기에 적용한다
     * @param owner 이 엔진의 상태를 바꿀 worker의 mailbox
     * @param transport 노드 간 전송 경계
     * @param timeSource deadline 판단에 쓸 시계. 테스트에서는 가상 시간을 넣는다
     */
    public PrimaryBackupEngine(
            PrimaryBackupConfig config, CacheEngine localKv, Mailbox owner, Transport transport, TimeSource timeSource) {
        this.config = Objects.requireNonNull(config, "config");
        this.localKv = Objects.requireNonNull(localKv, "localKv");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    }

    @Override
    public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(context, "context");
        if (closed.get()) {
            return CompletableFuture.completedFuture(status(ResultStatus.UNAVAILABLE));
        }
        if (context.deadline().isExpired(timeSource)) {
            return CompletableFuture.completedFuture(status(ResultStatus.TIMEOUT));
        }
        if (!config.isPrimary()) {
            return CompletableFuture.completedFuture(
                    Result.redirect(
                            ResultStatus.NOT_PRIMARY,
                            new Redirect(config.primaryId()),
                            RoutingVersion.ZERO,
                            config.epoch()));
        }
        return switch (command) {
            case Command.Set set -> submitSet(set, context);
            case Command.Get get -> localKv.execute(get, context).thenApply(this::withEpoch);
            case Command.Exists exists -> localKv.execute(exists, context).thenApply(this::withEpoch);
            case Command.Count count -> localKv.execute(count, context).thenApply(this::withEpoch);
            case Command.Delete delete -> CompletableFuture.completedFuture(status(ResultStatus.NOT_IMPLEMENTED));
        };
    }

    /** Primary: SET을 owner에 넘긴다. mailbox가 가득 차면 BUSY. */
    private CompletionStage<Result> submitSet(Command.Set set, RequestContext context) {
        CompletableFuture<Result> future = track(new CompletableFuture<>());
        if (!owner.offer(() -> startReplication(set, context, future))) {
            future.complete(status(ResultStatus.BUSY));
        }
        return future;
    }

    /** owner thread. 진행 중 요청으로 등록한 뒤 모든 Backup에 Replicate를 보낸다. */
    private void startReplication(Command.Set set, RequestContext context, CompletableFuture<Result> future) {
        if (closed.get()) {
            future.complete(status(ResultStatus.UNAVAILABLE));
            return;
        }
        if (context.deadline().isExpired(timeSource)) {
            future.complete(status(ResultStatus.TIMEOUT));
            return;
        }
        reconcile();
        if (inFlight != null) {
            // 이번 단계 정책: 동시에 진행하는 쓰기는 1건. 추가 요청은 BUSY로 거절하고 호출자가 재시도 여부를 정한다.
            future.complete(status(ResultStatus.BUSY));
            return;
        }
        long opNumber = nextOpNumber++;
        inFlight = new InFlight(opNumber, set, context, future);
        Replicate replicate = new Replicate(config.epoch(), opNumber, set);
        LOG.log(
                Level.DEBUG,
                "node {0} op {1}: SET accepted, replicating to {2}",
                config.nodeId(),
                opNumber,
                config.backupIds());
        for (NodeId backup : config.backupIds()) {
            // 전송 실패는 ACK 부재로 드러나 deadline에 TIMEOUT이 된다. 재전송은 미구현.
            transport.send(new PeerEnvelope(config.nodeId(), backup, replicate));
        }
    }

    @Override
    public void onPeerMessage(NodeId from, PeerMessage message) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(message, "message");
        if (!(message instanceof PrimaryBackupMessage replicationMessage)) {
            return;
        }
        if (!owner.offer(() -> handle(from, replicationMessage))) {
            LOG.log(Level.DEBUG, "node {0}: owner mailbox full, dropped {1} from {2}", config.nodeId(), message, from);
        }
    }

    /** owner thread. */
    private void handle(NodeId from, PrimaryBackupMessage message) {
        if (closed.get()) {
            return;
        }
        switch (message) {
            case Replicate replicate -> onReplicate(from, replicate);
            case ReplicateAck ack -> onReplicateAck(from, ack);
        }
    }

    /** owner thread, Backup. 지정된 Primary의 현재 epoch 메시지만 로컬 KV에 적용하고, 성공했을 때만 ACK를 보낸다. */
    private void onReplicate(NodeId from, Replicate replicate) {
        if (config.isPrimary() || !from.equals(config.primaryId()) || !replicate.epoch().equals(config.epoch())) {
            LOG.log(Level.DEBUG, "node {0}: ignored {1} from {2}", config.nodeId(), replicate, from);
            return;
        }
        // Primary를 대신해 적용하므로 clientId는 Primary 노드 번호, operationId는 op-number를 쓴다. 로컬 KV는 deadline만 본다.
        RequestContext applyContext =
                new RequestContext(
                        new ClientId(from.value()),
                        new OperationId(replicate.opNumber()),
                        RoutingVersion.ZERO,
                        config.epoch(),
                        Deadline.after(timeSource, BACKUP_APPLY_TIMEOUT));
        localKv.execute(replicate.command(), applyContext)
                .whenComplete(
                        (result, error) -> {
                            // KV worker thread일 수 있다. 엔진 상태는 건드리지 않고 ACK만 보낸다.
                            if (error == null && result.status() == ResultStatus.OK) {
                                LOG.log(
                                        Level.DEBUG,
                                        "node {0} op {1}: applied on backup, sending ack",
                                        config.nodeId(),
                                        replicate.opNumber());
                                transport.send(
                                        new PeerEnvelope(
                                                config.nodeId(),
                                                from,
                                                new ReplicateAck(replicate.epoch(), replicate.opNumber())));
                            } else {
                                LOG.log(
                                        Level.WARNING,
                                        "node {0} op {1}: backup apply failed ({2}), no ack",
                                        config.nodeId(),
                                        replicate.opNumber(),
                                        error != null ? error : result.status());
                            }
                        });
    }

    /** owner thread, Primary. 진행 중 요청과 일치하는 ACK를 Backup별로 한 번만 세고, 모두 모이면 로컬에 적용한다. */
    private void onReplicateAck(NodeId from, ReplicateAck ack) {
        reconcile();
        InFlight current = inFlight;
        if (!config.isPrimary() || current == null) {
            LOG.log(Level.DEBUG, "node {0}: no in-flight request, ignored {1} from {2}", config.nodeId(), ack, from);
            return;
        }
        if (!config.backupIds().contains(from)
                || !ack.epoch().equals(config.epoch())
                || ack.opNumber() != current.opNumber) {
            LOG.log(Level.DEBUG, "node {0} op {1}: ignored {2} from {3}", config.nodeId(), current.opNumber, ack, from);
            return;
        }
        if (current.context.deadline().isExpired(timeSource)) {
            finish(current, status(ResultStatus.TIMEOUT));
            return;
        }
        if (!current.acked.add(from)) {
            LOG.log(Level.DEBUG, "node {0} op {1}: duplicate ack from {2}", config.nodeId(), current.opNumber, from);
            return;
        }
        LOG.log(
                Level.DEBUG,
                "node {0} op {1}: ack {2}/{3} from {4}",
                config.nodeId(),
                current.opNumber,
                current.acked.size(),
                config.requiredAcks(),
                from);
        if (current.acked.size() < config.requiredAcks()) {
            return;
        }
        applyLocally(current);
    }

    /** owner thread, Primary. 모든 ACK가 모인 SET을 로컬 KV에 적용하고 그 결과로 클라이언트 응답을 끝낸다. */
    private void applyLocally(InFlight current) {
        localKv.execute(current.command, current.context)
                .whenComplete(
                        (result, error) -> {
                            Result outcome = error == null ? withEpoch(result) : status(ResultStatus.INTERNAL_ERROR);
                            if (!owner.offer(() -> finish(current, outcome))) {
                                // owner에 넘길 수 없으면 응답만 먼저 끝낸다. 상태 정리는 다음 owner 작업의 reconcile()이 한다.
                                current.future.complete(outcome);
                            }
                        });
    }

    /** owner thread. 진행 중 요청을 비우고 응답을 한 번만 완료한다. 이미 완료된 요청이면 아무 일도 하지 않는다. */
    private void finish(InFlight current, Result result) {
        if (inFlight == current) {
            inFlight = null;
        }
        if (current.future.complete(result)) {
            LOG.log(Level.DEBUG, "node {0} op {1}: completed {2}", config.nodeId(), current.opNumber, result.status());
        }
    }

    /** owner thread. 응답이 owner 밖에서 먼저 끝난 요청(close, mailbox 가득 참)이 남아 있으면 비운다. */
    private void reconcile() {
        if (inFlight != null && inFlight.future.isDone()) {
            inFlight = null;
        }
    }

    @Override
    public void onTimer(TimerEvent event) {
        Objects.requireNonNull(event, "event");
        if (!REPLICATE_DEADLINE_TIMER.equals(event.name())) {
            return;
        }
        if (!owner.offer(this::checkDeadline)) {
            LOG.log(Level.DEBUG, "node {0}: owner mailbox full, dropped timer {1}", config.nodeId(), event.name());
        }
    }

    /** owner thread, Primary. 진행 중 요청의 deadline이 지났으면 TIMEOUT으로 끝낸다. 아직이면 아무 일도 하지 않는다. */
    private void checkDeadline() {
        if (closed.get()) {
            return;
        }
        reconcile();
        InFlight current = inFlight;
        if (current != null && current.context.deadline().isExpired(timeSource)) {
            finish(current, status(ResultStatus.TIMEOUT));
        }
    }

    @Override
    public EngineSnapshot snapshot() {
        return new EngineSnapshot(config.nodeId(), timeSource.monotonicNanos(), RoutingVersion.ZERO, config.epoch());
    }

    /**
     * 새 요청을 거절하고 아직 완료되지 않은 요청을 UNAVAILABLE로 끝낸다. 여러 번 호출해도 안전하며 외부 소유 KV·mailbox·transport는
     * 닫지 않는다.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (CompletableFuture<Result> future : pending) {
            if (pending.remove(future)) {
                future.complete(status(ResultStatus.UNAVAILABLE));
            }
        }
    }

    private CompletableFuture<Result> track(CompletableFuture<Result> future) {
        future.whenComplete((result, error) -> pending.remove(future));
        pending.add(future);
        return future;
    }

    private Result status(ResultStatus status) {
        return Result.of(status, RoutingVersion.ZERO, config.epoch());
    }

    /** 로컬 KV의 결과에 이 노드가 아는 epoch를 붙인다. */
    private Result withEpoch(Result result) {
        return new Result(
                result.status(),
                result.value(),
                result.valueVersion(),
                result.routingVersion(),
                config.epoch(),
                result.redirect());
    }
}
