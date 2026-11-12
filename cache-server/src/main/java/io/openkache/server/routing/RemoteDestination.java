package io.openkache.server.routing;

import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.precopy.DeltaBatch;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.Tablet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * source 노드에서 B3 {@link PreCopyProtocol.Destination}을 TCP TRANSFER로 구현한 프록시. protocol은 batch를 넘긴 뒤 다음
 * step에서 ACK를 묻는 구조라, 응답 OK(= destination 적용 완료)가 도착한 batch만 ACK로 돌려준다.
 *
 * <p>protocol 호출은 source worker thread에서, 응답 기록은 Netty thread에서 일어난다. 실패한 응답이 하나라도 있으면
 * {@link #failure()}가 채워지고 driver가 job을 취소한다.
 */
final class RemoteDestination implements PreCopyProtocol.Destination {

    private final PeerRequests caller;
    private final NodeId self;
    private final NodeId target;
    private final long jobId;
    private final Map<Long, StopAndCopyProtocol.Acknowledgement> acks = new ConcurrentHashMap<>();
    private final Map<Long, DeltaBatch.Acknowledgement> deltaAcks = new ConcurrentHashMap<>();
    private volatile boolean snapshotComplete;
    private volatile boolean finalAcked;
    private volatile long ackedSeq;
    private volatile String failure;
    private Tablet tablet;
    private CompletableFuture<Boolean> prepared = CompletableFuture.completedFuture(false);

    RemoteDestination(PeerRequests caller, NodeId self, NodeId target, long jobId) {
        this.caller = caller;
        this.self = self;
        this.target = target;
        this.jobId = jobId;
    }

    /** 실패한 전송이 있으면 그 이유. */
    Optional<String> failure() {
        return Optional.ofNullable(failure);
    }

    private MigrationWire.Header header(int kind, OwnershipEpoch epoch) {
        return new MigrationWire.Header(self.value(), target.value(), jobId, tablet.id().value(), epoch.value(), kind);
    }

    private CompletableFuture<Boolean> send(byte[] body) {
        return caller.call(target, MigrationWire.TRANSFER, body).handle((reply, error) -> {
            if (error != null) {
                fail("transfer failed: " + error);
                return false;
            }
            if (!reply.ok()) {
                fail("transfer rejected: status " + reply.status() + " " + reply.detail());
                return false;
            }
            return true;
        });
    }

    private void fail(String reason) {
        if (failure == null) failure = reason;
    }

    /**
     * 준비 요청을 보내고 바로 true. 이후 전송은 준비 응답(OK) 뒤에만 나가므로 destination에서 순서가 뒤바뀌지 않는다.
     * 준비가 실패하면 failure로 드러난다.
     */
    @Override
    public boolean prepare(MigrationJob session, Tablet t) {
        tablet = t;
        prepared = send(MigrationWire.prepare(header(MigrationWire.PREPARE, t.epoch()), t.range()));
        return true;
    }

    /** 준비가 성공한 뒤에만 보낸다. */
    private CompletableFuture<Boolean> sendAfterPrepare(byte[] body) {
        return prepared.thenCompose(ok -> ok ? send(body) : CompletableFuture.completedFuture(false));
    }

    @Override
    public void receive(MigrationJob session, OwnershipEpoch epoch, TransferBatch batch) {
        sendAfterPrepare(MigrationWire.snapshot(header(MigrationWire.SNAPSHOT, epoch), batch)).thenAccept(ok -> {
            if (!ok) return;
            acks.put(batch.batchId(), StopAndCopyProtocol.Acknowledgement.of(batch));
            if (batch.last()) snapshotComplete = true;
        });
    }

    @Override
    public Optional<StopAndCopyProtocol.Acknowledgement> acknowledgement(MigrationJob session, long batchId) {
        return Optional.ofNullable(acks.get(batchId));
    }

    @Override
    public boolean complete(MigrationJob session) {
        return snapshotComplete;
    }

    @Override
    public void discard(MigrationJob session) {
        if (tablet != null) sendAfterPrepare(MigrationWire.discard(header(MigrationWire.DISCARD, tablet.epoch())));
    }

    @Override
    public void receiveDelta(MigrationJob session, OwnershipEpoch epoch, DeltaBatch batch) {
        sendAfterPrepare(MigrationWire.delta(header(MigrationWire.DELTA, epoch), batch)).thenAccept(ok -> {
            if (!ok) return;
            DeltaBatch.Acknowledgement ack = DeltaBatch.Acknowledgement.of(batch);
            if (ack.lastSequence() > 0) ackedSeq = ack.lastSequence();
            deltaAcks.put(batch.batchId(), ack);
            if (batch.finalRound() && batch.last()) finalAcked = true;
        });
    }

    @Override
    public Optional<DeltaBatch.Acknowledgement> deltaAcknowledgement(MigrationJob session, long batchId) {
        return Optional.ofNullable(deltaAcks.get(batchId));
    }

    @Override
    public boolean caughtUp(MigrationJob session, long finalSeq) {
        return finalAcked && ackedSeq >= finalSeq;
    }
}
