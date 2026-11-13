package io.openkache.core.migration.precopy;

import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol.Acknowledgement;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.Tablet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * In-process pre-copy staging. snapshot barrier·session/epoch·순서·checksum을 적용 전에 검증한다.
 * 모든 메서드는 동일 controller thread 전용이며 data()는 검증/게시 adapter용 스냅샷이다.
 * 실제 KvEngine/peer adapter가 아니며 commit 전 client에 staging을 노출하지 않아야 한다.
 */
public final class PreCopyReceiver implements PreCopyProtocol.Destination {
    private final java.util.function.ToLongFunction<Bytes> keyHash;
    public PreCopyReceiver() { this(KeyHash::of); }
    /** B1과 같은 불변 key 변환을 사용한다. controller thread 전용이다. */
    public PreCopyReceiver(java.util.function.ToLongFunction<Bytes> keyHash) { this.keyHash = java.util.Objects.requireNonNull(keyHash); }
    private MigrationJob session;
    private Tablet tablet;
    private final Map<Bytes, Bytes> data = new LinkedHashMap<>();
    private final Map<Long, Acknowledgement> snapshots = new HashMap<>();
    private final Map<Long, DeltaBatch.Acknowledgement> deltas = new HashMap<>();
    private final Map<Bytes, Long> lastSequences = new HashMap<>();
    private long nextBatch;
    private long lastSequence;
    private boolean snapshotComplete;
    private boolean finalComplete;

    @Override
    public boolean prepare(MigrationJob candidate, Tablet moving) {
        if (session != null || !candidate.variant().equals("pre-copy")
                || !candidate.change().unit().equals(moving.id())
                || !candidate.change().from().equals(moving.owner())) return false;
        session = candidate;
        tablet = moving;
        return true;
    }

    private boolean matches(MigrationJob candidate, OwnershipEpoch epoch) {
        return candidate == session && session != null && tablet.epoch().equals(epoch);
    }

    @Override
    public void receive(MigrationJob candidate, OwnershipEpoch epoch, TransferBatch batch) {
        if (!matches(candidate, epoch) || !batch.valid()) return;
        if (snapshots.containsKey(batch.batchId())) return; // 충돌한 중복에는 원본 metadata의 ACK만 반환한다.
        if (snapshotComplete || batch.batchId() != nextBatch) return;
        Set<Bytes> keys = new HashSet<>();
        for (var record : batch.records()) {
            if (!tablet.range().contains(keyHash.applyAsLong(record.key())) || data.containsKey(record.key())
                    || !keys.add(record.key())) return;
        }
        for (var record : batch.records()) data.put(record.key(), record.value());
        snapshots.put(batch.batchId(), Acknowledgement.of(batch));
        nextBatch++;
        snapshotComplete = batch.last();
    }

    @Override
    public Optional<Acknowledgement> acknowledgement(MigrationJob candidate, long batchId) {
        return candidate == session ? Optional.ofNullable(snapshots.get(batchId)) : Optional.empty();
    }

    @Override
    public void receiveDelta(MigrationJob candidate, OwnershipEpoch epoch, DeltaBatch batch) {
        if (!matches(candidate, epoch) || !batch.valid() || !snapshotComplete) return;
        if (deltas.containsKey(batch.batchId())) return;
        if (finalComplete || batch.batchId() != nextBatch) return;
        long expected = lastSequence;
        Set<Bytes> keys = new HashSet<>();
        for (var delta : batch.records()) {
            if (delta.seq() != ++expected || !tablet.range().contains(keyHash.applyAsLong(delta.key()))
                    || !keys.add(delta.key())) return;
        }
        for (var delta : batch.records()) {
            if (delta.seq() > lastSequences.getOrDefault(delta.key(), 0L)) {
                if (delta.value().isPresent()) data.put(delta.key(), delta.value().orElseThrow());
                else data.remove(delta.key());
                lastSequences.put(delta.key(), delta.seq());
            }
        }
        lastSequence = expected;
        deltas.put(batch.batchId(), DeltaBatch.Acknowledgement.of(batch));
        nextBatch++;
        finalComplete = batch.finalRound() && batch.last();
    }

    @Override
    public Optional<DeltaBatch.Acknowledgement> deltaAcknowledgement(MigrationJob candidate, long batchId) {
        return candidate == session ? Optional.ofNullable(deltas.get(batchId)) : Optional.empty();
    }

    @Override public boolean complete(MigrationJob candidate) { return candidate == session && snapshotComplete; }
    @Override public boolean caughtUp(MigrationJob candidate, long finalSeq) {
        return candidate == session && finalComplete && lastSequence == finalSeq;
    }

    /** 호출자가 routing commit 전후의 접근 경계를 관리한다. 반환 map은 불변이다. */
    public Map<Bytes, Bytes> data() {
        // 연속 byte key의 hash가 좁은 범위에 몰릴 수 있어 MapN의 선형 탐색 대신 hash table을 보존한다.
        return java.util.Collections.unmodifiableMap(new LinkedHashMap<>(data));
    }

    @Override
    public void discard(MigrationJob candidate) {
        if (candidate != session) return;
        session = null;
        tablet = null;
        data.clear();
        snapshots.clear();
        deltas.clear();
        lastSequences.clear();
        nextBatch = 0;
        lastSequence = 0;
        snapshotComplete = false;
        finalComplete = false;
    }
}
