package io.openkache.core.migration.precopy;

import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.MigrationProtocol;
import io.openkache.core.migration.MigrationPublication;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.TransferProgress;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Snapshot → dirty generation 반복 → freeze·final delta → 외부 commit을 수행한다.
 * 설계 §4.2의 source-owned pre-copy이며 실제 시계나 I/O를 사용하지 않는다.
 * source write·hook·step·commit은 동일 owner/controller thread에서 직렬 실행해야 한다.
 */
public final class PreCopyProtocol implements MigrationProtocol {
    /** source adapter는 성공한 SET/DELETE 직후, 다음 작업 전에 hook을 호출한다. */
    public interface Source extends StopAndCopyProtocol.Source {
        /** 다른 추적/freeze가 있으면 false. 등록 성공한 job만 stopTracking을 호출한다. */
        boolean startTracking(Tablet tablet, Consumer<Bytes> onWrite);
        void stopTracking(Tablet tablet);
        /** 추적 시작 뒤 write를 허용한 상태에서도 호출 가능한 일관된 snapshot. */
        @Override List<TransferBatch.Record> snapshot(Tablet tablet);
        /** 현재 값. 없으면 tombstone으로 전송한다. */
        Optional<Bytes> read(Tablet tablet, Bytes key);
    }

    /** snapshot ACK barrier 뒤 seq 순서로 delta를 적용하는 staging 경계. */
    public interface Destination extends StopAndCopyProtocol.Destination {
        void receiveDelta(MigrationJob session, OwnershipEpoch epoch, DeltaBatch batch);
        Optional<DeltaBatch.Acknowledgement> deltaAcknowledgement(MigrationJob session, long batchId);
        /** finalRound 마지막 batch를 적용하고 finalSeq까지 검증했는지. */
        boolean caughtUp(MigrationJob session, long finalSeq);
    }

    /** 임계 바이트 수는 헤더 포함, maxRounds는 쓰기를 허용하는 delta 라운드 상한이다. */
    public record Settings(int batchBytes, long stopThresholdBytes, int maxRounds) {
        public Settings {
            if (batchBytes <= DeltaBatch.HEADER_BYTES || stopThresholdBytes < 0 || maxRounds < 0) {
                throw new IllegalArgumentException("invalid pre-copy settings");
            }
        }
        public static Settings defaults() { return new Settings(64 * 1024, 256 * 1024, 10); }
    }

    public enum StopReason { THRESHOLD, ROUND_LIMIT, NOT_SHRINKING }

    /** STARTED는 generation 교체 직후, ACKNOWLEDGED는 마지막 ACK 뒤, ABORTED는 중단 직전이다. */
    public enum RoundEventType { STARTED, ACKNOWLEDGED, ABORTED }

    /**
     * 값 payload를 보관하지 않는 불변 라운드 관측. 모든 byte 값은 헤더를 포함한다.
     * dirtyBytes는 시작 경계에서 계산한 예상량이고 roundSentBytes는 실제 지급된 전송량이다.
     * generation은 final을 포함해 1부터 증가하며 liveRound는 완료 여부와 무관한 live 라운드 번호다.
     * previousDirtyBytes는 직전 live generation 예상량이며 첫 generation에서는 null이다.
     * tombstoneKeys는 시작 경계의 삭제 수이며 live 중 값이 바뀌면 실제 전송 레코드 종류와 다를 수 있다.
     * sequenceBefore/lastSequence는 생성 전/후 delta seq, acknowledgedSequence는 실제 확인된 seq다.
     * 프로토콜은 시계를 읽지 않는다. caller가 같은 controller 작업 안에서 가상 tick 또는 실제 시각을 붙인다.
     */
    public record RoundEvent(int generation, int liveRound, boolean finalRound, RoundEventType event,
            int dirtyKeys, int tombstoneKeys, long dirtyBytes, Long previousDirtyBytes, StopReason stopReason,
            long roundSentBytes, long sentBytes, long sequenceBefore, long lastSequence, long acknowledgedSequence) {}

    private record RoundStart(int generation, int liveRound, boolean finalRound, int dirtyKeys, int tombstoneKeys,
            long dirtyBytes, Long previousDirtyBytes, StopReason reason, long sentBytes, long sequence) {}

    private final java.util.function.ToLongFunction<Bytes> keyHash;
    private final Source source;
    private final Destination destination;
    private final RoutingTable expectedTable;
    private final Settings settings;
    private MigrationJob session;
    private MigrationJob job;
    private boolean prepared;
    private boolean tracking;
    private boolean ownsFreeze;
    private Set<Bytes> collecting = new LinkedHashSet<>();
    private List<Bytes> sending = List.of();
    private List<TransferBatch.Record> snapshot = List.of();
    private int offset;
    private TransferBatch pendingSnapshot;
    private DeltaBatch pendingDelta;
    private long paidBytes;
    private boolean dispatched;
    private long sequence;
    private long acknowledgedSequence;
    private long previousDirtyBytes = Long.MAX_VALUE;
    private int rounds;
    private StopReason stopReason;
    private final List<RoundEvent> roundEvents = new ArrayList<>();
    private RoundStart activeRound;
    private int generation;

    public PreCopyProtocol(Source source, Destination destination, RoutingTable expectedTable) {
        this(source, destination, expectedTable, Settings.defaults());
    }

    public PreCopyProtocol(Source source, Destination destination, RoutingTable expectedTable, Settings settings) {
        this(source, destination, expectedTable, settings, KeyHash::of);
    }

    /** B1의 key→점 변환을 보존한다. 변환은 불변이며 모든 호출은 같은 controller thread에서 한다. */
    public PreCopyProtocol(Source source, Destination destination, RoutingTable expectedTable, Settings settings,
                           java.util.function.ToLongFunction<Bytes> keyHash) {
        this.keyHash = Objects.requireNonNull(keyHash);
        this.source = Objects.requireNonNull(source, "source");
        this.destination = Objects.requireNonNull(destination, "destination");
        this.expectedTable = Objects.requireNonNull(expectedTable, "expectedTable");
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    @Override public String name() { return "pre-copy"; }
    public MigrationJob job() { return job; }
    public JobState state() { return job == null ? JobState.PENDING : job.state(); }
    public RoutingTable expectedTable() { return expectedTable; }
    public int rounds() { return rounds; }
    public long acknowledgedSequence() { return acknowledgedSequence; }
    public Optional<StopReason> stopReason() { return Optional.ofNullable(stopReason); }
    /** 동일 controller thread에서 읽는다. 이후 진행·취소에 영향받지 않는 관측 목록의 복사본이다. */
    public List<RoundEvent> roundEvents() { return List.copyOf(roundEvents); }
    private Tablet tablet() { return expectedTable.get(session.change().unit()); }

    @Override
    public JobState begin(MigrationJob candidate) {
        Objects.requireNonNull(candidate, "job");
        if (job != null) throw new IllegalStateException("already begun");
        if (candidate.state() != JobState.PENDING || !candidate.progress().equals(TransferProgress.initial())
                || !candidate.variant().equals(name())
                || !expectedTable.get(candidate.change().unit()).owner().equals(candidate.change().from())) {
            throw new IllegalArgumentException("invalid pending job");
        }
        session = candidate;
        job = candidate;
        try {
            prepared = destination.prepare(session, tablet());
            if (!prepared) return abort();
            tracking = source.startTracking(tablet(), this::onSourceWrite);
            if (!tracking) return abort();
            snapshot = List.copyOf(source.snapshot(tablet()));
            update(JobState.COPYING, job.progress());
            return state();
        } catch (RuntimeException failure) {
            abort();
            throw failure;
        }
    }

    private void onSourceWrite(Bytes key) {
        Objects.requireNonNull(key, "key");
        if (tracking && tablet().range().contains(keyHash.applyAsLong(key))) collecting.add(key);
    }

    @Override
    public JobState step(long budgetBytes) {
        if (budgetBytes < 0) throw new IllegalArgumentException("negative budget");
        if (job == null) throw new IllegalStateException("begin first");
        if (state() == JobState.CLEANING_UP) {
            if (source.cleanup(tablet())) {
                source.unfreeze(tablet());
                ownsFreeze = false;
                update(JobState.DONE, job.progress());
            }
            return state();
        }
        if (state() == JobState.DONE || state() == JobState.ABORTED) return state();
        if (ownsFreeze && !source.frozen(tablet())) return abort();
        if (state() == JobState.SWITCHING && job.progress().snapshotComplete()) return state();
        long remaining = budgetBytes;
        while (true) {
            boolean copying = state() == JobState.COPYING;
            if (copying && pendingSnapshot == null) pendingSnapshot = nextSnapshot();
            if (!copying && pendingDelta == null) pendingDelta = nextDelta();
            long size = copying ? pendingSnapshot.serializedBytes() : pendingDelta.serializedBytes();
            long id = copying ? pendingSnapshot.batchId() : pendingDelta.batchId();
            long used = Math.min(remaining, size - paidBytes);
            paidBytes += used;
            remaining -= used;
            TransferProgress p = job.progress();
            update(state(), new TransferProgress(false, p.lastSentBatchId(), p.lastAckBatchId(),
                    Math.addExact(p.sentBytes(), used)));
            if (paidBytes != size) break;
            if (!dispatched) {
                if (copying) destination.receive(session, tablet().epoch(), pendingSnapshot);
                else destination.receiveDelta(session, tablet().epoch(), pendingDelta);
                dispatched = true;
                update(state(), new TransferProgress(false, id, job.progress().lastAckBatchId(), job.progress().sentBytes()));
            }
            boolean acknowledged = copying
                    ? destination.acknowledgement(session, id).filter(ack -> ack.matches(pendingSnapshot)).isPresent()
                    : destination.deltaAcknowledgement(session, id).filter(ack -> ack.matches(pendingDelta)).isPresent();
            if (!acknowledged) break;
            boolean last = copying ? pendingSnapshot.last() : pendingDelta.last();
            boolean finalBatch = !copying && pendingDelta.finalRound() && last;
            if (!copying && !pendingDelta.records().isEmpty()) {
                acknowledgedSequence = pendingDelta.records().getLast().seq();
            }
            update(state(), new TransferProgress(finalBatch, id, id, job.progress().sentBytes()));
            pendingSnapshot = null;
            pendingDelta = null;
            paidBytes = 0;
            dispatched = false;
            if (last) {
                if (!copying) {
                    observeRound(RoundEventType.ACKNOWLEDGED);
                    activeRound = null;
                }
                if (!finalBatch) prepareRound();
                break; // 라운드 사이에 caller가 write를 공급할 수 있도록 다음 tick까지 기다린다.
            }
            if (remaining == 0) break;
        }
        return state();
    }

    private TransferBatch nextSnapshot() {
        List<TransferBatch.Record> records = new ArrayList<>();
        long bytes = TransferBatch.HEADER_BYTES;
        while (offset < snapshot.size()) {
            var record = snapshot.get(offset);
            if (!records.isEmpty() && bytes + record.serializedBytes() > settings.batchBytes()) break;
            records.add(record);
            bytes += record.serializedBytes();
            offset++;
        }
        return TransferBatch.create(job.progress().lastAckBatchId() + 1, offset == snapshot.size(), records);
    }

    private DeltaBatch nextDelta() {
        List<DeltaBatch.Delta> records = new ArrayList<>();
        long bytes = DeltaBatch.HEADER_BYTES;
        while (offset < sending.size()) {
            Bytes key = sending.get(offset);
            Optional<Bytes> value = source.read(tablet(), key);
            long size = 17L + key.size() + value.map(Bytes::size).orElse(0);
            if (!records.isEmpty() && bytes + size > settings.batchBytes()) break;
            sequence = Math.incrementExact(sequence);
            records.add(new DeltaBatch.Delta(sequence, key, value));
            bytes += size;
            offset++;
        }
        return DeltaBatch.create(job.progress().lastAckBatchId() + 1, offset == sending.size(),
                state() == JobState.SWITCHING, records);
    }

    private void prepareRound() {
        long dirtyBytes = DeltaBatch.HEADER_BYTES;
        long batchSize = DeltaBatch.HEADER_BYTES;
        int tombstones = 0;
        for (Bytes key : collecting) {
            Optional<Bytes> value = source.read(tablet(), key);
            if (value.isEmpty()) tombstones++;
            long size = 17L + key.size() + value.map(Bytes::size).orElse(0);
            if (batchSize > DeltaBatch.HEADER_BYTES && batchSize + size > settings.batchBytes()) {
                dirtyBytes = Math.addExact(dirtyBytes, DeltaBatch.HEADER_BYTES);
                batchSize = DeltaBatch.HEADER_BYTES;
            }
            dirtyBytes = Math.addExact(dirtyBytes, size);
            batchSize += size;
        }
        StopReason reason = dirtyBytes <= settings.stopThresholdBytes() ? StopReason.THRESHOLD
                : rounds >= settings.maxRounds() ? StopReason.ROUND_LIMIT
                : dirtyBytes >= previousDirtyBytes ? StopReason.NOT_SHRINKING : null;
        Long previous = generation == 0 ? null : previousDirtyBytes;
        if (reason != null) {
            ownsFreeze = source.freeze(tablet());
            if (!ownsFreeze) { abort(); return; }
            stopReason = reason;
            source.stopTracking(tablet());
            tracking = false;
            update(JobState.SWITCHING, job.progress());
        } else {
            rounds++;
            previousDirtyBytes = dirtyBytes;
            update(JobState.CATCHING_UP, job.progress());
        }
        sending = List.copyOf(collecting);
        activeRound = new RoundStart(++generation, rounds, reason != null, sending.size(), tombstones,
                dirtyBytes, previous, reason, job.progress().sentBytes(), sequence);
        observeRound(RoundEventType.STARTED);
        collecting = new LinkedHashSet<>();
        snapshot = List.of();
        offset = 0;
    }

    private void observeRound(RoundEventType event) {
        if (activeRound == null) return;
        RoundStart r = activeRound;
        roundEvents.add(new RoundEvent(r.generation(), r.liveRound(), r.finalRound(), event,
                r.dirtyKeys(), r.tombstoneKeys(), r.dirtyBytes(), r.previousDirtyBytes(), r.reason(),
                job.progress().sentBytes() - r.sentBytes(), job.progress().sentBytes(),
                r.sequence(), sequence, acknowledgedSequence));
    }

    /** snapshot과 final seq ACK 및 freeze가 모두 유지될 때만 외부 CAS를 요청한다. */
    public boolean readyToCommit() {
        return state() == JobState.SWITCHING && job.progress().snapshotComplete()
                && source.frozen(tablet()) && destination.complete(session)
                && destination.caughtUp(session, acknowledgedSequence);
    }

    /** 외부 CAS 직후 다른 작업을 끼우지 않고 실제 게시된 table로 호출한다. */
    public void committed(RoutingTable publishedTable) {
        committed(expectedTable, publishedTable);
    }

    /** 다른 Tablet의 전환을 보존하는 실제 CAS 전·후 표를 받는다. 같은 controller 작업에서 호출한다. */
    public void committed(RoutingTable before, RoutingTable publishedTable) {
        if (!readyToCommit()) throw new IllegalStateException("not ready to commit");
        MigrationPublication.verify(session, expectedTable, before, publishedTable);
        update(JobState.CLEANING_UP, job.progress());
    }

    @Override
    public boolean acceptsWrite() {
        return (state() == JobState.PENDING || state() == JobState.COPYING
                || state() == JobState.CATCHING_UP || state() == JobState.ABORTED)
                && (session == null || !source.frozen(tablet()));
    }

    @Override
    public JobState abort() {
        if (job == null) throw new IllegalStateException("begin first");
        if (state() == JobState.CLEANING_UP || state() == JobState.DONE || state() == JobState.ABORTED) return state();
        observeRound(RoundEventType.ABORTED);
        activeRound = null;
        if (prepared) destination.discard(session);
        if (tracking) source.stopTracking(tablet());
        if (ownsFreeze) source.unfreeze(tablet());
        prepared = false;
        tracking = false;
        ownsFreeze = false;
        snapshot = List.of();
        sending = List.of();
        collecting.clear();
        pendingSnapshot = null;
        pendingDelta = null;
        update(JobState.ABORTED, job.progress());
        return state();
    }

    private void update(JobState state, TransferProgress progress) {
        job = new MigrationJob(session.change(), name(), state, progress);
    }
}
