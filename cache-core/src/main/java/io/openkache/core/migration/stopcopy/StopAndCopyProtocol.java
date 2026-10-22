package io.openkache.core.migration.stopcopy;

import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.MigrationProtocol;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.TransferProgress;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Pure stop-and-copy baseline. 근거: Live Migration of Virtual Machines (NSDI 2005) §3.1.
 * 프로젝트 설계: experiments/b3-live-migration/design.md §4.1.
 *
 * <p>요청·전송·commit은 같은 owner/controller thread에서 직렬 실행한다.
 * begin에서 freeze하고 전체 batch ACK 뒤 SWITCHING에서 기다린다. 실행기가 외부
 * MigrationCommitter로 commit한 뒤 committed를 호출하면 source cleanup을 시작한다.
 * 실제 시계·network I/O를 사용하지 않는다.
 */
public final class StopAndCopyProtocol implements MigrationProtocol {
    public static final int DEFAULT_BATCH_BYTES = 64 * 1024;

    /** 같은 worker에서 호출하는 source 경계. KvEngine 공통 API를 변경하지 않는다. */
    public interface Source {
        /** 다른 job이 freeze 중이면 false. 성공한 job만 unfreeze한다. */
        boolean freeze(Tablet tablet);
        /** Tablet write가 여전히 차단돼 있는지. */
        boolean frozen(Tablet tablet);
        /** freeze 뒤 얻는 고정 순서 snapshot. */
        List<TransferBatch.Record> snapshot(Tablet tablet);
        /** 이 job이 획득한 freeze를 해제한다. */
        void unfreeze(Tablet tablet);
        /** 이 Tablet만 삭제. 부분 삭제·실패는 false이며 반복 호출해도 안전해야 한다. */
        boolean cleanup(Tablet tablet);
    }

    /**
     * 같은 worker의 staging/전송 경계. session은 begin 입력 객체로 job을 구별한다.
     * receive에서 epoch·범위·순서·checksum을 검증하고 중복 적용은 멱등이어야 한다.
     * staging은 routing commit 전 client에 노출하지 않는다.
     */
    public interface Destination {
        boolean prepare(MigrationJob session, Tablet tablet);
        /** batch 전체가 전송된 뒤 한 번 호출한다. */
        void receive(MigrationJob session, OwnershipEpoch epoch, TransferBatch batch);
        /** 적용이 끝난 batch만 ACK. 지연·누락은 빈 값으로 표현한다. */
        Optional<Acknowledgement> acknowledgement(MigrationJob session, long batchId);
        /** 마지막 batch까지 검증·적용했는지. */
        boolean complete(MigrationJob session);
        /** commit 전 이 session의 staging만 폐기한다. */
        void discard(MigrationJob session);
    }

    /** 적용 완료한 batch metadata. ACK bytes는 현재 크기 모델에서 제외한다. */
    public record Acknowledgement(long batchId, int recordCount, long serializedBytes, long checksum) {
        public static Acknowledgement of(TransferBatch batch) {
            return new Acknowledgement(batch.batchId(), batch.recordCount(), batch.serializedBytes(), batch.checksum());
        }

        public boolean matches(TransferBatch batch) { return equals(of(batch)); }
    }

    private final Source source;
    private final Destination destination;
    private final RoutingTable expectedTable;
    private final int batchBytes;
    private MigrationJob session;
    private MigrationJob job;
    private List<TransferBatch.Record> snapshot = List.of();
    private int offset;
    private TransferBatch pending;
    private long paidBytes;
    private boolean dispatched;
    private boolean prepared;
    private boolean ownsFreeze;

    public StopAndCopyProtocol(Source source, Destination destination, RoutingTable expectedTable) {
        this(source, destination, expectedTable, DEFAULT_BATCH_BYTES);
    }

    /** batchBytes는 헤더 포함 목표 크기. 큰 record도 여러 tick의 예산으로 전송한다. */
    public StopAndCopyProtocol(Source source, Destination destination, RoutingTable expectedTable, int batchBytes) {
        this.source = Objects.requireNonNull(source, "source");
        this.destination = Objects.requireNonNull(destination, "destination");
        this.expectedTable = Objects.requireNonNull(expectedTable, "expectedTable");
        if (batchBytes <= TransferBatch.HEADER_BYTES) throw new IllegalArgumentException("batch too small");
        this.batchBytes = batchBytes;
    }

    @Override public String name() { return "stop-and-copy"; }

    /** 최신 불변 job 상태. begin 전에는 null. */
    public MigrationJob job() { return job; }

    /** 현재 상태. begin 전에는 PENDING. */
    public JobState state() { return job == null ? JobState.PENDING : job.state(); }

    /** coordinator에 전달할 시작 시 예상 table. */
    public RoutingTable expectedTable() { return expectedTable; }

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
        prepared = destination.prepare(session, tablet());
        if (!prepared) return abort();
        ownsFreeze = source.freeze(tablet());
        if (!ownsFreeze) return abort();
        try {
            snapshot = List.copyOf(source.snapshot(tablet()));
            update(JobState.COPYING, job.progress());
            return state();
        } catch (RuntimeException failure) {
            abort();
            throw failure;
        }
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
        if (state() != JobState.COPYING) return state();
        if (!source.frozen(tablet())) return abort();
        long remaining = budgetBytes;
        while (state() == JobState.COPYING) {
            if (pending == null) pending = nextBatch();
            long used = Math.min(remaining, pending.serializedBytes() - paidBytes);
            remaining -= used;
            paidBytes += used;
            TransferProgress p = job.progress();
            update(state(), new TransferProgress(false, p.lastSentBatchId(), p.lastAckBatchId(),
                    Math.addExact(p.sentBytes(), used)));
            if (paidBytes != pending.serializedBytes()) break;
            if (!dispatched) {
                destination.receive(session, tablet().epoch(), pending);
                dispatched = true;
                p = job.progress();
                update(state(), new TransferProgress(false, pending.batchId(), p.lastAckBatchId(), p.sentBytes()));
            }
            Optional<Acknowledgement> ack = destination.acknowledgement(session, pending.batchId());
            if (ack.isEmpty() || !ack.get().matches(pending)) break;
            p = job.progress();
            update(pending.last() ? JobState.SWITCHING : JobState.COPYING,
                    new TransferProgress(pending.last(), pending.batchId(), pending.batchId(), p.sentBytes()));
            if (pending.last()) snapshot = List.of();
            pending = null;
            paidBytes = 0;
            dispatched = false;
            if (remaining == 0) break;
        }
        return state();
    }

    private TransferBatch nextBatch() {
        List<TransferBatch.Record> records = new ArrayList<>();
        long bytes = TransferBatch.HEADER_BYTES;
        while (offset < snapshot.size()) {
            TransferBatch.Record record = snapshot.get(offset);
            if (!records.isEmpty() && bytes + record.serializedBytes() > batchBytes) break;
            records.add(record);
            bytes += record.serializedBytes();
            offset++;
        }
        return TransferBatch.create(job.progress().lastAckBatchId() + 1, offset == snapshot.size(), records);
    }

    /** 외부 실행기가 commit 직전에 확인한다. */
    public boolean readyToCommit() {
        return state() == JobState.SWITCHING && job.progress().snapshotComplete()
                && source.frozen(tablet()) && destination.complete(session);
    }

    /**
     * 외부 committer 성공 후 게시된 실제 table로 확인한다. 예상 소유권/버전과 다르면 정리를 시작하지 않는다.
     * 실행기는 CAS와 이 확인 사이에 다른 작업을 끼워 넣으면 안 된다.
     */
    public void committed(RoutingTable publishedTable) {
        Objects.requireNonNull(publishedTable, "publishedTable");
        if (!readyToCommit()) throw new IllegalStateException("not ready to commit");
        RoutingTable planned = expectedTable.apply(job.change());
        if (!publishedTable.version().equals(planned.version())
                || !publishedTable.tablets().equals(planned.tablets())
                || publishedTable.nextTabletId() != planned.nextTabletId()) {
            throw new IllegalArgumentException("ownership was not committed as planned");
        }
        update(JobState.CLEANING_UP, job.progress());
    }

    @Override
    public boolean acceptsWrite() {
        return (state() == JobState.PENDING || state() == JobState.ABORTED)
                && (session == null || !source.frozen(tablet()));
    }

    @Override
    public JobState abort() {
        if (job == null) throw new IllegalStateException("begin first");
        if (state() == JobState.CLEANING_UP || state() == JobState.DONE || state() == JobState.ABORTED) return state();
        if (prepared) destination.discard(session);
        if (ownsFreeze) source.unfreeze(tablet());
        prepared = false;
        ownsFreeze = false;
        snapshot = List.of();
        pending = null;
        update(JobState.ABORTED, job.progress());
        return state();
    }

    private void update(JobState state, TransferProgress progress) {
        job = new MigrationJob(session.change(), name(), state, progress);
    }
}
