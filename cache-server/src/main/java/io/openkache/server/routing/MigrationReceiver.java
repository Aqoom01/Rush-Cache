package io.openkache.server.routing;

import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.precopy.DeltaBatch;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.TabletRange;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * destination 쪽 TRANSFER 처리. 받은 batch를 바로 자기 저장소에 적재한다. commit 전에는 이 노드가 Tablet 주인이 아니어서
 * 클라이언트 요청이 MOVED로 돌아가므로 적재한 값이 밖에 보이지 않는다(B3 Destination의 staging 계약).
 *
 * <p>worker thread 전용. batch id는 job 안에서 snapshot·delta가 함께 쓰는 하나의 증가 순서이고, 이미 적용한 id는 다시 적용하지
 * 않고 OK로 답한다(재전송 멱등).
 */
final class MigrationReceiver {

    private record Key(long from, long jobId) {}

    private static final class Session {
        final long tabletId;
        final long epoch;
        final TabletRange range;
        long lastBatch = Long.MIN_VALUE;
        long lastSeq;

        Session(long tabletId, long epoch, TabletRange range) {
            this.tabletId = tabletId;
            this.epoch = epoch;
            this.range = range;
        }
    }

    private final NodeId self;
    private final NodeStore store;
    private final Map<Key, Session> sessions = new HashMap<>();

    MigrationReceiver(NodeId self, NodeStore store) {
        this.self = self;
        this.store = store;
    }

    /** 적용했으면 빈 값, 거절하면 이유. */
    Optional<String> handle(MigrationWire.Transfer t, RoutingTable table) {
        MigrationWire.Header h = t.header();
        if (h.to() != self.value()) return Optional.of("misdirected transfer");
        Key key = new Key(h.from(), h.jobId());
        if (h.kind() == MigrationWire.PREPARE) return Optional.of("prepare must go through prepare()");
        Session s = sessions.get(key);
        if (s == null) return Optional.of("unknown migration session");
        if (s.tabletId != h.tabletId() || s.epoch != h.epoch()) return Optional.of("tablet or epoch mismatch");
        switch (h.kind()) {
            case MigrationWire.DISCARD -> {
                sessions.remove(key);
                if (!owns(table, s.range)) store.deleteRange(s.range);
                return Optional.empty();
            }
            case MigrationWire.SNAPSHOT -> {
                TransferBatch b = t.snapshot();
                if (s.lastBatch != Long.MIN_VALUE && b.batchId() <= s.lastBatch) return Optional.empty();
                if (s.lastBatch != Long.MIN_VALUE && b.batchId() != s.lastBatch + 1) return Optional.of("batch gap");
                for (TransferBatch.Record r : b.records()) {
                    if (!s.range.contains(KeyHash.of(r.key()))) return Optional.of("key outside tablet range");
                }
                for (TransferBatch.Record r : b.records()) store.load(r.key(), Optional.of(r.value()));
                s.lastBatch = b.batchId();
                return Optional.empty();
            }
            case MigrationWire.DELTA -> {
                DeltaBatch b = t.delta();
                if (s.lastBatch != Long.MIN_VALUE && b.batchId() <= s.lastBatch) return Optional.empty();
                if (s.lastBatch == Long.MIN_VALUE || b.batchId() != s.lastBatch + 1) return Optional.of("batch gap");
                long seq = s.lastSeq;
                for (DeltaBatch.Delta d : b.records()) {
                    if (!s.range.contains(KeyHash.of(d.key()))) return Optional.of("key outside tablet range");
                    if (d.seq() <= seq) return Optional.of("delta sequence not increasing");
                    seq = d.seq();
                }
                for (DeltaBatch.Delta d : b.records()) store.load(d.key(), d.value());
                s.lastSeq = seq;
                s.lastBatch = b.batchId();
                return Optional.empty();
            }
            default -> {
                return Optional.of("unknown transfer kind");
            }
        }
    }

    /**
     * PREPARE: 범위를 비운 뒤 session을 연다. 범위 비우기는 저장소 전체를 훑으므로 background에서 하고, 끝나면 worker에서 session을
     * 열고 {@code reply}로 응답한다(source는 이 응답 뒤에만 batch를 보낸다). worker thread에서 호출한다. background 작업은 한
     * thread에서 순서대로 실행되므로 앞서 시작한 source cleanup이 끝난 뒤에 돈다.
     */
    void prepare(MigrationWire.Transfer t, RoutingTable table, io.openkache.core.runtime.OwnerWorker worker,
                 java.util.function.Consumer<Optional<String>> reply) {
        MigrationWire.Header h = t.header();
        if (h.to() != self.value()) {
            reply.accept(Optional.of("misdirected transfer"));
            return;
        }
        if (owns(table, t.range())) {
            reply.accept(Optional.of("destination already owns the range"));
            return;
        }
        Key key = new Key(h.from(), h.jobId());
        store.inBackground(() -> {
            // 이전 소유 기간이나 취소된 이동이 남긴 값을 지우고 빈 범위에서 시작한다. 이 노드는 주인이 아니라 그 범위에 쓰지 않는다
            store.deleteRange(t.range());
            boolean offered = worker.offer(() -> {
                sessions.values().removeIf(s -> s.tabletId == h.tabletId());
                sessions.put(key, new Session(h.tabletId(), h.epoch(), t.range()));
                reply.accept(Optional.empty());
            });
            if (!offered) reply.accept(Optional.of("worker queue full"));
        });
    }

    private boolean owns(RoutingTable table, TabletRange range) {
        return table.lookup(range.start()).owner().equals(self);
    }
}
