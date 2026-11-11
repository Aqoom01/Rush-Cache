package io.openkache.server.routing;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.precopy.DeltaBatch;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** 미리 만든 snapshot·미리 켠 쓰기 추적이 실제 B3 protocol과 맞물려 begin 전 쓰기를 잃지 않는지 확인한다. */
class NodeStoreTest {

    /** 받은 batch를 바로 적용하고 바로 ACK하는 메모리 destination. */
    static final class MemoryDestination implements PreCopyProtocol.Destination {
        final Map<Bytes, Bytes> data = new HashMap<>();
        final Map<Long, StopAndCopyProtocol.Acknowledgement> acks = new HashMap<>();
        final Map<Long, DeltaBatch.Acknowledgement> deltaAcks = new HashMap<>();
        boolean complete;
        boolean finalAcked;
        long seq;

        @Override public boolean prepare(MigrationJob s, Tablet t) { return true; }
        @Override public void receive(MigrationJob s, OwnershipEpoch e, TransferBatch b) {
            b.records().forEach(r -> data.put(r.key(), r.value()));
            acks.put(b.batchId(), StopAndCopyProtocol.Acknowledgement.of(b));
            if (b.last()) complete = true;
        }
        @Override public Optional<StopAndCopyProtocol.Acknowledgement> acknowledgement(MigrationJob s, long id) {
            return Optional.ofNullable(acks.get(id));
        }
        @Override public boolean complete(MigrationJob s) { return complete; }
        @Override public void discard(MigrationJob s) { data.clear(); }
        @Override public void receiveDelta(MigrationJob s, OwnershipEpoch e, DeltaBatch b) {
            for (DeltaBatch.Delta d : b.records()) {
                if (d.value().isPresent()) data.put(d.key(), d.value().get());
                else data.remove(d.key());
                seq = d.seq();
            }
            deltaAcks.put(b.batchId(), DeltaBatch.Acknowledgement.of(b));
            if (b.finalRound() && b.last()) finalAcked = true;
        }
        @Override public Optional<DeltaBatch.Acknowledgement> deltaAcknowledgement(MigrationJob s, long id) {
            return Optional.ofNullable(deltaAcks.get(id));
        }
        @Override public boolean caughtUp(MigrationJob s, long finalSeq) { return finalAcked && seq >= finalSeq; }
    }

    @Test
    void writesBetweenPreTrackAndBeginReachTheDestination() {
        RoutingTable table = new HashRangeScheme(2).initial(List.of(new NodeId(0), new NodeId(1)));
        Tablet tablet = table.get(new io.openkache.core.model.TabletId(0));
        NodeStore store = new NodeStore();
        try {
            List<Bytes> keys = new ArrayList<>();
            for (int i = 0; keys.size() < 50; i++) {
                Bytes key = Bytes.utf8("key-" + i);
                if (tablet.range().contains(KeyHash.of(key))) keys.add(key);
            }
            keys.forEach(k -> store.set(k, Bytes.utf8("old")));

            assertTrue(store.preTrack(tablet));
            List<TransferBatch.Record> snapshot = store.scan(tablet.range());
            // snapshot을 만든 뒤, begin 전에 일어난 쓰기
            store.set(keys.get(0), Bytes.utf8("new"));
            store.delete(keys.get(1));

            MemoryDestination destination = new MemoryDestination();
            PreCopyProtocol protocol = new PreCopyProtocol(store, destination, table,
                    new PreCopyProtocol.Settings(4096, 0, 3));
            store.prepareSnapshot(tablet, snapshot);
            assertEquals(JobState.COPYING,
                    protocol.begin(MigrationJob.pending(new Change(tablet.id(), new NodeId(0), new NodeId(1)), "pre-copy")));
            store.cancelPrepared(tablet);
            for (int i = 0; i < 100 && !protocol.readyToCommit(); i++) protocol.step(Long.MAX_VALUE / 4);
            assertTrue(protocol.readyToCommit());

            assertEquals(Bytes.utf8("new"), destination.data.get(keys.get(0)));
            assertFalse(destination.data.containsKey(keys.get(1)));
            assertEquals(Bytes.utf8("old"), destination.data.get(keys.get(2)));
        } finally {
            store.close();
        }
    }

    @Test
    void reservedFreezeIsTakenByStopAndCopyBegin() {
        RoutingTable table = new HashRangeScheme(2).initial(List.of(new NodeId(0), new NodeId(1)));
        Tablet tablet = table.get(new io.openkache.core.model.TabletId(0));
        NodeStore store = new NodeStore();
        try {
            assertTrue(store.reserveFreeze(tablet));
            assertTrue(store.writeBlocked(tablet.id()));
            StopAndCopyProtocol protocol = new StopAndCopyProtocol(store, new MemoryDestination(), table);
            store.prepareSnapshot(tablet, store.scan(tablet.range()));
            assertEquals(JobState.COPYING, protocol.begin(
                    MigrationJob.pending(new Change(tablet.id(), new NodeId(0), new NodeId(1)), "stop-and-copy")));
            store.cancelPrepared(tablet);
            // 예약을 begin이 가져갔으므로 cancel이 freeze를 풀지 않는다
            assertTrue(store.writeBlocked(tablet.id()));
            assertEquals(JobState.ABORTED, protocol.abort());
            assertFalse(store.writeBlocked(tablet.id()));
        } finally {
            store.close();
        }
    }
}
