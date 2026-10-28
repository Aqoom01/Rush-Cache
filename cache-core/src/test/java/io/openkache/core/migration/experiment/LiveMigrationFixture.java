package io.openkache.core.migration.experiment;

import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.postcopy.PostCopyProtocol;
import io.openkache.core.migration.postcopy.PullReply;
import io.openkache.core.migration.precopy.DeltaBatch;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.migration.precopy.PreCopyReceiver;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol.Acknowledgement;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/** 같은 직렬 controller에서 실행하는 source·전송 지연·원자적 routing 교체 fixture. */
public final class LiveMigrationFixture {
    public static final NodeId SOURCE = new NodeId(1);
    public static final NodeId DESTINATION = new NodeId(2);
    public static final TabletId MOVING = new TabletId(0);
    public static final TabletId OTHER = new TabletId(1);
    public final Store source = new Store();
    public final Link link = new Link();
    public RoutingTable current;
    public int commits;

    public LiveMigrationFixture(boolean twoTablets) {
        List<Tablet> tablets = new ArrayList<>();
        var ranges = TabletRange.uniform(twoTablets ? 2 : 1);
        for (int i = 0; i < ranges.size(); i++) {
            tablets.add(new Tablet(new TabletId(i), ranges.get(i), SOURCE, OwnershipEpoch.ZERO));
        }
        current = RoutingTable.of(RoutingVersion.ZERO, tablets, tablets.size());
    }

    public MigrationJob job(String variant) {
        return MigrationJob.pending(new Change(MOVING, SOURCE, DESTINATION), variant);
    }

    public boolean commit(PreCopyProtocol protocol) {
        if (!protocol.readyToCommit() || current != protocol.expectedTable()) return false;
        current = current.apply(protocol.job().change());
        commits++;
        protocol.committed(current);
        return true;
    }

    public boolean commit(PostCopyProtocol protocol, MigrationJob job) {
        if (!protocol.readyToCommit() || current != protocol.expectedTable()) return false;
        current = current.apply(job.change());
        commits++;
        protocol.committed(current);
        return true;
    }

    public Map<Bytes, Bytes> movingData() {
        Map<Bytes, Bytes> result = new LinkedHashMap<>();
        source.data.forEach((key, value) -> {
            if (current.get(MOVING).range().contains(KeyHash.of(key))) result.put(key, value);
        });
        return result;
    }

    public final class Store implements PreCopyProtocol.Source, PostCopyProtocol.Source {
        public final Map<Bytes, Bytes> data = new LinkedHashMap<>();
        private final Map<TabletId, Tablet> frozen = new HashMap<>();
        private Tablet tracked;
        private Consumer<Bytes> hook;
        public boolean available = true;
        public boolean dropPull;
        public int cleanupFailures;
        public boolean partialCleanup;
        public int cleanupCalls;
        public final List<Bytes> pulled = new ArrayList<>();
        public UnaryOperator<PullReply> transform = UnaryOperator.identity();

        public void put(Bytes key, Bytes value) {
            mutate(key, Optional.of(value));
        }

        public void delete(Bytes key) { mutate(key, Optional.empty()); }

        private void mutate(Bytes key, Optional<Bytes> value) {
            Tablet owner = current.lookup(KeyHash.of(key));
            if (!owner.owner().equals(SOURCE) || frozen.containsKey(owner.id())) {
                throw new IllegalStateException("source is not writable");
            }
            if (value.isPresent()) data.put(key, value.orElseThrow());
            else data.remove(key);
            if (tracked != null && tracked.range().contains(KeyHash.of(key))) hook.accept(key);
        }

        @Override public boolean freeze(Tablet tablet) { return frozen.putIfAbsent(tablet.id(), tablet) == null; }
        @Override public boolean frozen(Tablet tablet) { return tablet.equals(frozen.get(tablet.id())); }
        @Override public void unfreeze(Tablet tablet) { frozen.remove(tablet.id(), tablet); }
        @Override public boolean available(Tablet tablet) { return available; }

        @Override
        public boolean startTracking(Tablet tablet, Consumer<Bytes> onWrite) {
            if (tracked != null || frozen(tablet)) return false;
            tracked = tablet;
            hook = onWrite;
            return true;
        }

        @Override public void stopTracking(Tablet tablet) {
            if (tablet.equals(tracked)) { tracked = null; hook = null; }
        }

        @Override
        public List<TransferBatch.Record> snapshot(Tablet tablet) {
            if (!tablet.equals(tracked)) throw new IllegalStateException("tracking must precede snapshot");
            List<TransferBatch.Record> records = new ArrayList<>();
            data.forEach((key, value) -> {
                if (tablet.range().contains(KeyHash.of(key))) records.add(new TransferBatch.Record(key, value));
            });
            return records;
        }

        @Override public Optional<Bytes> read(Tablet tablet, Bytes key) { return Optional.ofNullable(data.get(key)); }

        @Override public List<Bytes> keys(Tablet tablet) {
            if (!frozen(tablet)) throw new IllegalStateException("manifest needs freeze");
            return data.keySet().stream().filter(key -> tablet.range().contains(KeyHash.of(key))).toList();
        }

        @Override
        public Optional<PullReply> pull(MigrationJob session, OwnershipEpoch epoch, long id, Bytes key) {
            if (!available || dropPull) return Optional.empty();
            pulled.add(key);
            return Optional.of(transform.apply(PullReply.create(session, epoch, id, key, Optional.ofNullable(data.get(key)))));
        }

        @Override public boolean cleanup(Tablet tablet) {
            cleanupCalls++;
            if (cleanupFailures > 0) {
                cleanupFailures--;
                if (partialCleanup) data.keySet().stream().filter(key -> tablet.range().contains(KeyHash.of(key)))
                        .findFirst().ifPresent(data::remove);
                return false;
            }
            data.keySet().removeIf(key -> tablet.range().contains(KeyHash.of(key)));
            return true;
        }
    }

    public static final class Link implements PreCopyProtocol.Destination {
        public final PreCopyReceiver receiver = new PreCopyReceiver();
        public boolean holdSnapshotAck;
        public boolean holdFinalAck;
        public boolean holdLastFinalAck;
        public boolean corruptDelta;
        public TransferBatch lastSnapshot;
        public DeltaBatch lastDelta;

        @Override public boolean prepare(MigrationJob session, Tablet tablet) { return receiver.prepare(session, tablet); }
        @Override public void receive(MigrationJob session, OwnershipEpoch epoch, TransferBatch batch) {
            lastSnapshot = batch;
            receiver.receive(session, epoch, batch);
        }
        @Override public Optional<Acknowledgement> acknowledgement(MigrationJob session, long id) {
            return holdSnapshotAck ? Optional.empty() : receiver.acknowledgement(session, id);
        }
        @Override public boolean complete(MigrationJob session) { return receiver.complete(session); }
        @Override public void discard(MigrationJob session) { receiver.discard(session); }
        @Override public void receiveDelta(MigrationJob session, OwnershipEpoch epoch, DeltaBatch batch) {
            lastDelta = batch;
            receiver.receiveDelta(session, epoch, corruptDelta
                    ? new DeltaBatch(batch.batchId(), batch.recordCount(), batch.serializedBytes(), batch.checksum() ^ 1,
                            batch.last(), batch.finalRound(), batch.records()) : batch);
        }
        @Override public Optional<DeltaBatch.Acknowledgement> deltaAcknowledgement(MigrationJob session, long id) {
            Optional<DeltaBatch.Acknowledgement> ack = receiver.deltaAcknowledgement(session, id);
            boolean hold = holdFinalAck || (holdLastFinalAck && lastDelta != null && lastDelta.last());
            return hold && ack.filter(DeltaBatch.Acknowledgement::finalRound).isPresent() ? Optional.empty() : ack;
        }
        @Override public boolean caughtUp(MigrationJob session, long seq) { return receiver.caughtUp(session, seq); }
    }
}
