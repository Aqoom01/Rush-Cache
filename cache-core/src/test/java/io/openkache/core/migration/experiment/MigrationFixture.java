package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.Redirect;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationCommitter;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 최소 저장소·전송·coordinator를 제공하는 1단계 실험 adapter. 실제 KvEngine/서버를 사용하지 않는다.
 * 모든 요청·tick·commit을 하나의 thread에서 직렬 호출한다.
 */
public final class MigrationFixture {
    public static final NodeId SOURCE = new NodeId(1);
    public static final NodeId DESTINATION = new NodeId(2);
    public static final TabletId MOVING = new TabletId(0);
    public static final TabletId OTHER = new TabletId(1);

    public enum Fault { NONE, CHECKSUM, COUNT, BYTES, DUPLICATE, DROP, ACK_DELAY, BAD_ACK, LOST_ACK }

    public final Store source = new Store();
    public final Map<Bytes, Bytes> destination = new LinkedHashMap<>();
    public final Coordinator coordinator;
    public final MigrationJob initialJob;
    public final Link link;
    public final StopAndCopyProtocol protocol;

    public MigrationFixture(boolean twoTablets, Fault fault, int batchBytes) {
        List<TabletRange> ranges = TabletRange.uniform(twoTablets ? 2 : 1);
        List<Tablet> tablets = new ArrayList<>();
        for (int i = 0; i < ranges.size(); i++) {
            tablets.add(new Tablet(new TabletId(i), ranges.get(i), SOURCE, OwnershipEpoch.ZERO));
        }
        coordinator = new Coordinator(RoutingTable.of(RoutingVersion.ZERO, tablets, tablets.size()));
        initialJob = MigrationJob.pending(new Change(MOVING, SOURCE, DESTINATION), "stop-and-copy");
        link = new Link(fault, destination);
        protocol = new StopAndCopyProtocol(source, link, coordinator.current(), batchBytes);
    }

    public MigrationFixture(boolean twoTablets, Fault fault) {
        this(twoTablets, fault, StopAndCopyProtocol.DEFAULT_BATCH_BYTES);
    }

    /** key는 seed/index 각 8 bytes. 같은 seed에 내용과 순서가 고정된다. */
    public static Bytes key(long seed, long index) {
        return Bytes.copyOf(ByteBuffer.allocate(16).putLong(seed).putLong(index).array());
    }

    /** value는 128 bytes. */
    public static Bytes value(long version) {
        byte[] bytes = new byte[128];
        ByteBuffer.wrap(bytes).putLong(version);
        return Bytes.copyOf(bytes);
    }

    /** owner 판정 → freeze 확인 → GET/SET/DELETE. 다른 연산은 거부하며 staging은 commit 전 요청으로 노출하지 않는다. */
    public Result request(NodeId node, Command command) {
        Bytes key = switch (command) {
            case Command.Get get -> get.key();
            case Command.Set set -> set.key();
            case Command.Delete delete -> delete.key();
            default -> throw new IllegalArgumentException("migration baseline supports only GET/SET/DELETE");
        };
        RoutingTable table = coordinator.current();
        Tablet tablet = table.lookup(KeyHash.of(key));
        if (!tablet.owner().equals(node)) {
            return Result.redirect(ResultStatus.MOVED, new Redirect(tablet.owner()), table.version(), tablet.epoch());
        }
        if (node.equals(SOURCE) && !(command instanceof Command.Get) && source.frozen(tablet)) {
            return Result.of(ResultStatus.BUSY, table.version(), tablet.epoch());
        }
        Map<Bytes, Bytes> data = node.equals(SOURCE) ? source.data : destination;
        return switch (command) {
            case Command.Get get -> data.containsKey(get.key())
                    ? Result.ok(data.get(get.key()), 0, table.version(), tablet.epoch())
                    : Result.of(ResultStatus.MISS, table.version(), tablet.epoch());
            case Command.Set set -> {
                data.put(set.key(), set.value());
                yield Result.of(ResultStatus.OK, table.version(), tablet.epoch());
            }
            case Command.Delete delete -> Result.of(data.remove(delete.key()) == null
                    ? ResultStatus.MISS : ResultStatus.OK, table.version(), tablet.epoch());
            default -> throw new IllegalArgumentException("migration baseline supports only GET/SET/DELETE");
        };
    }

    /** 원본의 이동 Tablet 범위 전체를 복사해 exact compare에 쓴다. */
    public Map<Bytes, Bytes> movingData() {
        Tablet tablet = protocol.expectedTable().get(MOVING);
        Map<Bytes, Bytes> data = new LinkedHashMap<>();
        source.data.forEach((key, value) -> {
            if (tablet.range().contains(KeyHash.of(key))) data.put(key, value);
        });
        return data;
    }

    /** 예상 table 객체를 CAS로 교체한다. 상태·ACK·freeze 조건에 맞지 않는 job은 거부한다. */
    public final class Coordinator implements MigrationCommitter {
        private final AtomicReference<RoutingTable> table;
        public int successfulCommits;

        private Coordinator(RoutingTable initial) { table = new AtomicReference<>(initial); }
        public RoutingTable current() { return table.get(); }

        @Override
        public boolean commit(MigrationJob job, RoutingTable expectedTable) {
            if (job.state() != JobState.SWITCHING || !job.progress().snapshotComplete()
                    || !source.frozen(expectedTable.get(job.change().unit())) || table.get() != expectedTable) return false;
            if (!table.compareAndSet(expectedTable, expectedTable.apply(job.change()))) return false;
            successfulCommits++;
            return true;
        }
    }

    /** freeze가 Tablet별이며 삭제 실패 때 부분 정리도 모사한다. */
    public static final class Store implements StopAndCopyProtocol.Source {
        public final Map<Bytes, Bytes> data = new LinkedHashMap<>();
        private final Map<TabletId, Tablet> frozen = new HashMap<>();
        public int cleanupFailures;
        public boolean partialCleanup;
        public int cleanupCalls;

        @Override public boolean freeze(Tablet tablet) { return frozen.putIfAbsent(tablet.id(), tablet) == null; }
        @Override public boolean frozen(Tablet tablet) { return tablet.equals(frozen.get(tablet.id())); }
        @Override public void unfreeze(Tablet tablet) { frozen.remove(tablet.id(), tablet); }

        @Override
        public List<TransferBatch.Record> snapshot(Tablet tablet) {
            if (!frozen(tablet)) throw new IllegalStateException("snapshot requires freeze");
            List<TransferBatch.Record> records = new ArrayList<>();
            data.forEach((key, value) -> {
                if (tablet.range().contains(KeyHash.of(key))) records.add(new TransferBatch.Record(key, value));
            });
            return records;
        }

        @Override
        public boolean cleanup(Tablet tablet) {
            cleanupCalls++;
            if (cleanupFailures > 0) {
                cleanupFailures--;
                if (partialCleanup) {
                    var iterator = data.keySet().iterator();
                    while (iterator.hasNext()) {
                        if (tablet.range().contains(KeyHash.of(iterator.next()))) { iterator.remove(); break; }
                    }
                }
                return false;
            }
            data.keySet().removeIf(key -> tablet.range().contains(KeyHash.of(key)));
            return true;
        }
    }

    /** session·epoch fencing, batch 검증·중복 ACK와 전송 이상 주입을 담당한다. */
    public final class Link implements StopAndCopyProtocol.Destination {
        public final Fault fault;
        private final Map<Bytes, Bytes> data;
        private final Map<Long, Acknowledgement> acks = new HashMap<>();
        private MigrationJob session;
        private Tablet tablet;
        private boolean complete;
        public TransferBatch lastBatch;
        public int validationFailures;
        public int duplicateDeliveries;
        public boolean holdFinalAck;
        private int finalAckPolls;
        private boolean badAckDetected;

        public Link(Fault fault, Map<Bytes, Bytes> data) { this.fault = fault; this.data = data; }

        @Override
        public boolean prepare(MigrationJob candidate, Tablet expected) {
            if (session != null || !data.isEmpty() || !coordinator.current().get(expected.id()).equals(expected)
                    || !candidate.change().from().equals(expected.owner()) || !candidate.change().unit().equals(expected.id())) return false;
            session = candidate;
            tablet = expected;
            return true;
        }

        @Override
        public void receive(MigrationJob candidate, OwnershipEpoch epoch, TransferBatch batch) {
            lastBatch = batch;
            if (fault == Fault.DROP) return;
            TransferBatch delivered = switch (fault) {
                case CHECKSUM -> new TransferBatch(batch.batchId(), batch.recordCount(), batch.serializedBytes(), batch.checksum() ^ 1, batch.last(), batch.records());
                case COUNT -> new TransferBatch(batch.batchId(), batch.recordCount() + 1, batch.serializedBytes(), batch.checksum(), batch.last(), batch.records());
                case BYTES -> new TransferBatch(batch.batchId(), batch.recordCount(), batch.serializedBytes() + 1, batch.checksum(), batch.last(), batch.records());
                default -> batch;
            };
            deliver(candidate, epoch, delivered);
            if (fault == Fault.DUPLICATE) {
                deliver(candidate, epoch, delivered);
                duplicateDeliveries++;
            }
        }

        /** 손상·순서 오류 batch는 일부 record도 적용하지 않는다. */
        public boolean deliver(MigrationJob candidate, OwnershipEpoch epoch, TransferBatch batch) {
            if (session != candidate || !epoch.equals(tablet.epoch())
                    || !epoch.equals(coordinator.current().get(tablet.id()).epoch()) || !batch.valid()) return reject();
            Acknowledgement previous = acks.get(batch.batchId());
            if (previous != null) return previous.matches(batch) || reject();
            if (complete || batch.batchId() != acks.size()) return reject();
            Map<Bytes, Bytes> incoming = new LinkedHashMap<>();
            for (TransferBatch.Record record : batch.records()) {
                if (!tablet.range().contains(KeyHash.of(record.key())) || data.containsKey(record.key())
                        || incoming.put(record.key(), record.value()) != null) return reject();
            }
            data.putAll(incoming);
            acks.put(batch.batchId(), Acknowledgement.of(batch));
            complete = batch.last();
            return true;
        }

        private boolean reject() { validationFailures++; return false; }

        @Override
        public Optional<Acknowledgement> acknowledgement(MigrationJob candidate, long batchId) {
            if (candidate != session || fault == Fault.LOST_ACK) return Optional.empty();
            Acknowledgement ack = acks.get(batchId);
            if (ack == null) return Optional.empty();
            if (holdFinalAck && complete) return Optional.empty();
            if (fault == Fault.ACK_DELAY && complete && finalAckPolls++ < 3) return Optional.empty();
            if (fault == Fault.BAD_ACK) {
                if (!badAckDetected) { validationFailures++; badAckDetected = true; }
                return Optional.of(new Acknowledgement(ack.batchId() + 1, ack.recordCount(), ack.serializedBytes(), ack.checksum()));
            }
            return Optional.of(ack);
        }

        @Override public boolean complete(MigrationJob candidate) { return candidate == session && complete; }

        @Override
        public void discard(MigrationJob candidate) {
            if (candidate == session && coordinator.current().get(tablet.id()).equals(tablet)) {
                data.clear();
                acks.clear();
                session = null;
                complete = false;
                finalAckPolls = 0;
                badAckDetected = false;
            }
        }
    }
}
