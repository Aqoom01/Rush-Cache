package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.Redirect;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.MigrationPublication;
import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.migration.strategy.CopyDrivers;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.postcopy.PostCopyProtocol;
import io.openkache.core.migration.postcopy.PostCopyState;
import io.openkache.core.migration.postcopy.PullReply;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.migration.precopy.PreCopyReceiver;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** 다중 노드·공유 routing CAS·Tablet별 freeze/추적/저장소를 가진 실제 protocol 실행 fixture. */
final class MultiMigrationFixture {
    static final class Node {
        final Map<TabletId, Supplier<Map<Bytes, Bytes>>> tablets = new HashMap<>();
        final Map<TabletId, Tablet> frozen = new HashMap<>();
        final Map<TabletId, Consumer<Bytes>> hooks = new HashMap<>();
    }

    final AtomicReference<RoutingTable> routing;
    final Map<NodeId, Node> nodes = new HashMap<>();
    final List<Change> changes;
    final List<Handle> handles = new ArrayList<>();
    final TabletId guard;
    final Map<Bytes, Bytes> guardData = new LinkedHashMap<>();
    int commits;

    MultiMigrationFixture(int jobs, int keys, long seed, String endpoints, String variant, int batchBytes, long threshold) {
        this(plan(jobs, endpoints), java.util.Collections.nCopies(jobs, keys), seed, variant, batchBytes, threshold);
    }

    private static List<Change> plan(int jobs, String endpoints) {
        List<Change> result = new ArrayList<>();
        for (int i = 0; i < jobs; i++) result.add(new Change(new TabletId(i),
                new NodeId(endpoints.equals("shared-source") ? 1 : i + 1),
                new NodeId(endpoints.equals("shared-destination") ? jobs + 1 : jobs + i + 1)));
        return result;
    }

    MultiMigrationFixture(List<Change> plan, List<Integer> keyCounts, long seed, String variant, int batchBytes, long threshold) {
        int jobs = plan.size();
        if (jobs == 0 || keyCounts.size() != jobs || keyCounts.stream().anyMatch(n -> n < 1)) throw new IllegalArgumentException("invalid tablet sizes");
        List<TabletRange> ranges = TabletRange.uniform(jobs + 1);
        List<Tablet> tablets = new ArrayList<>();
        List<Change> moves = new ArrayList<>();
        for (int i = 0; i < jobs; i++) {
            Change move = plan.get(i);
            if (move.unit().value() != i) throw new IllegalArgumentException("tablet IDs must match plan order");
            NodeId source = move.from(), destination = move.to();
            moves.add(move);
            nodes.computeIfAbsent(source, ignored -> new Node());
            nodes.computeIfAbsent(destination, ignored -> new Node());
            tablets.add(new Tablet(move.unit(), ranges.get(i), source, OwnershipEpoch.ZERO));
        }
        guard = new TabletId(jobs);
        // 이동 source와 같은 노드의 비이동 Tablet을 cleanup이 건드리지 않는지도 검증한다.
        nodes.get(plan.getFirst().from()).tablets.put(guard, () -> guardData);
        tablets.add(new Tablet(guard, ranges.get(jobs), plan.getFirst().from(), OwnershipEpoch.ZERO));
        routing = new AtomicReference<>(RoutingTable.of(RoutingVersion.ZERO, tablets, jobs + 1));
        changes = List.copyOf(moves);
        for (Change move : changes) handles.add(new Handle(move, variant, batchBytes, threshold));
        int remaining = keyCounts.stream().mapToInt(Integer::intValue).sum() + 8;
        for (long i = 0; remaining > 0; i++) {
            Bytes key = MigrationFixture.key(seed, i);
            int unit = Math.toIntExact(routing.get().lookup(KeyHash.of(key)).id().value());
            if (unit == jobs) {
                if (guardData.size() < 8) { guardData.put(key, MigrationFixture.value(-1)); remaining--; }
            } else {
                Handle handle = handles.get(unit);
                if (handle.sourceData.size() < keyCounts.get(unit)) {
                    handle.sourceData.put(key, MigrationFixture.value(-1)); remaining--;
                }
            }
        }
        for (Handle h : handles) {
            h.keys = List.copyOf(h.sourceData.keySet());
            h.oracle.putAll(h.sourceData);
        }
    }

    Handle handle(Change move) {
        Handle h = handles.get(Math.toIntExact(move.unit().value()));
        if (!h.change.equals(move)) throw new IllegalArgumentException("changed input plan");
        return h;
    }

    final class Handle implements MigrationScheduler.Driver {
        final Change change;
        final String variant;
        final int batchBytes;
        final long threshold;
        final Map<Bytes, Bytes> sourceData = new LinkedHashMap<>();
        final Map<Bytes, Bytes> destinationData = new LinkedHashMap<>();
        final Map<Bytes, Bytes> oracle = new LinkedHashMap<>();
        final Source source = new Source();
        final PreCopyReceiver receiver = new PreCopyReceiver();
        final SnapshotReceiver snapshotReceiver = new SnapshotReceiver();
        List<Bytes> keys;
        StopAndCopyProtocol stop;
        PreCopyProtocol pre;
        PostCopyProtocol post;
        RoutingTable expected;
        MigrationJob session;
        long startTick = -1, commitTick = -1;
        long writes, accepted, writeBusy, reads, readBusy, blockedTicks;
        int cleanupFailures, cleanupCalls;
        boolean partialCleanup;
        final PreCopyRoundTrace dirtyTrace = new PreCopyRoundTrace();

        Handle(Change change, String variant, int batchBytes, long threshold) {
            this.change = change; this.variant = variant; this.batchBytes = batchBytes; this.threshold = threshold;
            nodes.get(change.from()).tablets.put(change.unit(), () -> sourceData);
        }

        CopyDrivers.Session selectedDriver;

        @Override public void begin() {
            expected = routing.get();
            session = MigrationJob.pending(change, variant);
            selectedDriver = CopyDrivers.create(variant, change, new CopyDrivers.Binding() {
                @Override public RoutingTable current() { return routing.get(); }
                @Override public boolean compareAndSet(RoutingTable before, RoutingTable after) { return routing.compareAndSet(before, after); }
                @Override public PreCopyProtocol.Source source() { return source; }
                @Override public StopAndCopyProtocol.Destination stopDestination() { return snapshotReceiver; }
                @Override public PreCopyProtocol.Destination preDestination() { return receiver; }
                @Override public PostCopyProtocol.Source postSource() { return source; }
                @Override public void installed(Change move, String copy, RoutingTable before, RoutingTable after) {
                    if (post == null) {
                        destinationData.putAll(stop == null ? receiver.data() : snapshotReceiver.data);
                        nodes.get(change.to()).tablets.put(change.unit(), () -> destinationData);
                    } else nodes.get(change.to()).tablets.put(change.unit(), post::data);
                    commits++;
                }
            }, new CopyDrivers.Settings(batchBytes, threshold, 10));
            selectedDriver.begin();
            stop = selectedDriver.stopProtocol(); pre = selectedDriver.preProtocol(); post = selectedDriver.postProtocol();
        }

        boolean ready() { return stop != null ? stop.readyToCommit() : pre != null ? pre.readyToCommit() : post != null && post.readyToCommit(); }
        boolean sourceWritable() {
            return session == null || (stop != null ? stop.acceptsWrite() : pre != null ? pre.acceptsWrite() : post.acceptsWrite());
        }

        @Override public void step(long budget) {
            selectedDriver.step(budget);
        }

        @Override public long sentBytes() { return stop != null ? stop.job().progress().sentBytes()
                : pre != null ? pre.job().progress().sentBytes() : post != null ? post.sentBytes() : 0; }
        @Override public boolean done() { return state().equals("DONE"); }
        @Override public boolean failed() { return state().equals("ABORTED") || state().equals("FAILED"); }
        @Override public String state() { return stop != null ? stop.state().name() : pre != null ? pre.state().name()
                : post != null ? post.state().name() : "PENDING"; }
        @Override public void stop() {
            if (stop != null) stop.abort();
            else if (pre != null) pre.abort();
            else if (post != null) post.fail();
        }
        int rounds() { return pre == null ? 0 : pre.rounds(); }
        void recordDirty(long tick) { if (pre != null) dirtyTrace.capture(pre.roundEvents(), tick); }
        String reason() { return pre == null ? "" : pre.stopReason().map(Enum::name).orElse(""); }
        Map<Bytes, Bytes> finalData() { return post == null ? destinationData : post.data(); }

        Result request(NodeId target, Command command) {
            RoutingTable table = routing.get();
            Tablet tablet = table.get(change.unit());
            if (!target.equals(tablet.owner())) return Result.redirect(ResultStatus.MOVED,
                    new Redirect(tablet.owner()), table.version(), tablet.epoch());
            boolean destination = tablet.owner().equals(change.to());
            if (destination && post != null) return post.execute(command);
            Map<Bytes, Bytes> data = destination ? destinationData : sourceData;
            boolean writable = destination || sourceWritable();
            if (!(command instanceof Command.Get) && !writable) return result(ResultStatus.BUSY);
            return switch (command) {
                case Command.Get get -> data.containsKey(get.key()) ? Result.ok(data.get(get.key()), 0, table.version(), tablet.epoch()) : result(ResultStatus.MISS);
                case Command.Set set -> {
                    data.put(set.key(), set.value()); if (!destination) notifyWrite(set.key()); yield result(ResultStatus.OK);
                }
                case Command.Delete delete -> {
                    boolean existed = data.remove(delete.key()) != null;
                    if (existed && !destination) notifyWrite(delete.key());
                    yield result(existed ? ResultStatus.OK : ResultStatus.MISS);
                }
                default -> throw new IllegalArgumentException("CRUD only");
            };
        }

        Result request(Command command) { return request(routing.get().get(change.unit()).owner(), command); }
        Result result(ResultStatus status) { return Result.of(status, routing.get().version(), routing.get().get(change.unit()).epoch()); }
        void notifyWrite(Bytes key) {
            Consumer<Bytes> hook = nodes.get(change.from()).hooks.get(change.unit());
            if (hook != null) hook.accept(key);
        }

        final class SnapshotReceiver implements StopAndCopyProtocol.Destination {
            final Map<Bytes, Bytes> data = new LinkedHashMap<>();
            final Map<Long, StopAndCopyProtocol.Acknowledgement> acks = new HashMap<>();
            MigrationJob identity;
            Tablet moving;
            boolean complete;
            @Override public boolean prepare(MigrationJob job, Tablet tablet) {
                if (identity != null || !job.variant().equals("stop-and-copy")
                        || !job.change().unit().equals(tablet.id()) || !job.change().from().equals(tablet.owner())) return false;
                identity = job; moving = tablet; return true;
            }
            @Override public void receive(MigrationJob job, OwnershipEpoch epoch, TransferBatch batch) {
                if (job != identity || !moving.epoch().equals(epoch) || !batch.valid()
                        || complete || batch.batchId() != acks.size()) return;
                var seen = new java.util.HashSet<Bytes>();
                for (var record : batch.records()) if (!moving.range().contains(KeyHash.of(record.key()))
                        || data.containsKey(record.key()) || !seen.add(record.key())) return;
                batch.records().forEach(r -> data.put(r.key(), r.value()));
                acks.put(batch.batchId(), StopAndCopyProtocol.Acknowledgement.of(batch));
                complete = batch.last();
            }
            @Override public Optional<StopAndCopyProtocol.Acknowledgement> acknowledgement(MigrationJob job, long id) {
                return job == identity ? Optional.ofNullable(acks.get(id)) : Optional.empty();
            }
            @Override public boolean complete(MigrationJob job) { return job == identity && complete; }
            @Override public void discard(MigrationJob job) {
                if (job == identity) { identity = null; moving = null; data.clear(); acks.clear(); complete = false; }
            }
        }

        final class Source implements PreCopyProtocol.Source, PostCopyProtocol.Source {
            Node node() { return nodes.get(change.from()); }
            @Override public boolean freeze(Tablet tablet) { return node().frozen.putIfAbsent(tablet.id(), tablet) == null; }
            @Override public boolean frozen(Tablet tablet) { return tablet.equals(node().frozen.get(tablet.id())); }
            @Override public void unfreeze(Tablet tablet) { node().frozen.remove(tablet.id(), tablet); }
            @Override public boolean startTracking(Tablet tablet, Consumer<Bytes> hook) {
                return !frozen(tablet) && node().hooks.putIfAbsent(tablet.id(), hook) == null;
            }
            @Override public void stopTracking(Tablet tablet) { node().hooks.remove(tablet.id()); }
            @Override public List<TransferBatch.Record> snapshot(Tablet tablet) {
                if (!frozen(tablet) && !node().hooks.containsKey(tablet.id())) throw new IllegalStateException("snapshot without barrier");
                List<TransferBatch.Record> records = new ArrayList<>();
                sourceData.forEach((k, v) -> {
                    if (!tablet.range().contains(KeyHash.of(k))) throw new IllegalStateException("key outside tablet");
                    records.add(new TransferBatch.Record(k, v));
                });
                return records;
            }
            @Override public Optional<Bytes> read(Tablet tablet, Bytes key) { return Optional.ofNullable(sourceData.get(key)); }
            @Override public List<Bytes> keys(Tablet tablet) {
                if (!frozen(tablet)) throw new IllegalStateException("manifest without freeze");
                return List.copyOf(sourceData.keySet());
            }
            @Override public boolean available(Tablet tablet) { return true; }
            @Override public Optional<PullReply> pull(MigrationJob job, OwnershipEpoch epoch, long id, Bytes key) {
                return Optional.of(PullReply.create(job, epoch, id, key, Optional.ofNullable(sourceData.get(key))));
            }
            @Override public boolean cleanup(Tablet tablet) {
                cleanupCalls++;
                if (cleanupFailures-- > 0) {
                    if (partialCleanup && !sourceData.isEmpty()) sourceData.remove(sourceData.keySet().iterator().next());
                    return false;
                }
                sourceData.clear(); node().tablets.remove(tablet.id()); return true;
            }
        }
    }
}
