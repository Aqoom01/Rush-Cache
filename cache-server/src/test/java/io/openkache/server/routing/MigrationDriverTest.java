package io.openkache.server.routing;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.TabletId;
import io.openkache.core.model.WorkerId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.routing.LocalRouting;
import io.openkache.core.tablet.routing.TableMessage;
import io.openkache.server.netty.Protocol;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class MigrationDriverTest {
    private static final NodeId CONTROLLER = new NodeId(0);
    private static final NodeId SOURCE = new NodeId(1);
    private static final TabletId FIRST = new TabletId(2);
    private static final TabletId SECOND = new TabletId(3);
    private static final RoutingTable INITIAL = new HashRangeScheme(2).initial(List.of(CONTROLLER, SOURCE));

    @Test
    void retriesOnlyStaleCommitAndKeepsSourceFencedUntilNewTable() throws Exception {
        try (Fixture f = new Fixture()) {
            long firstJob = f.start(FIRST);
            long secondJob = f.start(SECOND);
            Commit a = f.nextCommit();
            Commit b = f.nextCommit();
            assertEquals(INITIAL.version().value(), a.version());
            assertEquals(a.version(), b.version());
            long rejectedJob = b.tablet().equals(FIRST) ? firstJob : secondJob;

            RoutingTable firstPublished = INITIAL.apply(new Change(a.tablet(), SOURCE, CONTROLLER));
            a.reply().complete(ok(firstPublished));
            b.reply().complete(stale(firstPublished));

            f.nextTableRequest();
            f.deliver(INITIAL);
            f.nextTableRequest();
            assertTrue(f.commits.isEmpty());
            assertEquals(MigrationDriver.Phase.COMMITTING, f.phase(rejectedJob));
            assertTrue(f.onWorker(() -> f.store.writeBlocked(b.tablet())));
            assertFalse(f.routing.check(INITIAL.get(b.tablet()).range().start(), INITIAL.version(),
                    OwnershipEpoch.ZERO).allowed());

            f.deliver(firstPublished);
            Commit retry = f.nextCommit();
            assertEquals(firstPublished.version().value(), retry.version());
            assertEquals(b.tablet(), retry.tablet());
            assertTrue(f.onWorker(() -> f.store.writeBlocked(b.tablet())));
            assertTrue(f.routing.isRefusing(b.tablet()));
            RoutingTable finalTable = firstPublished.apply(new Change(b.tablet(), SOURCE, CONTROLLER));
            retry.reply().complete(ok(finalTable));
            f.deliver(finalTable);
            f.awaitPhase(firstJob, MigrationDriver.Phase.DONE);
            f.awaitPhase(secondJob, MigrationDriver.Phase.DONE);
            assertEquals(1, f.retries(rejectedJob));
        }
    }

    @Test
    void splitBeforeRetryReleasesFenceOnBothPiecesAndAborts() throws Exception {
        try (Fixture f = new Fixture()) {
            long job = f.start(FIRST);
            Commit attempt = f.nextCommit();
            long start = INITIAL.get(FIRST).range().start();
            long mid = start + (1L << 61);
            RoutingTable split = INITIAL.withSplit(FIRST, mid);
            TabletId right = split.lookup(mid).id();
            assertNotEquals(FIRST, right);

            // A push may arrive before the stale CONTROL reply.
            f.deliver(split);
            assertTrue(f.routing.isRefusing(FIRST));
            assertTrue(f.routing.isRefusing(right));
            attempt.reply().complete(stale(split));

            f.awaitPhase(job, MigrationDriver.Phase.FAILED);
            assertTrue(f.failure(job).contains("moving tablet changed"));
            assertEquals(0, f.retries(job));
            assertFalse(f.routing.isRefusing(FIRST));
            assertFalse(f.routing.isRefusing(right));
            assertFalse(f.onWorker(() -> f.store.writeBlocked(FIRST)));
            assertTrue(f.commits.isEmpty());
        }
    }

    @Test
    void lostCommitReplyRetainsFenceAndDoesNotRetry() throws Exception {
        try (Fixture f = new Fixture()) {
            long job = f.start(FIRST);
            f.nextCommit().reply().completeExceptionally(new TimeoutException("reply lost"));
            f.awaitPhase(job, MigrationDriver.Phase.FAILED);
            assertTrue(f.failure(job).startsWith("commit outcome unknown"));
            assertTrue(f.routing.isRefusing(FIRST));
            assertTrue(f.onWorker(() -> f.store.writeBlocked(FIRST)));
            assertEquals(0, f.retries(job));
            assertTrue(f.commits.isEmpty());
            assertTrue(f.tableRequests.isEmpty());
        }
    }

    @Test
    void otherUnavailableReplyFailsWithoutRetry() throws Exception {
        try (Fixture f = new Fixture()) {
            long job = f.start(FIRST);
            f.nextCommit().reply().complete(new PeerCaller.Reply(Protocol.statusCode(ResultStatus.UNAVAILABLE),
                    INITIAL.version().value(), "different rejection", new byte[0]));
            f.awaitPhase(job, MigrationDriver.Phase.FAILED);
            assertTrue(f.failure(job).contains("different rejection"));
            assertEquals(0, f.retries(job));
            assertFalse(f.routing.isRefusing(FIRST));
            assertFalse(f.onWorker(() -> f.store.writeBlocked(FIRST)));
            assertTrue(f.commits.isEmpty());
            assertTrue(f.tableRequests.isEmpty());
        }
    }

    @Test
    void repeatedVersionRejectionsStopAtRetryLimit() throws Exception {
        try (Fixture f = new Fixture()) {
            long job = f.start(FIRST);
            RoutingTable latest = INITIAL;
            for (int i = 0; i <= 16; i++) {
                Commit commit = f.nextCommit();
                assertEquals(latest.version().value(), commit.version());
                NodeId from = latest.get(SECOND).owner();
                latest = latest.apply(new Change(SECOND, from, from.equals(SOURCE) ? CONTROLLER : SOURCE));
                f.deliver(latest);
                commit.reply().complete(stale(latest));
            }
            f.awaitPhase(job, MigrationDriver.Phase.FAILED);
            assertEquals(16, f.retries(job));
            assertTrue(f.failure(job).contains("after 16 retries"));
            assertFalse(f.routing.isRefusing(FIRST));
            assertFalse(f.onWorker(() -> f.store.writeBlocked(FIRST)));
            assertTrue(f.commits.isEmpty());
        }
    }

    private static PeerCaller.Reply ok(RoutingTable table) {
        return new PeerCaller.Reply(0, table.version().value(), "", new byte[0]);
    }

    private static PeerCaller.Reply stale(RoutingTable table) {
        return new PeerCaller.Reply(Protocol.statusCode(ResultStatus.UNAVAILABLE), table.version().value(),
                B1Wire.STALE_EXPECTED_VERSION, new byte[0]);
    }

    private record Commit(long version, TabletId tablet, CompletableFuture<PeerCaller.Reply> reply) {}

    private static final class Fixture implements AutoCloseable {
        final OwnerWorker worker = new OwnerWorker(new WorkerId(0), 128);
        final NodeStore store = new NodeStore();
        final LinkedBlockingQueue<Commit> commits = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<PeerEnvelope> tableRequests = new LinkedBlockingQueue<>();
        final LocalRouting routing = new LocalRouting(SOURCE, INITIAL, envelope -> {
            tableRequests.add(envelope);
            return CompletableFuture.completedFuture(null);
        });
        final MigrationDriver driver = new MigrationDriver(SOURCE, CONTROLLER, store, routing, null,
                new PeerRequests() {
                    @Override public boolean knows(NodeId node) { return node.equals(CONTROLLER); }

                    @Override
                    public CompletableFuture<PeerCaller.Reply> call(NodeId target, int opcode, byte[] body) {
                        if (opcode == MigrationWire.TRANSFER) return CompletableFuture.completedFuture(ok(INITIAL));
                        ByteBuffer input = ByteBuffer.wrap(body);
                        input.get();
                        var reply = new CompletableFuture<PeerCaller.Reply>();
                        commits.add(new Commit(input.getLong(), new TabletId(input.getLong()), reply));
                        return reply;
                    }
                }, worker);

        long start(TabletId id) throws Exception {
            MigrationDriver.Result result = onWorker(() -> driver.start(new MigrationWire.Migrate(
                    MigrationWire.Method.STOP_AND_COPY, id.value(), CONTROLLER.value(), 0, 16384, 0, 10)));
            assertTrue(result.started(), result.reason());
            return result.jobId();
        }

        Commit nextCommit() throws InterruptedException {
            Commit commit = commits.poll(3, TimeUnit.SECONDS);
            assertNotNull(commit, "missing CONTROL commit");
            return commit;
        }

        void nextTableRequest() throws InterruptedException {
            PeerEnvelope envelope = tableRequests.poll(3, TimeUnit.SECONDS);
            assertNotNull(envelope, "missing GetTable");
            assertInstanceOf(TableMessage.GetTable.class, envelope.message());
        }

        void deliver(RoutingTable table) {
            routing.onPeerMessage(CONTROLLER, new TableMessage.TableReply(Optional.of(table), table.version()));
        }

        <T> T onWorker(Callable<T> action) throws Exception {
            var result = new CompletableFuture<T>();
            assertTrue(worker.offer(() -> {
                try {
                    result.complete(action.call());
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            }));
            return result.get(3, TimeUnit.SECONDS);
        }

        MigrationDriver.Phase phase(long job) throws Exception {
            return onWorker(() -> driver.job(job).orElseThrow().phase());
        }

        int retries(long job) throws Exception {
            return onWorker(() -> driver.job(job).orElseThrow().commitRetries);
        }

        String failure(long job) throws Exception {
            return onWorker(() -> driver.job(job).orElseThrow().failure);
        }

        void awaitPhase(long job, MigrationDriver.Phase expected) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                var actual = phase(job);
                if (actual == expected) return;
                if (actual == MigrationDriver.Phase.DONE || actual == MigrationDriver.Phase.FAILED) break;
                Thread.sleep(5);
            }
            assertEquals(expected, phase(job), failure(job));
        }

        @Override public void close() {
            driver.close();
            worker.close();
            store.close();
        }
    }
}
