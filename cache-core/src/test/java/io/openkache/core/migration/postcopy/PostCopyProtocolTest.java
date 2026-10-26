package io.openkache.core.migration.postcopy;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Command;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.experiment.LiveMigrationFixture;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.KeyHash;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class PostCopyProtocolTest {
    private static final Bytes KEY = Bytes.utf8("key");
    private static final Bytes OLD = Bytes.utf8("old");
    private static final Bytes NEW = Bytes.utf8("new");

    private static PostCopyProtocol started(LiveMigrationFixture f) {
        var p = new PostCopyProtocol(f.source, f.current);
        var job = f.job(p.name());
        assertEquals(PostCopyState.SWITCHING, p.begin(job));
        assertEquals(0, p.sentBytes());
        assertTrue(f.commit(p, job));
        return p;
    }

    private static void finish(PostCopyProtocol p) {
        for (int i = 0; i < 10_000 && p.state() != PostCopyState.DONE; i++) {
            long before = p.sentBytes();
            p.step(19);
            assertTrue(p.sentBytes() - before <= 19);
        }
        assertEquals(PostCopyState.DONE, p.state());
    }

    @Test
    void ownershipChangesBeforeCopyAndUnarrivedReadsGetPriorityInsteadOfFalseMiss() {
        var f = new LiveMigrationFixture(false);
        Bytes hot = Bytes.utf8("hot");
        f.source.put(KEY, OLD);
        f.source.put(hot, NEW);
        var expected = Map.copyOf(f.source.data);
        var p = started(f);
        assertEquals(LiveMigrationFixture.DESTINATION, f.current.get(LiveMigrationFixture.MOVING).owner());
        assertTrue(p.data().isEmpty());
        assertFalse(p.acceptsWrite());
        assertThrows(IllegalStateException.class, () -> f.source.put(KEY, NEW));
        assertEquals(ResultStatus.BUSY, p.execute(new Command.Get(hot)).status());
        assertEquals(ResultStatus.MISS, p.execute(new Command.Get(Bytes.utf8("absent"))).status());
        p.step(1);
        assertEquals(hot, f.source.pulled.getFirst());
        assertEquals(ResultStatus.BUSY, p.execute(new Command.Get(hot)).status());
        finish(p);
        assertEquals(expected, p.data());
        assertEquals(NEW, p.execute(new Command.Get(hot)).value().orElseThrow());
        assertEquals(1, f.current.version().value());
        assertEquals(1, f.current.get(LiveMigrationFixture.MOVING).epoch().value());
        assertEquals(1, f.commits);
        assertTrue(f.source.data.isEmpty());
    }

    @Test
    void latePullCannotOverwriteSuccessfulWriteDeleteOrDeleteThenRecreate() {
        for (int operation = 0; operation < 3; operation++) {
            var f = new LiveMigrationFixture(false);
            f.source.put(KEY, OLD);
            var p = started(f);
            p.step(1); // old reply is in flight
            if (operation == 0) assertEquals(ResultStatus.OK, p.execute(new Command.Set(KEY, NEW)).status());
            else {
                assertEquals(ResultStatus.OK, p.execute(new Command.Delete(KEY)).status());
                if (operation == 2) assertEquals(ResultStatus.OK, p.execute(new Command.Set(KEY, Bytes.empty())).status());
            }
            finish(p);
            if (operation == 1) {
                assertEquals(ResultStatus.MISS, p.execute(new Command.Get(KEY)).status());
                assertTrue(p.data().isEmpty());
            } else assertEquals(operation == 0 ? NEW : Bytes.empty(), p.execute(new Command.Get(KEY)).value().orElseThrow());
            assertEquals(1, p.completedPulls());
        }
    }

    @Test
    void writesResolveUnsentKeysAndDeletesDoNotRequirePulls() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        Bytes deleted = Bytes.utf8("deleted");
        f.source.put(deleted, OLD);
        var p = started(f);
        assertEquals(ResultStatus.OK, p.execute(new Command.Set(KEY, NEW)).status());
        assertEquals(ResultStatus.OK, p.execute(new Command.Delete(deleted)).status());
        assertEquals(ResultStatus.MISS, p.execute(new Command.Delete(deleted)).status());
        finish(p);
        assertEquals(0, p.sentBytes());
        assertTrue(f.source.pulled.isEmpty());
        assertEquals(Map.of(KEY, NEW), p.data());
    }

    @Test
    void sourceFailureKeepsOwnershipAndWritesAndExposesUnavailableUntilResume() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        Bytes another = Bytes.utf8("another");
        f.source.put(another, OLD);
        var p = started(f);
        assertEquals(ResultStatus.OK, p.execute(new Command.Set(KEY, NEW)).status());
        f.source.available = false;
        assertEquals(PostCopyState.FAILED, p.step(1_000));
        assertEquals(PostCopyState.FAILED, p.abort());
        assertEquals(ResultStatus.UNAVAILABLE, p.execute(new Command.Get(another)).status());
        assertEquals(NEW, p.execute(new Command.Get(KEY)).value().orElseThrow());
        assertEquals(0, f.source.cleanupCalls);
        assertEquals(LiveMigrationFixture.DESTINATION, f.current.get(LiveMigrationFixture.MOVING).owner());
        assertThrows(IllegalStateException.class, p::resume);
        f.source.available = true;
        assertEquals(PostCopyState.PULLING, p.resume());
        finish(p);
        assertEquals(Map.of(KEY, NEW, another, OLD), p.data());
    }

    @Test
    void invalidStaleAndWrongKeyRepliesNeverResolveOrDeleteSourceAndCanRecover() {
        for (String fault : List.of("checksum", "epoch", "session", "id", "key")) {
            var f = new LiveMigrationFixture(false);
            f.source.put(KEY, OLD);
            var p = started(f);
            f.source.transform = reply -> switch (fault) {
                case "checksum" -> new PullReply(reply.session(), reply.epoch(), reply.requestId(), reply.key(),
                        reply.value(), reply.checksum() ^ 1);
                case "epoch" -> PullReply.create(reply.session(), OwnershipEpoch.ZERO.next(), reply.requestId(), KEY, reply.value());
                case "session" -> PullReply.create(f.job("post-copy"), reply.epoch(), reply.requestId(), KEY, reply.value());
                case "id" -> PullReply.create(reply.session(), reply.epoch(), reply.requestId() + 1, KEY, reply.value());
                case "key" -> PullReply.create(reply.session(), reply.epoch(), reply.requestId(), Bytes.empty(), reply.value());
                default -> throw new IllegalArgumentException(fault);
            };
            p.step(10_000);
            assertEquals(0, p.completedPulls(), fault);
            assertEquals(ResultStatus.BUSY, p.execute(new Command.Get(KEY)).status());
            assertEquals(Map.of(KEY, OLD), f.source.data);
            assertEquals(PostCopyState.FAILED, p.fail());
            assertEquals(ResultStatus.UNAVAILABLE, p.execute(new Command.Get(KEY)).status());
            f.source.transform = reply -> reply;
            p.resume();
            finish(p);
            assertEquals(Map.of(KEY, OLD), p.data());
        }
    }

    @Test
    void missingManifestDataIsFailureAndNeverSilentMissOrDone() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = started(f);
        f.source.data.clear();
        assertEquals(PostCopyState.FAILED, p.step(1_000));
        assertEquals(ResultStatus.UNAVAILABLE, p.execute(new Command.Get(KEY)).status());
        assertEquals(0, f.source.cleanupCalls);
        f.source.data.put(KEY, OLD);
        p.resume();
        finish(p);
        assertEquals(Map.of(KEY, OLD), p.data());
    }

    @Test
    void cancellationOnlyBeforeCommitAndCleanupRetryCannotRollbackDestinationWrites() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = new PostCopyProtocol(f.source, f.current);
        p.begin(f.job(p.name()));
        assertEquals(ResultStatus.MOVED, p.execute(new Command.Get(KEY)).status());
        assertEquals(PostCopyState.ABORTED, p.abort());
        assertEquals(PostCopyState.ABORTED, p.abort());
        assertTrue(p.acceptsWrite());
        assertEquals(Map.of(KEY, OLD), f.source.data);
        assertEquals(0, f.commits);
        p = started(f);
        f.source.cleanupFailures = 2;
        f.source.partialCleanup = true;
        assertEquals(PostCopyState.PULLING, p.abort());
        assertEquals(PostCopyState.CLEANING_UP, p.step(10_000));
        assertEquals(PostCopyState.CLEANING_UP, p.abort());
        assertEquals(ResultStatus.OK, p.execute(new Command.Set(KEY, NEW)).status());
        assertEquals(PostCopyState.CLEANING_UP, p.step(0));
        assertEquals(PostCopyState.CLEANING_UP, p.step(0));
        assertEquals(PostCopyState.DONE, p.step(0));
        assertEquals(PostCopyState.DONE, p.abort());
        assertEquals(Map.of(KEY, NEW), p.data());
        assertFalse(p.acceptsWrite());
    }

    @Test
    void otherTabletsRemainWritableAndNeverGetCopiedOrDeleted() {
        var f = new LiveMigrationFixture(true);
        for (int i = 0; i < 100; i++) f.source.put(Bytes.utf8("key" + i), OLD);
        Map<Bytes, Bytes> expected = f.movingData();
        var otherTablet = f.current.get(LiveMigrationFixture.OTHER);
        Bytes other = f.source.data.keySet().stream().filter(key -> otherTablet.range().contains(KeyHash.of(key)))
                .findFirst().orElseThrow();
        var p = started(f);
        f.source.put(other, NEW);
        assertThrows(IllegalArgumentException.class, () -> p.execute(new Command.Get(other)));
        finish(p);
        assertEquals(expected, p.data());
        assertEquals(NEW, f.source.data.get(other));
        assertEquals(otherTablet, f.current.get(LiveMigrationFixture.OTHER));
    }

    @Test
    void byteBudgetsAreExactForOversizedReplyAndEmptyTabletStillCommitsFirst() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = started(f);
        assertEquals(PostCopyState.PULLING, p.step(0));
        assertEquals(0, p.sentBytes());
        long bytes = PullReply.create(f.job(p.name()), OwnershipEpoch.ZERO, 0, KEY, Optional.of(OLD)).serializedBytes();
        for (long i = 0; i < bytes; i++) {
            long before = p.sentBytes();
            p.step(1);
            assertEquals(1, p.sentBytes() - before);
        }
        assertEquals(1, p.completedPulls());
        finish(p);
        var empty = started(new LiveMigrationFixture(false));
        assertEquals(PostCopyState.CLEANING_UP, empty.step(0));
        assertEquals(PostCopyState.DONE, empty.step(0));
        assertEquals(0, empty.sentBytes());
    }

    @Test
    void lostReplyTimesOutWithoutRollbackAndRecoveryRetriesTheSameRequest() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = started(f);
        f.source.dropPull = true;
        p.step(10_000);
        assertEquals(0, p.sentBytes());
        assertEquals(PostCopyState.FAILED, p.fail());
        assertEquals(Map.of(KEY, OLD), f.source.data);
        f.source.dropPull = false;
        p.resume();
        finish(p);
        assertEquals(Map.of(KEY, OLD), p.data());
    }

    @Test
    void invalidLifecycleWrongPublicationAndFreezeConflictAreRejected() {
        var f = new LiveMigrationFixture(false);
        var p = new PostCopyProtocol(f.source, f.current);
        assertThrows(IllegalStateException.class, () -> p.step(0));
        assertThrows(IllegalStateException.class, p::abort);
        assertThrows(IllegalStateException.class, p::fail);
        assertThrows(IllegalArgumentException.class, () -> p.begin(f.job("pre-copy")));
        var tablet = f.current.get(LiveMigrationFixture.MOVING);
        f.source.freeze(tablet);
        assertEquals(PostCopyState.ABORTED, p.begin(f.job(p.name())));
        assertTrue(f.source.frozen(tablet));
        assertFalse(p.acceptsWrite());
        f.source.unfreeze(tablet);
        var q = new PostCopyProtocol(f.source, f.current);
        q.begin(f.job(q.name()));
        assertThrows(IllegalArgumentException.class, () -> q.committed(f.current));
        assertThrows(IllegalArgumentException.class, () -> q.step(-1));
        assertThrows(IllegalStateException.class, () -> q.begin(f.job(q.name())));
        f.source.unfreeze(tablet);
        assertFalse(q.readyToCommit());
        assertEquals(PostCopyState.ABORTED, q.abort());
    }

    @Test
    void writesCanResolveAPendingLostReplyWithoutWaitingForIt() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = started(f);
        f.source.dropPull = true;
        p.step(1_000);
        assertEquals(ResultStatus.OK, p.execute(new Command.Set(KEY, NEW)).status());
        finish(p);
        assertEquals(Map.of(KEY, NEW), p.data());
        assertEquals(0, p.completedPulls());
    }

    @Test
    void seededReadsWritesAndDeletesPreserveLogicalStateDuringPartialPulls() {
        for (int seed = 0; seed < 5; seed++) {
            var f = new LiveMigrationFixture(false);
            for (int i = 0; i < 100; i++) f.source.put(Bytes.utf8("key" + i), OLD);
            Map<Bytes, Bytes> expected = new LinkedHashMap<>(f.source.data);
            var p = started(f);
            var random = new SplittableRandom(seed);
            for (int tick = 0; tick < 10_000 && p.state() != PostCopyState.DONE; tick++) {
                Bytes key = Bytes.utf8("key" + random.nextInt(150));
                int operation = random.nextInt(3);
                if (operation == 0) {
                    Bytes value = Bytes.utf8("value" + tick);
                    assertEquals(ResultStatus.OK, p.execute(new Command.Set(key, value)).status());
                    expected.put(key, value);
                } else if (operation == 1) {
                    boolean existed = expected.remove(key) != null;
                    assertEquals(existed ? ResultStatus.OK : ResultStatus.MISS, p.execute(new Command.Delete(key)).status());
                } else {
                    var result = p.execute(new Command.Get(key));
                    if (!expected.containsKey(key)) assertEquals(ResultStatus.MISS, result.status());
                    else if (result.status() == ResultStatus.OK) assertEquals(expected.get(key), result.value().orElseThrow());
                    else assertEquals(ResultStatus.BUSY, result.status());
                }
                long budget = random.nextLong(256);
                long before = p.sentBytes();
                p.step(budget);
                assertTrue(p.sentBytes() - before <= budget);
            }
            assertEquals(PostCopyState.DONE, p.state(), "seed " + seed);
            assertEquals(expected, p.data(), "seed " + seed);
            assertEquals(1, f.commits);
            assertTrue(f.source.data.isEmpty());
        }
    }
}
