package io.openkache.core.migration.stopcopy;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.migration.*;
import io.openkache.core.migration.experiment.MigrationFixture;
import io.openkache.core.migration.experiment.TickRunner;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.KeyHash;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class StopAndCopyProtocolTest extends SourceOwnedMigrationContract {
    @Test
    void oneByteBudgetTransfersAnOversizedRecordWithoutCreditBanking() {
        MigrationFixture f = new MigrationFixture(false, MigrationFixture.Fault.NONE, 80);
        f.source.data.put(Bytes.empty(), MigrationFixture.value(1));
        var p = f.protocol;
        p.begin(f.initialJob);
        assertEquals(JobState.COPYING, p.step(0));
        assertEquals(0, p.job().progress().sentBytes());
        long ticks = 0;
        while (p.state() == JobState.COPYING && ticks++ < 1_000) {
            long before = p.job().progress().sentBytes();
            p.step(1);
            assertEquals(1, p.job().progress().sentBytes() - before);
            if (p.state() == JobState.COPYING) assertTrue(f.destination.isEmpty());
        }
        assertEquals(TransferBatch.HEADER_BYTES + 8 + 128, ticks);
        assertTrue(p.readyToCommit());
        finish(f, 1);
    }

    @Test
    void emptyTabletStillNeedsFinalBatchAndAck() {
        MigrationFixture f = new MigrationFixture(false, MigrationFixture.Fault.NONE);
        f.protocol.begin(f.initialJob);
        f.protocol.step(TransferBatch.HEADER_BYTES - 1);
        assertFalse(f.protocol.readyToCommit());
        f.protocol.step(1);
        assertEquals(0, f.protocol.job().progress().lastAckBatchId());
        assertTrue(f.protocol.readyToCommit());
        finish(f, 1);
    }

    @Test
    void corruptDropLostAckAndBadAckNeverCommitAndTimeoutRestoresSource() {
        for (MigrationFixture.Fault fault : new MigrationFixture.Fault[] {
                MigrationFixture.Fault.CHECKSUM, MigrationFixture.Fault.COUNT, MigrationFixture.Fault.BYTES,
                MigrationFixture.Fault.DROP, MigrationFixture.Fault.BAD_ACK, MigrationFixture.Fault.LOST_ACK}) {
            MigrationFixture f = new MigrationFixture(false, fault);
            f.source.data.put(Bytes.utf8("key"), Bytes.utf8("value"));
            Map<Bytes, Bytes> expected = new LinkedHashMap<>(f.source.data);
            f.protocol.begin(f.initialJob);
            TickRunner runner = new TickRunner(f, 5);
            for (int tick = 0; tick < 6; tick++) {
                assertFalse(f.coordinator.commit(f.protocol.job(), f.protocol.expectedTable()));
                runner.step(1_000);
            }
            assertEquals(JobState.ABORTED, f.protocol.state(), fault.name());
            assertSame(f.protocol.expectedTable(), f.coordinator.current());
            assertEquals(expected, f.source.data);
            assertTrue(f.destination.isEmpty());
            assertEquals(0, f.coordinator.successfulCommits);
        }
    }

    @Test
    void duplicateDeliveryAndDelayedAckAreSafeAndPollDoesNotSendAgain() {
        for (MigrationFixture.Fault fault : new MigrationFixture.Fault[] {
                MigrationFixture.Fault.DUPLICATE, MigrationFixture.Fault.ACK_DELAY}) {
            MigrationFixture f = new MigrationFixture(false, fault);
            f.source.data.put(Bytes.utf8("key"), Bytes.utf8("value"));
            f.protocol.begin(f.initialJob);
            f.protocol.step(1_000);
            if (fault == MigrationFixture.Fault.ACK_DELAY) {
                assertTrue(f.link.complete(f.initialJob));
                assertEquals(JobState.COPYING, f.protocol.state());
                assertFalse(f.protocol.readyToCommit());
            }
            finish(f, 1_000);
            assertEquals(Map.of(Bytes.utf8("key"), Bytes.utf8("value")), f.destination);
            assertEquals(TransferBatch.HEADER_BYTES + 8 + 3 + 5, f.protocol.job().progress().sentBytes());
        }
    }

    @Test
    void conflictingCommitKeepsWinningTableAndAbortsStaleJob() {
        MigrationFixture f = populated(true);
        var p = f.protocol;
        p.begin(f.initialJob);
        var otherTablet = p.expectedTable().get(MigrationFixture.OTHER);
        assertTrue(f.source.freeze(otherTablet));
        MigrationJob other = new MigrationJob(new Change(MigrationFixture.OTHER, MigrationFixture.SOURCE, MigrationFixture.DESTINATION),
                p.name(), JobState.SWITCHING, new TransferProgress(true, 0, 0, 1));
        assertTrue(f.coordinator.commit(other, p.expectedTable()));
        var winner = f.coordinator.current();
        TickRunner runner = new TickRunner(f, 100);
        while (p.state() == JobState.COPYING) runner.step(100_000);
        assertEquals(JobState.ABORTED, runner.step(1));
        assertSame(winner, f.coordinator.current());
        assertTrue(f.destination.isEmpty());
        assertFalse(f.source.frozen(p.expectedTable().get(MigrationFixture.MOVING)));
    }

    @Test
    void freezeMustStillHoldAndPublicationMustBeRealBeforeCleanup() {
        MigrationFixture f = populated(false);
        var p = f.protocol;
        p.begin(f.initialJob);
        while (p.state() == JobState.COPYING) p.step(100_000);
        assertThrows(IllegalArgumentException.class, () -> p.committed(p.expectedTable()));
        assertEquals(JobState.SWITCHING, p.state());
        f.source.unfreeze(p.expectedTable().get(MigrationFixture.MOVING));
        assertFalse(p.readyToCommit());
        assertEquals(JobState.ABORTED, new TickRunner(f, 100).step(1));
        assertEquals(100, f.source.data.size());
    }

    @Test
    void freezeConflictDoesNotReleaseFirstJobsFreeze() {
        MigrationFixture f = populated(false);
        f.protocol.begin(f.initialJob);
        var otherLink = f.new Link(MigrationFixture.Fault.NONE, new LinkedHashMap<>());
        var competing = new StopAndCopyProtocol(f.source, otherLink, f.protocol.expectedTable());
        assertEquals(JobState.ABORTED, competing.begin(MigrationJob.pending(f.initialJob.change(), "stop-and-copy")));
        assertTrue(f.source.frozen(f.protocol.expectedTable().get(MigrationFixture.MOVING)));
        assertFalse(competing.acceptsWrite());
        finish(f, 100_000);
    }

    @Test
    void receiverRejectsGapsConflictingDuplicateWrongSessionAndStaleEpoch() {
        MigrationFixture f = new MigrationFixture(false, MigrationFixture.Fault.NONE);
        var tablet = f.protocol.expectedTable().get(MigrationFixture.MOVING);
        assertTrue(f.link.prepare(f.initialJob, tablet));
        TransferBatch first = TransferBatch.create(0, false, List.of(new TransferBatch.Record(Bytes.utf8("a"), Bytes.utf8("1"))));
        TransferBatch last = TransferBatch.create(1, true, List.of(new TransferBatch.Record(Bytes.utf8("b"), Bytes.utf8("2"))));
        assertFalse(f.link.deliver(f.initialJob, tablet.epoch(), last));
        assertTrue(f.destination.isEmpty());
        assertTrue(f.link.deliver(f.initialJob, tablet.epoch(), first));
        assertTrue(f.link.deliver(f.initialJob, tablet.epoch(), first));
        assertFalse(f.link.deliver(f.initialJob, tablet.epoch(), TransferBatch.create(0, false,
                List.of(new TransferBatch.Record(Bytes.utf8("a"), Bytes.utf8("wrong"))))));
        assertFalse(f.link.deliver(MigrationJob.pending(f.initialJob.change(), "stop-and-copy"), tablet.epoch(), last));
        assertFalse(f.link.deliver(f.initialJob, OwnershipEpoch.ZERO.next(), last));
        assertTrue(f.link.deliver(f.initialJob, tablet.epoch(), last));
        assertEquals(Map.of(Bytes.utf8("a"), Bytes.utf8("1"), Bytes.utf8("b"), Bytes.utf8("2")), f.destination);
        f.link.discard(f.initialJob);
        MigrationJob next = MigrationJob.pending(f.initialJob.change(), "stop-and-copy");
        assertTrue(f.link.prepare(next, tablet));
        assertFalse(f.link.deliver(f.initialJob, tablet.epoch(), first));
        assertTrue(f.destination.isEmpty());
    }

    @Test
    void outOfRangeAndRepeatedKeyBatchAreRejectedWithoutPartialMutation() {
        MigrationFixture f = populated(true);
        var tablet = f.protocol.expectedTable().get(MigrationFixture.MOVING);
        Bytes moving = f.movingData().keySet().iterator().next();
        Bytes other = f.source.data.keySet().stream().filter(k -> !tablet.range().contains(KeyHash.of(k))).findFirst().orElseThrow();
        f.link.prepare(f.initialJob, tablet);
        assertFalse(f.link.deliver(f.initialJob, tablet.epoch(), TransferBatch.create(0, true, List.of(
                new TransferBatch.Record(moving, Bytes.empty()), new TransferBatch.Record(other, Bytes.empty())))));
        assertTrue(f.destination.isEmpty());
        assertFalse(f.link.deliver(f.initialJob, tablet.epoch(), TransferBatch.create(0, true, List.of(
                new TransferBatch.Record(moving, Bytes.empty()), new TransferBatch.Record(moving, Bytes.empty())))));
        assertTrue(f.destination.isEmpty());
    }

    @Test
    void seededBudgetsHaveIdenticalStatesAndNeverExceedBudget() {
        MigrationFixture a = populated(false);
        MigrationFixture b = populated(false);
        a.protocol.begin(a.initialJob);
        b.protocol.begin(b.initialJob);
        TickRunner ra = new TickRunner(a, 1_000);
        TickRunner rb = new TickRunner(b, 1_000);
        SplittableRandom random = new SplittableRandom(42);
        int ticks = 0;
        while (a.protocol.state() != JobState.DONE && ticks++ < 1_010) {
            long budget = random.nextLong(1_000);
            long before = a.protocol.job().progress().sentBytes();
            assertEquals(ra.step(budget), rb.step(budget));
            assertEquals(a.protocol.job(), b.protocol.job());
            assertTrue(a.protocol.job().progress().sentBytes() - before <= budget);
        }
        assertEquals(JobState.DONE, a.protocol.state());
        assertEquals(a.destination, b.destination);
    }

    @Test
    void rejectsInvalidLifecycleAndMetadata() {
        MigrationFixture f = populated(false);
        assertThrows(IllegalStateException.class, () -> f.protocol.step(1));
        assertThrows(IllegalStateException.class, f.protocol::abort);
        MigrationJob stale = MigrationJob.pending(new Change(MigrationFixture.MOVING, new NodeId(3), MigrationFixture.DESTINATION), "stop-and-copy");
        assertThrows(IllegalArgumentException.class, () -> f.protocol.begin(stale));
        f.protocol.begin(f.initialJob);
        assertThrows(IllegalStateException.class, () -> f.protocol.begin(f.initialJob));
        assertThrows(IllegalArgumentException.class, () -> f.protocol.step(-1));
        assertThrows(IllegalStateException.class, () -> f.protocol.committed(f.coordinator.current()));
        assertThrows(IllegalArgumentException.class, () -> new TransferProgress(true, -1, -1, 0));
        f.protocol.abort();
    }
}
