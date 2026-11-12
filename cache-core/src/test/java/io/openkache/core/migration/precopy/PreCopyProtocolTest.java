package io.openkache.core.migration.precopy;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.migration.JobState;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.experiment.LiveMigrationFixture;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.KeyHash;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class PreCopyProtocolTest {
    private static final Bytes KEY = Bytes.utf8("key");
    private static final Bytes OLD = Bytes.utf8("old");
    private static final Bytes NEW = Bytes.utf8("new");

    private static PreCopyProtocol protocol(LiveMigrationFixture f, long threshold, int maxRounds) {
        return new PreCopyProtocol(f.source, f.link, f.current, new PreCopyProtocol.Settings(90, threshold, maxRounds));
    }

    private static void finish(LiveMigrationFixture f, PreCopyProtocol p) {
        for (int i = 0; i < 10_000 && p.state() != JobState.DONE; i++) {
            long before = p.job().progress().sentBytes();
            p.step(37);
            assertTrue(p.job().progress().sentBytes() - before <= 37);
            if (p.readyToCommit()) assertTrue(f.commit(p));
        }
        assertEquals(JobState.DONE, p.state());
        assertEquals(1, f.commits);
    }

    @Test
    void dirtyHistoryShowsDecreaseThenGrowthAndStopsAtActualDecision() {
        var f = new LiveMigrationFixture(false);
        var keys = List.of(Bytes.utf8("k0"), Bytes.utf8("k1"), Bytes.utf8("k2"));
        keys.forEach(k -> f.source.put(k, OLD));
        var p = protocol(f, 0, 10);
        p.begin(f.job(p.name()));
        keys.forEach(k -> f.source.put(k, NEW));
        p.step(10_000); // generation 1: dirty 3
        f.source.put(keys.getFirst(), NEW);
        p.step(10_000); // generation 2: dirty 1
        f.source.put(keys.get(0), NEW);
        f.source.put(keys.get(1), NEW);
        p.step(10_000); // generation 3: dirty 2, freeze
        assertEquals(PreCopyProtocol.StopReason.NOT_SHRINKING, p.stopReason().orElseThrow());
        var starts = p.roundEvents().stream().filter(e -> e.event() == PreCopyProtocol.RoundEventType.STARTED).toList();
        assertEquals(List.of(3, 1, 2), starts.stream().map(PreCopyProtocol.RoundEvent::dirtyKeys).toList());
        assertTrue(starts.get(0).dirtyBytes() > starts.get(1).dirtyBytes());
        assertTrue(starts.get(1).dirtyBytes() < starts.get(2).dirtyBytes());
        assertNull(starts.getFirst().previousDirtyBytes());
        assertEquals(starts.get(1).dirtyBytes(), starts.get(2).previousDirtyBytes());
        assertEquals(2, p.rounds());
        assertTrue(starts.getLast().finalRound());
        finish(f, p);
        assertEquals(6, p.roundEvents().size());
        var end = p.roundEvents().getLast();
        assertEquals(PreCopyProtocol.RoundEventType.ACKNOWLEDGED, end.event());
        assertEquals(end.dirtyBytes(), end.roundSentBytes());
        assertEquals(6, end.acknowledgedSequence());
        assertThrows(UnsupportedOperationException.class, () -> p.roundEvents().clear());
    }

    @Test
    void historyKeepsTombstonesAndDistinguishesEstimateFromLiveTransfer() {
        var f = new LiveMigrationFixture(false);
        Bytes deleted = Bytes.utf8("deleted");
        f.source.put(KEY, OLD); f.source.put(deleted, OLD);
        var p = protocol(f, 0, 10);
        p.begin(f.job(p.name()));
        f.source.put(KEY, NEW); f.source.delete(deleted);
        p.step(10_000);
        var start = p.roundEvents().getFirst();
        assertEquals(2, start.dirtyKeys()); assertEquals(1, start.tombstoneKeys());
        f.source.put(KEY, Bytes.utf8("longer-value-created-after-the-generation-boundary"));
        p.step(10_000);
        var ack = p.roundEvents().get(1);
        assertEquals(start.dirtyBytes(), ack.dirtyBytes());
        assertNotEquals(ack.dirtyBytes(), ack.roundSentBytes());
        finish(f, p);
        assertFalse(f.link.receiver.data().containsKey(deleted));
    }

    @Test
    void missingAckIsNotCompletionAndAbortPreservesPartialObservation() {
        for (long budget : new long[] {1, 10_000}) {
            var f = new LiveMigrationFixture(false);
            f.source.put(KEY, OLD);
            var p = protocol(f, 1_000, 10);
            p.begin(f.job(p.name())); f.source.put(KEY, NEW); p.step(10_000);
            f.link.holdFinalAck = true;
            p.step(budget);
            var before = p.roundEvents();
            assertEquals(1, before.size()); // full transfer alone is not an ACK
            p.abort(); p.abort();
            assertEquals(1, before.size());
            assertEquals(2, p.roundEvents().size());
            var aborted = p.roundEvents().getLast();
            assertEquals(PreCopyProtocol.RoundEventType.ABORTED, aborted.event());
            assertEquals(Math.min(budget, before.getFirst().dirtyBytes()), aborted.roundSentBytes());
            assertEquals(0, aborted.acknowledgedSequence());
            assertEquals(Map.of(KEY, NEW), f.source.data);
        }
    }

    @Test
    void emptyFinalRoundStillRecordsHeaderAndAckWithoutSequenceGrowth() {
        var f = new LiveMigrationFixture(false);
        var p = protocol(f, 1_000, 10);
        p.begin(f.job(p.name())); finish(f, p);
        var end = p.roundEvents().getLast();
        assertEquals(0, end.dirtyKeys());
        assertEquals(DeltaBatch.HEADER_BYTES, end.dirtyBytes());
        assertEquals(DeltaBatch.HEADER_BYTES, end.roundSentBytes());
        assertEquals(0, end.lastSequence());
    }

    @Test
    void snapshotBarrierKeepsWritesAndDeletesUntilFinalSequenceAck() {
        var f = new LiveMigrationFixture(false);
        Bytes deleted = Bytes.utf8("deleted");
        f.source.put(KEY, OLD);
        f.source.put(deleted, OLD);
        var p = protocol(f, 1_000, 10);
        var session = f.job(p.name());
        assertEquals(JobState.COPYING, p.begin(session));
        assertTrue(p.acceptsWrite());
        f.link.holdSnapshotAck = true;
        p.step(10_000);
        f.source.put(KEY, NEW);
        f.source.delete(deleted);
        f.source.put(Bytes.utf8("created"), Bytes.empty());
        p.step(10_000);
        assertEquals(JobState.COPYING, p.state());
        assertNull(f.link.lastDelta);
        assertFalse(p.readyToCommit());
        f.link.holdSnapshotAck = false;
        p.step(10_000);
        assertEquals(JobState.SWITCHING, p.state());
        assertFalse(p.acceptsWrite());
        assertFalse(p.readyToCommit());
        Map<Bytes, Bytes> expected = Map.copyOf(f.source.data);
        f.link.holdFinalAck = true;
        p.step(10_000);
        assertFalse(p.readyToCommit());
        assertFalse(f.commit(p));
        long sent = p.job().progress().sentBytes();
        p.step(10_000);
        assertEquals(sent, p.job().progress().sentBytes());
        f.link.holdFinalAck = false;
        finish(f, p);
        assertEquals(expected, f.link.receiver.data());
        assertTrue(f.source.data.isEmpty());
        assertEquals(3, p.acknowledgedSequence());
        assertEquals(1, f.current.version().value());
        assertEquals(1, f.current.get(LiveMigrationFixture.MOVING).epoch().value());
        assertFalse(p.acceptsWrite());
        assertEquals(JobState.DONE, p.abort());
    }

    @Test
    void writeDuringSendingGenerationIsNotLostAndNonShrinkingRoundsFreeze() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = protocol(f, 0, 10);
        p.begin(f.job(p.name()));
        f.source.put(KEY, NEW);
        p.step(10_000); // snapshot ACK → first dirty generation
        assertEquals(JobState.CATCHING_UP, p.state());
        p.step(1); // delta captures NEW, only one byte has been sent
        f.source.put(KEY, Bytes.utf8("latest"));
        f.source.put(Bytes.utf8("another"), NEW);
        assertTrue(p.acceptsWrite());
        p.step(10_000);
        assertEquals(JobState.SWITCHING, p.state());
        assertEquals(PreCopyProtocol.StopReason.NOT_SHRINKING, p.stopReason().orElseThrow());
        Map<Bytes, Bytes> expected = Map.copyOf(f.source.data);
        finish(f, p);
        assertEquals(expected, f.link.receiver.data());
        assertEquals(3, p.acknowledgedSequence());
    }

    @Test
    void roundLimitAndThresholdAreIndependentTerminationConditions() {
        for (boolean limit : new boolean[] {false, true}) {
            var f = new LiveMigrationFixture(false);
            f.source.put(KEY, OLD);
            var p = protocol(f, limit ? 0 : 1_000, limit ? 0 : 10);
            p.begin(f.job(p.name()));
            f.source.delete(KEY);
            p.step(10_000);
            assertEquals(limit ? PreCopyProtocol.StopReason.ROUND_LIMIT : PreCopyProtocol.StopReason.THRESHOLD,
                    p.stopReason().orElseThrow());
            finish(f, p);
            assertTrue(f.link.receiver.data().isEmpty());
        }
    }

    @Test
    void abortAtCopyCatchupAndFinalTransferPreservesSuccessfulSourceChanges() {
        for (JobState boundary : new JobState[] {JobState.COPYING, JobState.CATCHING_UP, JobState.SWITCHING}) {
            var f = new LiveMigrationFixture(false);
            f.source.put(KEY, OLD);
            var p = protocol(f, boundary == JobState.SWITCHING ? 1_000 : 0, 10);
            var original = f.current;
            p.begin(f.job(p.name()));
            f.source.put(KEY, NEW);
            if (boundary != JobState.COPYING) p.step(10_000);
            assertEquals(boundary, p.state());
            p.step(1);
            assertEquals(JobState.ABORTED, p.abort());
            assertEquals(JobState.ABORTED, p.abort());
            assertSame(original, f.current);
            assertEquals(Map.of(KEY, NEW), f.source.data);
            assertTrue(f.link.receiver.data().isEmpty());
            assertTrue(p.acceptsWrite());
            f.source.put(KEY, OLD); // tracking/freeze was released
        }
    }

    @Test
    void corruptDeltaAndLostFinalAckCannotCommitAndCanAbort() {
        for (boolean corrupt : new boolean[] {true, false}) {
            var f = new LiveMigrationFixture(false);
            f.source.put(KEY, OLD);
            var p = protocol(f, 1_000, 10);
            p.begin(f.job(p.name()));
            f.source.put(KEY, NEW);
            p.step(10_000);
            f.link.corruptDelta = corrupt;
            f.link.holdFinalAck = !corrupt;
            for (int i = 0; i < 5; i++) p.step(10_000);
            assertFalse(p.readyToCommit());
            assertFalse(f.commit(p));
            assertEquals(JobState.ABORTED, p.abort());
            assertEquals(Map.of(KEY, NEW), f.source.data);
        }
    }

    @Test
    void cleanupRetriesAndFreezeAreScopedToMovingTablet() {
        var f = new LiveMigrationFixture(true);
        for (int i = 0; i < 100; i++) f.source.put(Bytes.utf8("key" + i), OLD);
        var p = protocol(f, 1_000, 10);
        var moving = f.current.get(LiveMigrationFixture.MOVING);
        Bytes other = f.source.data.keySet().stream().filter(key -> !moving.range().contains(KeyHash.of(key)))
                .findFirst().orElseThrow();
        Map<Bytes, Bytes> expected = f.movingData();
        var unchanged = f.current.get(LiveMigrationFixture.OTHER);
        f.source.cleanupFailures = 2;
        f.source.partialCleanup = true;
        p.begin(f.job(p.name()));
        while (!p.readyToCommit()) p.step(10_000);
        f.source.put(other, NEW);
        assertTrue(f.commit(p));
        assertEquals(JobState.CLEANING_UP, p.abort());
        assertEquals(JobState.CLEANING_UP, p.step(0));
        assertEquals(JobState.CLEANING_UP, p.step(0));
        assertEquals(JobState.DONE, p.step(0));
        assertEquals(expected, f.link.receiver.data());
        assertEquals(NEW, f.source.data.get(other));
        assertEquals(unchanged, f.current.get(LiveMigrationFixture.OTHER));
    }

    @Test
    void freezeConflictsLostFenceAndStaleRoutingNeverCleanSource() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = protocol(f, 1_000, 10);
        var tablet = f.current.get(LiveMigrationFixture.MOVING);
        assertTrue(f.source.freeze(tablet));
        assertEquals(JobState.ABORTED, p.begin(f.job(p.name())));
        assertTrue(f.source.frozen(tablet));
        assertFalse(p.acceptsWrite());
        f.source.unfreeze(tablet);
        p = protocol(f, 1_000, 10);
        p.begin(f.job(p.name()));
        while (!p.readyToCommit()) p.step(10_000);
        var waiting = p;
        assertThrows(IllegalArgumentException.class, () -> waiting.committed(waiting.expectedTable()));
        f.current = f.current.apply(waiting.job().change()); // competing CAS winner
        assertFalse(f.commit(p));
        assertEquals(JobState.ABORTED, p.abort());
        assertEquals(Map.of(KEY, OLD), f.source.data);
        assertEquals(0, f.source.cleanupCalls);
    }

    @Test
    void receiverRejectsGapsCorruptionStaleSessionAndLateSnapshotWithoutPartialApplication() {
        var f = new LiveMigrationFixture(false);
        var receiver = f.link.receiver;
        var session = f.job("pre-copy");
        var tablet = f.current.get(LiveMigrationFixture.MOVING);
        assertTrue(receiver.prepare(session, tablet));
        var put = new DeltaBatch.Delta(1, KEY, Optional.of(NEW));
        var delta = DeltaBatch.create(1, true, true, List.of(put));
        receiver.receiveDelta(session, tablet.epoch(), delta);
        assertTrue(receiver.data().isEmpty());
        var snapshot = TransferBatch.create(0, true, List.of(new TransferBatch.Record(KEY, OLD)));
        receiver.receive(session, tablet.epoch(), snapshot);
        receiver.receiveDelta(f.job("pre-copy"), tablet.epoch(), delta);
        receiver.receiveDelta(session, OwnershipEpoch.ZERO.next(), delta);
        receiver.receiveDelta(session, tablet.epoch(), DeltaBatch.create(2, true, true, List.of(put)));
        receiver.receiveDelta(session, tablet.epoch(), new DeltaBatch(1, 1, delta.serializedBytes(), delta.checksum() ^ 1,
                true, true, List.of(put)));
        assertEquals(Map.of(KEY, OLD), receiver.data());
        assertTrue(receiver.deltaAcknowledgement(session, 1).isEmpty());
        receiver.receiveDelta(session, tablet.epoch(), delta);
        receiver.receiveDelta(session, tablet.epoch(), delta);
        receiver.receive(session, tablet.epoch(), snapshot);
        assertEquals(Map.of(KEY, NEW), receiver.data());
        assertTrue(receiver.caughtUp(session, 1));
        assertFalse(receiver.caughtUp(session, 2));
        receiver.receiveDelta(session, tablet.epoch(), DeltaBatch.create(2, true, true,
                List.of(new DeltaBatch.Delta(2, KEY, Optional.empty()))));
        assertEquals(Map.of(KEY, NEW), receiver.data());
    }

    @Test
    void emptyTabletRequiresSnapshotAndFinalBarrierWithinBudget() {
        var f = new LiveMigrationFixture(false);
        var p = protocol(f, 1_000, 10);
        assertThrows(IllegalStateException.class, () -> p.step(1));
        assertThrows(IllegalStateException.class, p::abort);
        assertThrows(IllegalArgumentException.class, () -> p.begin(f.job("stop-and-copy")));
        p.begin(f.job(p.name()));
        assertThrows(IllegalStateException.class, () -> p.begin(f.job(p.name())));
        assertThrows(IllegalArgumentException.class, () -> p.step(-1));
        assertEquals(JobState.COPYING, p.step(0));
        for (int i = 0; i < TransferBatch.HEADER_BYTES; i++) p.step(1);
        assertEquals(JobState.SWITCHING, p.state());
        assertFalse(p.readyToCommit());
        for (int i = 0; i < DeltaBatch.HEADER_BYTES; i++) p.step(1);
        assertTrue(p.readyToCommit());
        assertEquals(TransferBatch.HEADER_BYTES + DeltaBatch.HEADER_BYTES, p.job().progress().sentBytes());
        assertTrue(f.commit(p));
        p.step(0);
        assertEquals(JobState.DONE, p.state());
    }

    @Test
    void receiverValidatesWholeDeltaBeforeMutatingAndDistinguishesEmptyPutFromDelete() {
        var f = new LiveMigrationFixture(false);
        var receiver = f.link.receiver;
        var session = f.job("pre-copy");
        var tablet = f.current.get(LiveMigrationFixture.MOVING);
        receiver.prepare(session, tablet);
        receiver.receive(session, tablet.epoch(), TransferBatch.create(0, true, List.of()));
        receiver.receiveDelta(session, tablet.epoch(), DeltaBatch.create(1, true, false, List.of(
                new DeltaBatch.Delta(1, KEY, Optional.of(NEW)),
                new DeltaBatch.Delta(3, Bytes.utf8("bad-seq"), Optional.of(NEW)))));
        assertTrue(receiver.data().isEmpty());
        receiver.receiveDelta(session, tablet.epoch(), DeltaBatch.create(1, true, false,
                List.of(new DeltaBatch.Delta(1, KEY, Optional.of(Bytes.empty())))));
        assertEquals(Map.of(KEY, Bytes.empty()), receiver.data());
        receiver.receiveDelta(session, tablet.epoch(), DeltaBatch.create(2, true, true,
                List.of(new DeltaBatch.Delta(2, KEY, Optional.empty()))));
        assertTrue(receiver.data().isEmpty());
    }

    @Test
    void seededConcurrentWritesAndDeletesPreserveExactDataAcrossVariableBudgets() {
        for (int seed = 0; seed < 5; seed++) {
            var f = new LiveMigrationFixture(false);
            for (int i = 0; i < 100; i++) f.source.put(Bytes.utf8("key" + i), OLD);
            var p = protocol(f, 128, 3);
            p.begin(f.job(p.name()));
            var random = new SplittableRandom(seed);
            Map<Bytes, Bytes> expected = Map.copyOf(f.source.data);
            for (int tick = 0; tick < 10_000 && p.state() != JobState.DONE; tick++) {
                if (p.acceptsWrite()) {
                    Bytes key = Bytes.utf8("key" + random.nextInt(150));
                    if (random.nextBoolean()) f.source.put(key, Bytes.utf8("value" + tick));
                    else f.source.delete(key);
                    expected = Map.copyOf(f.source.data);
                }
                long budget = random.nextLong(256);
                long before = p.job().progress().sentBytes();
                p.step(budget);
                assertTrue(p.job().progress().sentBytes() - before <= budget);
                if (p.readyToCommit()) assertTrue(f.commit(p));
            }
            assertEquals(JobState.DONE, p.state(), "seed " + seed);
            assertEquals(expected, f.link.receiver.data(), "seed " + seed);
            assertTrue(f.source.data.isEmpty());
            assertEquals(1, f.commits);
        }
    }

    @Test
    void lostFreezeBeforeFinalAckAbortsWithoutPublishingOrCleaning() {
        var f = new LiveMigrationFixture(false);
        f.source.put(KEY, OLD);
        var p = protocol(f, 1_000, 10);
        p.begin(f.job(p.name()));
        p.step(10_000);
        f.source.unfreeze(f.current.get(LiveMigrationFixture.MOVING));
        assertFalse(p.readyToCommit());
        assertEquals(JobState.ABORTED, p.step(10_000));
        assertEquals(Map.of(KEY, OLD), f.source.data);
        assertEquals(0, f.commits);
        assertEquals(0, f.source.cleanupCalls);
    }
}
