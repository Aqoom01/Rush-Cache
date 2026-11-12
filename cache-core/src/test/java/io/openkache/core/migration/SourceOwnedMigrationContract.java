package io.openkache.core.migration;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Command;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.experiment.MigrationFixture;
import io.openkache.core.migration.experiment.TickRunner;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 설계 §5의 source-owned 불변식. 현재 fixture는 stop-and-copy를 사용한다. */
public abstract class SourceOwnedMigrationContract {
    protected static MigrationFixture populated(boolean twoTablets) {
        MigrationFixture f = new MigrationFixture(twoTablets, MigrationFixture.Fault.NONE, 400);
        for (int i = 0; i < 100; i++) f.source.data.put(MigrationFixture.key(1, i), MigrationFixture.value(i));
        return f;
    }

    protected static void finish(MigrationFixture f, long budget) {
        TickRunner runner = new TickRunner(f, 10_000);
        int tick = 0;
        while (f.protocol.state() != JobState.DONE && f.protocol.state() != JobState.ABORTED && tick++ < 10_010) {
            runner.step(budget);
        }
        assertEquals(JobState.DONE, f.protocol.state());
    }

    @Test
    void acknowledgedWritesAndDeletesSurviveCutoverWithExactlyOneOwner() {
        MigrationFixture f = populated(false);
        Bytes key = MigrationFixture.key(1, 3);
        Bytes deleted = MigrationFixture.key(1, 4);
        assertEquals(ResultStatus.OK, f.request(MigrationFixture.SOURCE, new Command.Set(key, MigrationFixture.value(999))).status());
        assertEquals(ResultStatus.OK, f.request(MigrationFixture.SOURCE, new Command.Delete(deleted)).status());
        Map<Bytes, Bytes> expected = new LinkedHashMap<>(f.source.data);
        var p = f.protocol;
        p.begin(f.initialJob);
        TickRunner runner = new TickRunner(f, 10_000);
        int ticks = 0;
        while (p.state() != JobState.DONE && ticks++ < 10_000) {
            var sourceWrite = f.request(MigrationFixture.SOURCE, new Command.Set(key, expected.get(key)));
            var destinationWrite = f.request(MigrationFixture.DESTINATION, new Command.Set(key, expected.get(key)));
            assertTrue((sourceWrite.status() == ResultStatus.OK ? 1 : 0)
                    + (destinationWrite.status() == ResultStatus.OK ? 1 : 0) <= 1);
            if (p.state() == JobState.COPYING || p.state() == JobState.SWITCHING) {
                assertEquals(ResultStatus.BUSY, sourceWrite.status());
                assertEquals(ResultStatus.MOVED, destinationWrite.status());
                assertEquals(ResultStatus.OK, f.request(MigrationFixture.SOURCE, new Command.Get(key)).status());
                assertEquals(ResultStatus.BUSY, f.request(MigrationFixture.SOURCE, new Command.Delete(key)).status());
            }
            runner.step(173);
        }
        assertEquals(JobState.DONE, p.state());
        assertEquals(expected, f.destination);
        assertTrue(f.source.data.isEmpty());
        assertEquals(1, f.coordinator.successfulCommits);
        assertEquals(1, f.coordinator.current().version().value());
        assertEquals(1, f.coordinator.current().get(MigrationFixture.MOVING).epoch().value());
        assertEquals(ResultStatus.MISS, f.request(MigrationFixture.DESTINATION, new Command.Get(deleted)).status());
        for (Command request : new Command[] {new Command.Get(key), new Command.Set(key, Bytes.empty()), new Command.Delete(key)}) {
            var moved = f.request(MigrationFixture.SOURCE, request);
            assertEquals(ResultStatus.MOVED, moved.status());
            assertEquals(MigrationFixture.DESTINATION, moved.redirect().orElseThrow().target());
            assertEquals(f.coordinator.current().version(), moved.routingVersion());
        }
        assertFalse(f.coordinator.commit(p.job(), p.expectedTable()));
        assertEquals(1, f.coordinator.successfulCommits);
        assertFalse(f.link.deliver(f.initialJob, p.expectedTable().get(MigrationFixture.MOVING).epoch(), f.link.lastBatch));
        assertEquals(expected, f.destination);
        assertEquals(ResultStatus.OK, f.request(MigrationFixture.DESTINATION, new Command.Set(key, Bytes.empty())).status());
    }

    @Test
    void abortAtEachPreCommitBoundaryRestoresDataAndRouting() {
        for (String boundary : new String[] {"begin", "partial", "ack-wait", "switching"}) {
            MigrationFixture f = boundary.equals("ack-wait")
                    ? new MigrationFixture(false, MigrationFixture.Fault.ACK_DELAY, 400) : populated(false);
            if (boundary.equals("ack-wait")) f.source.data.put(Bytes.utf8("a"), Bytes.utf8("value"));
            Map<Bytes, Bytes> expected = new LinkedHashMap<>(f.source.data);
            var p = f.protocol;
            var table = p.expectedTable();
            p.begin(f.initialJob);
            if (boundary.equals("partial")) p.step(1);
            if (boundary.equals("ack-wait")) p.step(100_000);
            if (boundary.equals("switching")) while (p.state() == JobState.COPYING) p.step(100_000);
            assertEquals(JobState.ABORTED, p.abort());
            assertEquals(JobState.ABORTED, p.abort());
            assertSame(table, f.coordinator.current());
            assertEquals(expected, f.source.data);
            assertTrue(f.destination.isEmpty());
            assertFalse(f.source.frozen(table.get(MigrationFixture.MOVING)));
            assertTrue(p.acceptsWrite());
        }
    }

    @Test
    void cleanupCanPartiallyFailAndRetryWithoutRollback() {
        MigrationFixture f = populated(false);
        Map<Bytes, Bytes> expected = new LinkedHashMap<>(f.source.data);
        f.source.cleanupFailures = 2;
        f.source.partialCleanup = true;
        f.protocol.begin(f.initialJob);
        TickRunner runner = new TickRunner(f, 1_000);
        while (f.protocol.state() != JobState.CLEANING_UP) runner.step(100_000);
        assertEquals(JobState.CLEANING_UP, f.protocol.abort());
        assertEquals(JobState.CLEANING_UP, runner.step(0));
        assertEquals(99, f.source.data.size());
        assertEquals(expected, f.destination);
        assertEquals(JobState.CLEANING_UP, f.protocol.abort());
        assertEquals(JobState.CLEANING_UP, runner.step(0));
        assertEquals(98, f.source.data.size());
        assertEquals(JobState.DONE, runner.step(0));
        assertEquals(JobState.DONE, f.protocol.abort());
        assertTrue(f.source.data.isEmpty());
        assertEquals(expected, f.destination);
        assertFalse(f.protocol.acceptsWrite());
    }

    @Test
    void freezeAndCleanupAffectOnlyMovingTablet() {
        MigrationFixture f = populated(true);
        Map<Bytes, Bytes> expected = f.movingData();
        Bytes moving = expected.keySet().iterator().next();
        Bytes other = f.source.data.keySet().stream()
                .filter(k -> !f.protocol.expectedTable().get(MigrationFixture.MOVING).range().contains(KeyHash.of(k))).findFirst().orElseThrow();
        f.protocol.begin(f.initialJob);
        assertEquals(ResultStatus.BUSY, f.request(MigrationFixture.SOURCE, new Command.Delete(moving)).status());
        assertEquals(ResultStatus.OK, f.request(MigrationFixture.SOURCE, new Command.Set(other, Bytes.utf8("other"))).status());
        finish(f, 1_000);
        assertEquals(expected, f.destination);
        assertEquals(Bytes.utf8("other"), f.source.data.get(other));
        assertEquals(f.protocol.expectedTable().get(MigrationFixture.OTHER), f.coordinator.current().get(MigrationFixture.OTHER));
    }
}
