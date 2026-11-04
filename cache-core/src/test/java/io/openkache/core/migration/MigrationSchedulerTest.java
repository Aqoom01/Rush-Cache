package io.openkache.core.migration;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MigrationSchedulerTest {
    static Change change(long tablet, long source, long destination) { return new Change(new TabletId(tablet), new NodeId(source), new NodeId(destination)); }
    static MigrationScheduler.Settings settings(int jobs, int endpoints, long cluster, long node, long timeout) {
        return new MigrationScheduler.Settings(jobs, endpoints, cluster, node, timeout);
    }
    static final class Driver implements MigrationScheduler.Driver {
        final long target;
        long sent;
        boolean stopped;
        Driver(long target) { this.target = target; }
        @Override public void begin() {}
        @Override public void step(long budget) { sent += Math.min(target - sent, budget); }
        @Override public long sentBytes() { return sent; }
        @Override public boolean done() { return sent >= target; }
        @Override public boolean failed() { return stopped; }
        @Override public String state() { return stopped ? "FAILED" : done() ? "DONE" : "COPYING"; }
        @Override public void stop() { stopped = true; }
    }

    @Test void admissionSkipsBlockedHeadAndRespectsBothEndpointRoles() {
        var s = new MigrationScheduler(List.of(change(0, 1, 2), change(1, 1, 3), change(2, 4, 5), change(3, 2, 6)),
                settings(4, 1, 2, 1, 100), ignored -> new Driver(4));
        var tick = s.step();
        assertEquals(List.of(0, 2), tick.admitted());
        assertEquals(MigrationScheduler.Status.QUEUED, tick.jobs().get(1).status());
        assertEquals(MigrationScheduler.Status.QUEUED, tick.jobs().get(3).status());
        assertEquals(2, tick.consumed().values().stream().mapToLong(Long::longValue).sum());
    }

    @Test void rotatingSmallBudgetDoesNotStarveSharedEndpoint() {
        var s = new MigrationScheduler(List.of(change(0, 1, 2), change(1, 1, 3), change(2, 1, 4), change(3, 1, 5)),
                settings(4, 4, 1, 1, 100), ignored -> new Driver(100));
        for (int i = 0; i < 4; i++) {
            var tick = s.step();
            assertEquals(1, tick.consumed().values().stream().mapToLong(Long::longValue).sum());
            assertTrue(tick.nodeConsumed().values().stream().allMatch(n -> n <= 1));
        }
        assertTrue(s.jobs().stream().allMatch(j -> j.sentBytes() == 1));
    }

    @Test void unusedGrantsAreNotSavedOrOverspent() {
        var s = new MigrationScheduler(List.of(change(0, 1, 2), change(1, 3, 4)), settings(2, 2, 8, 3, 100), ignored -> new Driver(1));
        var tick = s.step();
        assertEquals(Map.of(0, 3L, 1, 3L), tick.allocated());
        assertEquals(Map.of(0, 1L, 1, 1L), tick.consumed());
        assertTrue(s.finished());
        assertEquals(2, s.jobs().size());
    }

    @Test void timeoutStopsDriverAndReleasesEndpointsForNextJob() {
        var s = new MigrationScheduler(List.of(change(0, 1, 2), change(1, 1, 3)), settings(1, 1, 1, 1, 2), ignored -> new Driver(100));
        s.step(); s.step();
        var expired = s.step();
        assertEquals(MigrationScheduler.Status.TIMED_OUT, expired.jobs().getFirst().status());
        assertEquals(0L, expired.consumed().get(0));
        assertEquals(List.of(1), s.step().admitted());
    }

    @Test void refusesDuplicateTabletAndInvalidSettingsBeforeExecution() {
        assertThrows(IllegalArgumentException.class, () -> new MigrationScheduler(List.of(change(0, 1, 2), change(0, 1, 3)),
                settings(1, 1, 1, 1, 1), ignored -> new Driver(1)));
        assertThrows(IllegalArgumentException.class, () -> settings(0, 1, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> settings(1, 1, 0, 1, 1));
        var empty = new MigrationScheduler(List.of(), settings(1, 1, 1, 1, 1), ignored -> new Driver(1));
        assertTrue(empty.finished());
    }
}
