package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class VersionedMembershipViewTest {
    @Test
    void staleAndDuplicateRemovalCannotChangeViewOrResurrectMember() {
        VersionedMembershipView view = new VersionedMembershipView(
                List.of("node-0", "node-1", "node-2"));

        assertEquals(VersionedMembershipView.ApplyOutcome.MEMBER_REMOVED,
                view.applyRemoval("node-1", stamp(2, "node-0")));
        assertEquals(VersionedMembershipView.ApplyOutcome.STALE_OR_DUPLICATE,
                view.applyRemoval("node-1", stamp(1, "node-2")));
        assertEquals(VersionedMembershipView.ApplyOutcome.STALE_OR_DUPLICATE,
                view.applyRemoval("node-1", stamp(2, "node-0")));

        assertFalse(view.contains("node-1"));
        assertEquals(List.of("node-0", "node-2"), view.members());
        assertEquals(2, view.viewVersion());
        assertEquals(stamp(2, "node-0"), view.removalStamp("node-1"));
    }

    @Test
    void concurrentNewerOriginAdvancesTombstoneWithoutAdvancingViewVersion() {
        VersionedMembershipView view = new VersionedMembershipView(
                List.of("node-0", "node-1", "node-2"));

        view.applyRemoval("node-1", stamp(1, "node-0"));
        assertEquals(VersionedMembershipView.ApplyOutcome.TOMBSTONE_ADVANCED,
                view.applyRemoval("node-1", stamp(1, "node-2")));

        assertEquals(2, view.viewVersion());
        assertEquals(stamp(1, "node-2"), view.removalStamp("node-1"));
    }

    private static VersionedMembershipView.VersionStamp stamp(long time, String origin) {
        return new VersionedMembershipView.VersionStamp(time, origin);
    }
}
