package com.openkache.failuredetector.deadline;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RingMonitorAssignmentTest {
    @Test
    void deterministicRingWrapsAroundForThreeFiveAndEightNodes() {
        for (int size : List.of(3, 5, 8)) {
            List<String> members = java.util.stream.IntStream.range(0, size)
                    .mapToObj(index -> "node-" + index)
                    .toList();
            RingMonitorAssignment ring = new RingMonitorAssignment(members, 1);

            assertEquals(List.of("node-1"), ring.monitoredBy("node-0"));
            assertEquals(List.of("node-0"), ring.monitoredBy("node-" + (size - 1)));
            assertEquals(size, ring.assignments().size());
        }
    }

    @Test
    void viewUpdateReassignsSuccessorAfterNodeRemovalAndRestart() {
        RingMonitorAssignment ring = new RingMonitorAssignment(
                List.of("node-0", "node-1", "node-2", "node-3"), 1);
        ring.updateView(List.of("node-0", "node-2", "node-3"));
        assertEquals(List.of("node-2"), ring.monitoredBy("node-0"));

        ring.updateView(List.of("node-3", "node-1", "node-0", "node-2"));
        assertEquals(List.of("node-1"), ring.monitoredBy("node-0"));
    }

    @Test
    void multipleNeighborsRemainUniqueAcrossWrapAround() {
        RingMonitorAssignment ring = new RingMonitorAssignment(
                List.of("node-0", "node-1", "node-2"), 5);

        assertEquals(List.of("node-1", "node-2"), ring.monitoredBy("node-0"));
    }
}
