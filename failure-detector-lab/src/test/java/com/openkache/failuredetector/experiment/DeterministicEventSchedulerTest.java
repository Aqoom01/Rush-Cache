package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeterministicEventSchedulerTest {
    @Test
    void sameTimestampUsesDocumentedPhaseThenStableInsertionOrder() {
        DeterministicEventScheduler scheduler = new DeterministicEventScheduler();
        List<String> order = new ArrayList<>();

        scheduler.schedule(10, DeterministicEventScheduler.Phase.TIMEOUT, () -> order.add("timeout"));
        scheduler.schedule(10, DeterministicEventScheduler.Phase.DELIVERY, () -> order.add("delivery-1"));
        scheduler.schedule(10, DeterministicEventScheduler.Phase.FAULT, () -> order.add("fault"));
        scheduler.schedule(10, DeterministicEventScheduler.Phase.DELIVERY, () -> order.add("delivery-2"));
        scheduler.runUntil(10);

        assertEquals(List.of("fault", "delivery-1", "delivery-2", "timeout"), order);
    }
}
