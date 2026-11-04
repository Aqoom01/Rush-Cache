package com.openkache.failuredetector.health;

import com.openkache.failuredetector.core.NodeStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HealthMonitorTest {
    @Test
    void consecutiveFailuresDegradeAndConsecutiveSuccessesRecover() {
        HealthMonitor monitor = new HealthMonitor(Duration.ofMillis(100), 3, 2);

        assertTrue(monitor.record(1, "node-1", HealthMonitor.Component.DISK,
                false, 200_000_000, "slow").isEmpty());
        assertTrue(monitor.record(2, "node-1", HealthMonitor.Component.DISK,
                false, 200_000_000, "slow").isEmpty());
        assertTrue(monitor.record(3, "node-1", HealthMonitor.Component.DISK,
                false, 200_000_000, "slow").isPresent());
        assertEquals(NodeStatus.HEALTH_DEGRADED,
                monitor.status("node-1", HealthMonitor.Component.DISK));

        monitor.record(4, "node-1", HealthMonitor.Component.DISK, true, 10, "ok");
        assertTrue(monitor.record(5, "node-1", HealthMonitor.Component.DISK,
                true, 10, "ok").isPresent());
        assertEquals(NodeStatus.ALIVE,
                monitor.status("node-1", HealthMonitor.Component.DISK));
    }
}
