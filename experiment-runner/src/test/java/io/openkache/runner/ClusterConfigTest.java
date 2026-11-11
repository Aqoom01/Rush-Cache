package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClusterConfigTest {
    @Test
    void schedulerAndDrainAreOptInWithIndependentDefaults() {
        var values = new HashMap<>(Map.of("cluster.mode", "b1", "load.rate", "100"));
        var errors = new ArrayList<String>();
        var legacy = ClusterConfig.parse(values::get, errors, 4).orElseThrow();
        assertTrue(legacy.scheduler().isEmpty());
        assertTrue(legacy.drain().isEmpty());

        values.put("migration.scheduler", "fifo");
        values.put("drain.node", "3");
        var config = ClusterConfig.parse(values::get, errors, 4).orElseThrow();
        assertTrue(errors.isEmpty(), errors.toString());
        assertEquals(4, config.scheduler().orElseThrow().maxConcurrent());
        assertEquals(4, config.scheduler().orElseThrow().maxPerNode());
        assertEquals(new ClusterConfig.Drain(3, 10000), config.drain().orElseThrow());
    }

    @Test
    void rejectsInvalidSchedulerAndDrainSettings() {
        rejects(Map.of("migration.scheduler", "unknown"), 4, "migration.scheduler");
        rejects(Map.of("migration.scheduler", "fifo", "migration.scheduler.max.concurrent", "0"), 4, "max.concurrent");
        rejects(Map.of("migration.scheduler", "fifo", "migration.scheduler.max.per.node", "no"), 4, "max.per.node");
        rejects(Map.of("migration.scheduler", "slamig"), 4, "deadline.ms");
        rejects(Map.of("migration.scheduler", "slamig", "migration.scheduler.deadline.ms", "-1"), 4, "deadline.ms");
        rejects(Map.of("drain.at.ms", "1"), 4, "needs drain.node");
        rejects(Map.of("drain.node", "3"), 3, "drain.node");
        rejects(Map.of("drain.node", "0"), 1, "at least two targets");
        rejects(Map.of("drain.node", "0", "migration.tablet", "2"), 4, "explicit migration list");
        rejects(Map.of("migration.scheduler", "fifo", "balance.split", "none"), 4, "balance.split");
        rejects(Map.of("drain.node", "0", "balance.split", "none"), 4, "balance.split");
    }

    private static void rejects(Map<String, String> overrides, int targets, String expected) {
        var values = new HashMap<>(Map.of("cluster.mode", "b1", "load.rate", "100"));
        values.putAll(overrides);
        var errors = new ArrayList<String>();
        ClusterConfig.parse(values::get, errors, targets);
        assertTrue(errors.stream().anyMatch(e -> e.contains(expected)), errors.toString());
    }
}
