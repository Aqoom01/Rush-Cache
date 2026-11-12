package com.openkache.failuredetector.product;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProductBenchmarkRunnerTest {
    private final ProductBenchmarkRunner runner = new ProductBenchmarkRunner();

    @Test
    void allProfilesDetectSingleCrashInEightNodeSchedule() {
        for (ProductAlgorithm algorithm : ProductAlgorithm.values()) {
            ProductRunResult result = runner.run(
                    algorithm, ProductScenario.CRASH, 1, 8, "native");
            assertTrue(result.detectionSuccess(), algorithm.id());
            assertNotNull(result.detectionLatencyNanos(), algorithm.id());
            assertEquals(1, result.detectedTargets(), algorithm.id());
            assertTrue(result.txBytes() > 0, algorithm.id());
        }
    }

    @Test
    void consecutiveCrashCoverageRequiresEveryTarget() {
        for (ProductAlgorithm algorithm : ProductAlgorithm.values()) {
            ProductRunResult result = runner.run(
                    algorithm, ProductScenario.CONSECUTIVE_3, 2, 8, "native");
            assertEquals(3, result.detectedTargets(), algorithm.id());
            assertTrue(result.detectionSuccess(), algorithm.id());
        }
    }

    @Test
    void cassandraLocalDownHasNoClusterConfirmationOrMembershipRemoval() {
        ProductRunResult result = runner.run(
                ProductAlgorithm.PHI_GOSSIP, ProductScenario.CRASH, 3, 8, "native");
        assertTrue(result.events().stream().noneMatch(event ->
                event.type() == BenchmarkEventType.CLUSTER_CONFIRMED
                        || event.type() == BenchmarkEventType.MEMBERSHIP_CHANGED));
    }

    @Test
    void delayedDeliveriesNeverCreateBackwardsDetectorEvents() {
        for (ProductAlgorithm algorithm : ProductAlgorithm.values()) {
            ProductRunResult result = runner.run(
                    algorithm, ProductScenario.DELAY_JITTER, 17, 8, "native");
            long previous = Long.MIN_VALUE;
            for (BenchmarkEvent event : result.events()) {
                assertTrue(event.atNanos() >= previous,
                        algorithm.id() + " emitted a backwards event timeline");
                previous = event.atNanos();
            }
        }
    }
}
