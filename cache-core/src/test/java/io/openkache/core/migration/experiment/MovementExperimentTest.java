package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.MigrationPublication;
import org.junit.jupiter.api.Test;

class MovementExperimentTest {
    @Test void guardTabletSharesTheMovingSourceNode() {
        var f = new MultiMigrationFixture(4, 4, 1, "shared-source", "stop-and-copy", 1024, 256);
        var source = f.changes.getFirst().from();
        assertEquals(source, f.routing.get().get(f.guard).owner());
        assertEquals(5, f.nodes.get(source).tablets.size());
    }
    private static MovementExperiment.Case condition(int jobs, int concurrency, int endpointLimit, String endpoints, double rho, String variant) {
        return new MovementExperiment.Case("test", "M5", jobs, concurrency, endpointLimit, endpoints, 64, 4096, 1024, 1024, 1024, 256, rho, variant);
    }
    private static double metric(MovementExperiment.Run run, String name) {
        return ((Number) BaselineJson.object(run.metrics().get(name)).get("value")).doubleValue();
    }

    @Test void allThreeProtocolsCommitAgainstSharedTableAndPreserveColocatedTablets() {
        for (String variant : MovementExperiment.VARIANTS) {
            var run = MovementExperiment.run(condition(4, 4, 4, "shared-source", .5, variant), 1);
            assertEquals(4, metric(run, "exact_comparisons"), variant);
            assertEquals(0, metric(run, "invariant_violations"), variant);
            assertEquals(1, metric(run, "completion_ratio"), variant);
            assertTrue(run.jobs().stream().allMatch(j -> ((Number) j.get("startTick")).longValue() == 0));
        }
    }

    @Test void destinationSharingUsesSeparateStagingAndEnforcesNodeBudget() {
        for (String variant : MovementExperiment.VARIANTS) {
            var run = MovementExperiment.run(condition(4, 4, 4, "shared-destination", .9, variant), 2);
            assertEquals(4, metric(run, "exact_comparisons"));
            assertTrue(metric(run, "aggregate_bytes_per_tick") <= 1024);
        }
    }

    @Test void independentEndpointsActuallyRunConcurrently() {
        var one = MovementExperiment.run(condition(1, 4, 1, "independent", .1, "stop-and-copy"), 1);
        var four = MovementExperiment.run(condition(4, 4, 1, "independent", .1, "stop-and-copy"), 1);
        assertTrue(metric(four, "aggregate_bytes_per_tick") > 3 * metric(one, "aggregate_bytes_per_tick"));
    }

    @Test void endpointLimitChangesAdmissionAndQueueing() {
        var independent = MovementExperiment.run(condition(4, 4, 1, "independent", .1, "stop-and-copy"), 1);
        var shared = MovementExperiment.run(condition(4, 4, 1, "shared-source", .1, "stop-and-copy"), 1);
        assertTrue(metric(shared, "makespan_ticks") > 2 * metric(independent, "makespan_ticks"));
        assertTrue(metric(shared, "wait_p99_ticks") > metric(independent, "wait_p99_ticks"));
    }

    @Test void highWriteLoadCompletesAndSameInputIsReproducible() {
        var c = condition(4, 4, 4, "shared-source", 1.2, "pre-copy");
        var first = MovementExperiment.run(c, 3);
        assertEquals(first, MovementExperiment.run(c, 3));
        assertTrue(first.jobs().stream().allMatch(j -> !j.get("stopReason").equals("")));
        assertTrue(metric(first, "writes_attempted") > 0);
    }

    @Test void workloadSpecificationIsIndependentOfSchedulerAndVariant() {
        String hash = MovementExperiment.workloadHash(condition(4, 1, 1, "independent", .5, "stop-and-copy"), 1);
        assertEquals(hash, MovementExperiment.workloadHash(condition(4, 4, 4, "shared-source", .5, "post-copy"), 1));
        assertNotEquals(hash, MovementExperiment.workloadHash(condition(4, 4, 4, "shared-source", .9, "post-copy"), 1));
        assertEquals(42, MovementExperiment.cases("full").size());
    }

    @Test void publicationAllowsOtherTabletButRejectsStaleOrExtraChanges() {
        var f = new MultiMigrationFixture(3, 4, 1, "independent", "stop-and-copy", 1024, 256);
        var started = f.routing.get();
        var first = f.changes.getFirst();
        var second = f.changes.get(1);
        var job = MigrationJob.pending(first, "stop-and-copy");
        var before = started.apply(second);
        assertDoesNotThrow(() -> MigrationPublication.verify(job, started, before, before.apply(first)));
        assertThrows(IllegalArgumentException.class, () -> MigrationPublication.verify(job, started, before, started.apply(first)));
        assertThrows(IllegalArgumentException.class, () -> MigrationPublication.verify(job, started, started.apply(first), started.apply(first)));
        assertThrows(IllegalArgumentException.class, () -> MigrationPublication.verify(job, started, started, started.apply(first).apply(second)));
    }
}
