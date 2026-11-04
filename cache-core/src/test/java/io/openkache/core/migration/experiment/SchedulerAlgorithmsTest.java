package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchedulerAlgorithmsTest {
    private static Change c(int id, int from, int to) { return new Change(new TabletId(id), new NodeId(from), new NodeId(to)); }
    private static List<SchedulerAlgorithms.Task> tasks(List<Change> changes) {
        var result = new ArrayList<SchedulerAlgorithms.Task>();
        for (int i = 0; i < changes.size(); i++) result.add(new SchedulerAlgorithms.Task(i, changes.get(i), 10000, 0, 10000));
        return result;
    }
    @Test void gwinAvoidsTheFifoBlockerAndPicksCompatibleEdges() {
        var tasks = tasks(List.of(c(0, 1, 3), c(1, 1, 4), c(2, 2, 3)));
        var order = SchedulerAlgorithms.graphPlan(tasks, false, 1000);
        assertEquals(List.of(1, 2, 0), order);
        assertFalse(SchedulerAlgorithms.conflict(tasks.get(order.get(0)).change(), tasks.get(order.get(1)).change()));
    }
    @Test void formalConflictScenarioContainsCrossedDependenciesInsteadOfSortedIndependentBatches() {
        var condition = SchedulerComparisonExperiment.condition("conflict", "full");
        var plan = SchedulerComparisonExperiment.planFor(condition, "fifo");
        assertTrue(SchedulerAlgorithms.conflict(plan.get(0), plan.get(1)));
        assertTrue(SchedulerAlgorithms.conflict(plan.get(0), plan.get(2)));
        assertFalse(SchedulerAlgorithms.conflict(plan.get(1), plan.get(2)));
        var definitions = tasks(plan);
        assertNotEquals(definitions.stream().map(SchedulerAlgorithms.Task::id).toList(), SchedulerAlgorithms.graphPlan(definitions, false, condition.nodeBudget()));
    }
    @Test void cliqueEnumerationAndIndependentSetsAreMaximalAndCompleteOnSmallGraphs() {
        // 모든 4정점 무방향 graph를 완전 탐색 oracle과 비교한다.
        for (int edges = 0; edges < 64; edges++) {
            long[] g = new long[4]; int e = 0;
            for (int i = 0; i < 4; i++) for (int j = 0; j < i; j++, e++) if ((edges & (1 << e)) != 0) { g[i] |= 1L << j; g[j] |= 1L << i; }
            for (boolean complement : List.of(false, true)) {
                var actual = new java.util.HashSet<>(SchedulerAlgorithms.cliques(g, complement)); var expected = new java.util.HashSet<Long>();
                for (long subset = 1; subset < 16; subset++) {
                    boolean valid = true, maximal = true;
                    for (int i = 0; i < 4; i++) if ((subset & (1L << i)) != 0) for (int j = 0; j < i; j++) if ((subset & (1L << j)) != 0
                            && (((g[i] & (1L << j)) != 0) == complement)) valid = false;
                    if (!valid) continue;
                    for (int v = 0; v < 4; v++) if ((subset & (1L << v)) == 0) {
                        long neighbors = complement ? ~g[v] & 15 & ~(1L << v) : g[v];
                        if ((neighbors & subset) == subset) maximal = false;
                    }
                    if (maximal) expected.add(subset);
                }
                assertEquals(expected, actual);
            }
        }
    }
    @Test void fptaRespectsAllResourceCapacitiesAndUsesIndependentPaths() {
        var settings = new MigrationScheduler.Settings(4, 4, 4000, 1000, 1000);
        var independent = tasks(List.of(c(0, 1, 11), c(1, 2, 12), c(2, 3, 13), c(3, 4, 14)));
        var a = SchedulerAlgorithms.fpta(independent, settings, .2);
        assertTrue(a.values().stream().allMatch(n -> n > 600 && n <= 1000));
        assertTrue(a.values().stream().mapToLong(Long::longValue).sum() <= 4000);
        var shared = SchedulerAlgorithms.fpta(tasks(List.of(c(0, 1, 11), c(1, 1, 12), c(2, 1, 13))), settings, .2);
        long total = shared.values().stream().mapToLong(Long::longValue).sum();
        assertTrue(total > 600 && total <= 1000);
        assertTrue(shared.values().stream().allMatch(n -> n > 0));
    }
    @Test void vmbuddiesModelIncreasesCostWithDirtyAndDecreasesWithBandwidth() {
        assertEquals(10, SchedulerAlgorithms.copyTime(10000, 0, 1000));
        assertTrue(SchedulerAlgorithms.copyTime(10000, 500, 1000) > 10);
        assertTrue(SchedulerAlgorithms.copyTime(10000, 500, 2000) < SchedulerAlgorithms.copyTime(10000, 500, 1000));
    }
    @Test void camigChoosesAllowedDestinationsAndReducesSharedDestinationPressure() {
        var candidates = List.of(List.of(c(0, 1, 10), c(0, 1, 11)), List.of(c(1, 2, 10), c(1, 2, 12)),
                List.of(c(2, 3, 10), c(2, 3, 13)));
        var plan = SchedulerAlgorithms.camig(candidates, List.of(1000L, 1000L, 1000L), 1000);
        for (int i = 0; i < plan.size(); i++) assertTrue(candidates.get(i).contains(plan.get(i)));
        assertEquals(3, plan.stream().map(Change::to).distinct().count());
    }
    @Test void everyMethodExecutesTheRealPrecopyProtocolAndPreservesData() {
        var condition = SchedulerComparisonExperiment.condition("conflict", "quick");
        for (String method : SchedulerAlgorithms.METHODS) {
            var result = SchedulerComparisonExperiment.run(condition, method, "pre-copy", 1);
            var metrics = BaselineJson.object(result.get("metrics"));
            assertEquals(12, ((Number) metrics.get("exact_comparisons")).intValue(), method);
            assertEquals(0, ((Number) metrics.get("invariant_violations")).intValue(), method);
        }
    }
    @Test void camigAndBaselineHaveTheSameCandidateInputButDifferentPlans() {
        var condition = SchedulerComparisonExperiment.condition("candidate-plan", "quick");
        var baseline = SchedulerComparisonExperiment.run(condition, "candidate-first", "post-copy", 1);
        var camig = SchedulerComparisonExperiment.run(condition, "camig-selector", "post-copy", 1);
        assertEquals(baseline.get("inputHash"), camig.get("inputHash"));
        assertEquals(baseline.get("workloadHash"), camig.get("workloadHash"));
        assertNotEquals(baseline.get("planHash"), camig.get("planHash"));
        assertEquals(12, ((Number) BaselineJson.object(camig.get("metrics")).get("exact_comparisons")).intValue());
    }
    @Test void schedulerRejectsMaliciousAllocationAndUnknownAdmissionBeforeTransfer() {
        var settings = new MigrationScheduler.Settings(4, 4, 1000, 1000, 100);
        MigrationScheduler.Driver driver = new MigrationScheduler.Driver() {
            @Override public void begin() {}
            @Override public void step(long budget) { fail("invalid budget reached driver"); }
            @Override public long sentBytes() { return 0; }
            @Override public boolean done() { return false; }
            @Override public boolean failed() { return false; }
            @Override public String state() { return "COPYING"; }
            @Override public void stop() {}
        };
        var policy = new MigrationScheduler.Policy() {
            @Override public Map<Integer, Long> allocate(long tick, List<MigrationScheduler.JobView> active, MigrationScheduler.Settings s) { return Map.of(0, 1001L); }
        };
        var scheduler = new MigrationScheduler(List.of(c(0, 1, 2)), settings, ignored -> driver, policy);
        assertThrows(IllegalArgumentException.class, scheduler::step);
        var invalidOrder = new MigrationScheduler.Policy() {
            @Override public List<Integer> order(long tick, List<MigrationScheduler.JobView> jobs, MigrationScheduler.Settings s) { return List.of(99); }
        };
        var bad = new MigrationScheduler(List.of(c(0, 1, 2)), settings, ignored -> driver, invalidOrder);
        assertThrows(IllegalArgumentException.class, bad::step);
    }
}
