package io.openkache.runner.benchmark.measurement;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.measurement.MeasurementFixture.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class LatencyCollectorTest {
    @Test void populationsSeparatePhaseOutcomeCensoringAndAllThreeTimeBoundaries() {
        var collector = collector();
        for (var phase : TracePhase.values()) { for (var category : ClientOutcome.Category.values()) {
            var request = generator().at(phase, category.ordinal());
            collector.outcome(request, 100, outcome(request, category, 110, 150));
        } }
        var s = collector.freeze(); assertEquals(27, s.series().size());
        for (var series : s.series()) {
            assertEquals(series.population().outcome() == ClientOutcome.Category.NO_RESPONSE, series.population().censored());
            assertEquals(switch(series.population().kind()) { case SERVICE -> 40L; case SCHEDULED -> 50L; case DISPATCH_LAG -> 10L; }, series.histogram().minimumNanos());
        }
    }
    @Test void simultaneousCallbacksAreCountedWithoutSampling() throws Exception {
        var collector = collector();
        try (var workers = Executors.newFixedThreadPool(4)) {
            List<Future<?>> jobs = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) { final int slot = thread;
                jobs.add(workers.submit(() -> { for (int i = 0; i < 1000; i++) {
                    var request = generator().at(TracePhase.MEASURE, slot * 1000L + i);
                    collector.outcome(request, 100, outcome(request, ClientOutcome.Category.SUCCESS, 110, 150));
                } }));
            }
            for (var job : jobs) { job.get(); }
        }
        assertEquals(4000, collector.freeze().counts().values().stream().mapToLong(LatencyCollector.Tally::observed).sum());
        assertEquals(12_000, collector.freeze().series().stream().mapToLong(s -> s.histogram().count()).sum());
    }
    @Test void identityAndNegativeClockAreFailuresRatherThanZeroLatency() {
        var c = collector(); var request = generator().at(TracePhase.MEASURE, 0);
        assertThrows(IllegalArgumentException.class, () -> c.outcome(request, 200, outcome(request, ClientOutcome.Category.SUCCESS, 110, 150)));
        assertNotNull(c.freeze().failure()); assertTrue(c.freeze().series().isEmpty());
        var wrong = new LatencyCollector("other", "slot", "clock", 1000);
        assertThrows(IllegalArgumentException.class, () -> wrong.outcome(request, 100, outcome(request, ClientOutcome.Category.SUCCESS, 110, 150)));
    }
    @Test void frozenSnapshotIsImmutableAndRejectsLateMutation() {
        var c = collector(); var request = generator().at(TracePhase.MEASURE, 0);
        c.outcome(request, 100, outcome(request, ClientOutcome.Category.SUCCESS, 110, 150));
        var s = c.freeze(); assertSame(s, c.freeze());
        assertThrows(IllegalStateException.class, () -> c.outcome(request, 100, outcome(request, ClientOutcome.Category.SUCCESS, 110, 150)));
        assertThrows(UnsupportedOperationException.class, () -> s.series().clear());
    }
    @Test void optionalClosedLoopObserverReceivesGeneratedDueAndCorrectCohorts() {
        var c = collector(); var result = new ClosedLoopRunner(generator(), closedPlan(), new Client(), probe(), c).run();
        assertTrue(result.passed(), result.toString());
        var s = c.freeze(); assertEquals(17, s.counts().values().stream().mapToLong(LatencyCollector.Tally::success).sum());
        assertEquals(10, s.counts().entrySet().stream().filter(e -> e.getKey().phase() == TracePhase.MEASURE).mapToLong(e -> e.getValue().success()).sum());
        assertTrue(s.series().stream().allMatch(h -> h.histogram().minimumNanos() >= 0));
    }
    @Test void closedLoopObserverFailureCannotPassTheRun() {
        var result = new ClosedLoopRunner(generator(), closedPlan(), new Client(), probe(), (request, due, outcome) -> {
            throw new IllegalStateException("collector failure");
        }).run();
        assertFalse(result.passed()); assertEquals("OUTCOME_HANDLER_FAILED", result.failure());
    }
    @Test void retriesHaveOneLatencySampleAndAnIndependentAttemptCount() {
        var c = collector(); var request = generator().at(TracePhase.MEASURE, 0);
        var base = outcome(request, ClientOutcome.Category.SUCCESS, 110, 150);
        var retried = new ClientOutcome(base.id(), base.operation(), base.category(), base.reason(), base.result(),
                110, 150, 150, base.wireClientId(), base.wireOperationId(), 2,
                List.of(new ClientOutcome.Attempt(1, 110, 120L, null, "retry"),
                        new ClientOutcome.Attempt(2, 130, 150L, base.result().status(), null)));
        c.outcome(request, 100, retried);
        var tally = c.freeze().counts().values().iterator().next();
        assertEquals(1, tally.success()); assertEquals(2, tally.attempts()); assertEquals(1, tally.retried());
        assertTrue(c.freeze().series().stream().allMatch(s -> s.histogram().count() == 1));
    }
}
