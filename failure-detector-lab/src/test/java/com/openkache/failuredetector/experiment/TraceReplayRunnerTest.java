package com.openkache.failuredetector.experiment;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TraceReplayRunnerTest {
    @Test
    void feedsExactlyTheSameObservationTimesToBothDetectors() {
        List<TraceReplayRunner.Observation> observations = new ArrayList<>();
        for (int second = 1; second <= 10; second++) {
            observations.add(new TraceReplayRunner.Observation(
                    Duration.ofSeconds(second).toNanos(),
                    "target",
                    true,
                    true,
                    true,
                    true,
                    0,
                    1,
                    1
            ));
        }

        TraceReplayRunner.ReplayResult result = new TraceReplayRunner().replay(observations, 5);

        assertEquals(2, result.eventsByAlgorithm().size());
        assertEquals(0, result.eventsByAlgorithm().get("swim-lifeguard").size());
        assertEquals(0, result.eventsByAlgorithm().get("phi-gossip").size());
    }
}
