package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.phi.GossipHeartbeat;
import com.openkache.failuredetector.phi.PhiAccrualFailureDetector;
import com.openkache.failuredetector.swim.SwimLifeguardDetector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Replays one ordered observation trace into both decision engines. Network
 * message generation is intentionally excluded so this isolates decision
 * behavior from protocol overhead.
 */
public final class TraceReplayRunner {
    public ReplayResult replay(List<Observation> observations, int clusterSize) {
        SwimLifeguardDetector swim = new SwimLifeguardDetector("replay-observer", clusterSize);
        PhiAccrualFailureDetector phi = new PhiAccrualFailureDetector("replay-observer");
        Map<String, List<DetectorEvent>> events = new LinkedHashMap<>();
        events.put(swim.algorithm(), new ArrayList<>());
        events.put(phi.algorithm(), new ArrayList<>());
        long version = 0;

        long previousAt = 0;
        for (Observation observation : observations.stream()
                .sorted(java.util.Comparator.comparingLong(Observation::atNanos))
                .toList()) {
            if (observation.localProcessingDelayNanos() > 0) {
                swim.observeLocalDelay(observation.localProcessingDelayNanos());
            } else if (observation.atNanos() > previousAt) {
                swim.observeLocalDelay(observation.atNanos() - previousAt);
            }

            swim.recordProbe(
                    observation.atNanos(),
                    observation.target(),
                    observation.probeSucceeded(),
                    observation.incarnation()
            );
            swim.tick(observation.atNanos());
            events.get(swim.algorithm()).addAll(swim.drainEvents());

            if (observation.heartbeatReceived()) {
                version++;
                phi.recordHeartbeat(
                        new GossipHeartbeat(
                                observation.target(),
                                observation.generation(),
                                version,
                                observation.atNanos()
                        ),
                        observation.channelReachable()
                );
            }
            phi.tick(observation.atNanos());
            events.get(phi.algorithm()).addAll(phi.drainEvents());
            previousAt = observation.atNanos();
        }
        return new ReplayResult(events);
    }

    public record Observation(
            long atNanos,
            String target,
            boolean groundTruthAlive,
            boolean probeSucceeded,
            boolean heartbeatReceived,
            boolean channelReachable,
            long localProcessingDelayNanos,
            long generation,
            long incarnation
    ) {
        public Observation {
            if (atNanos < 0 || localProcessingDelayNanos < 0) {
                throw new IllegalArgumentException("timestamps and delays cannot be negative");
            }
        }
    }

    public record ReplayResult(Map<String, List<DetectorEvent>> eventsByAlgorithm) {
        public ReplayResult {
            Map<String, List<DetectorEvent>> immutable = new LinkedHashMap<>();
            eventsByAlgorithm.forEach((key, value) -> immutable.put(key, List.copyOf(value)));
            eventsByAlgorithm = Map.copyOf(immutable);
        }
    }
}
