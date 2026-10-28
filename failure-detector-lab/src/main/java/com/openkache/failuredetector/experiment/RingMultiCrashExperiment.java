package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.deadline.FixedDeadlineConfig;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.deadline.RingMonitorAssignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deterministic edge-case experiment for simultaneous crashes in a static Ring-N view.
 * This isolates observation coverage; membership reconfiguration is intentionally absent.
 */
public final class RingMultiCrashExperiment {
    public static final Duration DEFAULT_FAULT_START = Duration.ofSeconds(20);
    public static final Duration DEFAULT_DURATION = Duration.ofSeconds(40);

    private final FixedDeadlineConfig detectorConfig;
    private final Duration faultStart;
    private final Duration duration;

    public RingMultiCrashExperiment() {
        this(FixedDeadlineConfig.ring(), DEFAULT_FAULT_START, DEFAULT_DURATION);
    }

    public RingMultiCrashExperiment(
            FixedDeadlineConfig detectorConfig,
            Duration faultStart,
            Duration duration
    ) {
        if (detectorConfig == null
                || detectorConfig.peerSelectionMode()
                != FixedDeadlineConfig.PeerSelectionMode.DETERMINISTIC_RING) {
            throw new IllegalArgumentException("Ring experiment requires deterministic Ring config");
        }
        if (faultStart == null || duration == null
                || faultStart.isNegative() || duration.compareTo(faultStart) <= 0) {
            throw new IllegalArgumentException("faultStart must be non-negative and before duration");
        }
        this.detectorConfig = detectorConfig;
        this.faultStart = faultStart;
        this.duration = duration;
    }

    public Result runAdjacentCrash(int nodeCount, int neighborCount, int firstCrashIndex) {
        return runConsecutiveCrash(nodeCount, neighborCount, firstCrashIndex, 2);
    }

    public Result runConsecutiveCrash(
            int nodeCount,
            int neighborCount,
            int firstCrashIndex,
            int crashCount
    ) {
        List<String> members = members(nodeCount);
        if (firstCrashIndex < 0 || firstCrashIndex >= nodeCount) {
            throw new IllegalArgumentException("firstCrashIndex is outside the Ring");
        }
        if (crashCount < 2 || crashCount > nodeCount) {
            throw new IllegalArgumentException("crashCount must be between 2 and nodeCount");
        }
        return run(
                members,
                neighborCount,
                java.util.stream.IntStream.range(0, crashCount)
                        .mapToObj(offset -> members.get((firstCrashIndex + offset) % nodeCount))
                        .toList()
        );
    }

    public Result run(List<String> members, int neighborCount, List<String> crashedTargets) {
        if (members == null || members.size() < 3) {
            throw new IllegalArgumentException("multi-crash Ring experiment needs at least 3 nodes");
        }
        Set<String> crashed = new LinkedHashSet<>(crashedTargets);
        if (crashed.size() != crashedTargets.size() || crashed.size() < 2
                || !members.containsAll(crashed)) {
            throw new IllegalArgumentException("crashed targets must be distinct Ring members");
        }

        RingMonitorAssignment assignment = new RingMonitorAssignment(members, neighborCount);
        Map<String, FixedDeadlineFailureDetector> detectors = new LinkedHashMap<>();
        for (String observer : assignment.members()) {
            FixedDeadlineFailureDetector detector = new FixedDeadlineFailureDetector(
                    observer, detectorConfig);
            for (String target : assignment.monitoredBy(observer)) {
                detector.registerTarget(0, target, 1);
            }
            detectors.put(observer, detector);
        }

        List<DetectorEvent> events = new ArrayList<>();
        long messages = 0;
        long intervalNanos = detectorConfig.heartbeatInterval().toNanos();
        long faultAtNanos = faultStart.toNanos();
        long durationNanos = duration.toNanos();
        for (long nowNanos = 0; nowNanos <= durationNanos; nowNanos += intervalNanos) {
            boolean faultActive = nowNanos >= faultAtNanos;
            for (Map.Entry<String, List<String>> edge : assignment.assignments().entrySet()) {
                String observer = edge.getKey();
                boolean observerAlive = !faultActive || !crashed.contains(observer);
                FixedDeadlineFailureDetector detector = detectors.get(observer);
                for (String target : edge.getValue()) {
                    boolean targetAlive = !faultActive || !crashed.contains(target);
                    if (targetAlive) {
                        messages++;
                        if (observerAlive) {
                            detector.recordHeartbeat(nowNanos, target, 1);
                        }
                    }
                }
                if (observerAlive) {
                    detector.tick(nowNanos);
                    events.addAll(detector.drainEvents());
                }
            }
        }

        List<TargetOutcome> outcomes = crashed.stream()
                .map(target -> outcome(target, assignment, crashed, events, faultAtNanos))
                .toList();
        return new Result(
                assignment.members().size(),
                neighborCount,
                List.copyOf(crashed),
                faultAtNanos,
                durationNanos,
                messages,
                messages * detectorConfig.payloadBytes(),
                outcomes,
                events
        );
    }

    private static TargetOutcome outcome(
            String target,
            RingMonitorAssignment assignment,
            Set<String> crashed,
            List<DetectorEvent> events,
            long faultAtNanos
    ) {
        List<String> assignedObservers = assignment.assignments().entrySet().stream()
                .filter(entry -> entry.getValue().contains(target))
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
        List<String> liveObservers = assignedObservers.stream()
                .filter(observer -> !crashed.contains(observer))
                .toList();
        Long suspectAt = firstTransitionAt(events, target, NodeStatus.SUSPECT);
        Long deadAt = firstTransitionAt(events, target, NodeStatus.DEAD);
        return new TargetOutcome(
                target,
                assignedObservers,
                liveObservers,
                suspectAt == null ? null : suspectAt - faultAtNanos,
                deadAt == null ? null : deadAt - faultAtNanos
        );
    }

    private static Long firstTransitionAt(
            List<DetectorEvent> events,
            String target,
            NodeStatus status
    ) {
        return events.stream()
                .filter(event -> event.target().equals(target) && event.newStatus() == status)
                .map(DetectorEvent::atNanos)
                .min(Long::compareTo)
                .orElse(null);
    }

    private static List<String> members(int nodeCount) {
        if (nodeCount < 3) {
            throw new IllegalArgumentException("nodeCount must be at least 3");
        }
        return java.util.stream.IntStream.range(0, nodeCount)
                .mapToObj(index -> "node-" + index)
                .toList();
    }

    public record TargetOutcome(
            String target,
            List<String> assignedObservers,
            List<String> liveObservers,
            Long suspectLatencyNanos,
            Long deadLatencyNanos
    ) {
        public TargetOutcome {
            assignedObservers = List.copyOf(assignedObservers);
            liveObservers = List.copyOf(liveObservers);
        }

        public boolean detectedDead() {
            return deadLatencyNanos != null;
        }
    }

    public record Result(
            int nodeCount,
            int neighborCount,
            List<String> crashedTargets,
            long faultAtNanos,
            long durationNanos,
            long protocolMessages,
            long protocolBytes,
            List<TargetOutcome> outcomes,
            List<DetectorEvent> events
    ) {
        public Result {
            crashedTargets = List.copyOf(crashedTargets);
            outcomes = List.copyOf(outcomes);
            events = List.copyOf(events);
        }

        public long detectedDeadCount() {
            return outcomes.stream().filter(TargetOutcome::detectedDead).count();
        }

        public boolean allDetectedDead() {
            return detectedDeadCount() == outcomes.size();
        }
    }
}
