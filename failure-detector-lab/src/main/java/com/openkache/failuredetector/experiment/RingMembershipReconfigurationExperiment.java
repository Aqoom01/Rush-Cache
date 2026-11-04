package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.deadline.FixedDeadlineConfig;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.deadline.RingMonitorAssignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ideal shared-view Ring reconfiguration driven only by detector DEAD events.
 * The injected crash set controls transport delivery and offline scoring, but is
 * never passed to the membership update decision.
 */
public final class RingMembershipReconfigurationExperiment {
    private final FixedDeadlineConfig detectorConfig;
    private final Duration faultStart;
    private final Duration duration;

    public RingMembershipReconfigurationExperiment() {
        this(
                FixedDeadlineConfig.ring(),
                RingMultiCrashExperiment.DEFAULT_FAULT_START,
                RingMultiCrashExperiment.DEFAULT_DURATION
        );
    }

    public RingMembershipReconfigurationExperiment(
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

    public Result runConsecutiveCrash(
            int nodeCount,
            int neighborCount,
            int firstCrashIndex,
            int crashCount
    ) {
        if (nodeCount < 5 || crashCount < 2 || crashCount > nodeCount - 2) {
            throw new IllegalArgumentException(
                    "reconfiguration experiment needs at least two surviving members");
        }
        if (firstCrashIndex < 0 || firstCrashIndex >= nodeCount) {
            throw new IllegalArgumentException("firstCrashIndex is outside the Ring");
        }
        List<String> members = java.util.stream.IntStream.range(0, nodeCount)
                .mapToObj(index -> "node-" + index)
                .toList();
        List<String> crashedTargets = java.util.stream.IntStream.range(0, crashCount)
                .mapToObj(offset -> members.get((firstCrashIndex + offset) % nodeCount))
                .toList();
        return run(members, neighborCount, crashedTargets);
    }

    public Result run(
            List<String> initialMembers,
            int neighborCount,
            List<String> crashedTargets
    ) {
        if (initialMembers == null || initialMembers.size() < 5) {
            throw new IllegalArgumentException("reconfiguration experiment needs at least 5 nodes");
        }
        Set<String> crashed = new LinkedHashSet<>(crashedTargets);
        if (crashed.size() != crashedTargets.size() || crashed.size() < 2
                || !initialMembers.containsAll(crashed)
                || initialMembers.size() - crashed.size() < 2) {
            throw new IllegalArgumentException(
                    "crashed targets must be distinct members with two survivors");
        }

        RingMonitorAssignment assignment = new RingMonitorAssignment(
                initialMembers, neighborCount);
        Map<String, FixedDeadlineFailureDetector> detectors = new LinkedHashMap<>();
        Map<String, Set<String>> registeredTargets = new LinkedHashMap<>();
        for (String observer : assignment.members()) {
            detectors.put(observer, new FixedDeadlineFailureDetector(observer, detectorConfig));
            registeredTargets.put(observer, new LinkedHashSet<>());
        }
        syncTargets(0, assignment, detectors, registeredTargets);

        List<DetectorEvent> detectorEvents = new ArrayList<>();
        List<MembershipViewEvent> membershipEvents = new ArrayList<>();
        Map<String, Long> firstLiveObserverAt = new LinkedHashMap<>();
        long heartbeatMessages = 0;
        long faultAtNanos = faultStart.toNanos();
        long durationNanos = duration.toNanos();
        long intervalNanos = detectorConfig.heartbeatInterval().toNanos();
        int viewVersion = 1;

        for (long nowNanos = 0; nowNanos <= durationNanos; nowNanos += intervalNanos) {
            boolean faultActive = nowNanos >= faultAtNanos;
            if (faultActive) {
                recordFirstLiveObserver(
                        nowNanos, assignment, crashed, firstLiveObserverAt);
            }

            List<DetectorEvent> roundEvents = new ArrayList<>();
            for (Map.Entry<String, List<String>> edge : assignment.assignments().entrySet()) {
                String observer = edge.getKey();
                boolean observerAlive = !faultActive || !crashed.contains(observer);
                FixedDeadlineFailureDetector detector = detectors.get(observer);
                for (String target : edge.getValue()) {
                    boolean targetAlive = !faultActive || !crashed.contains(target);
                    if (targetAlive) {
                        heartbeatMessages++;
                        if (observerAlive) {
                            detector.recordHeartbeat(nowNanos, target, 1);
                        }
                    }
                }
                if (observerAlive) {
                    detector.tick(nowNanos);
                    roundEvents.addAll(detector.drainEvents());
                }
            }
            detectorEvents.addAll(roundEvents);

            Set<String> deadFromDetector = roundEvents.stream()
                    .filter(event -> event.newStatus() == NodeStatus.DEAD)
                    .map(DetectorEvent::target)
                    .filter(assignment.members()::contains)
                    .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
            if (!deadFromDetector.isEmpty()) {
                List<String> before = assignment.members();
                List<String> after = before.stream()
                        .filter(member -> !deadFromDetector.contains(member))
                        .toList();
                if (after.size() < 2) {
                    throw new IllegalStateException("membership view cannot shrink below two nodes");
                }
                assignment.updateView(after);
                syncTargets(nowNanos, assignment, detectors, registeredTargets);
                recordFirstLiveObserver(
                        nowNanos, assignment, crashed, firstLiveObserverAt);
                viewVersion++;
                List<String> evidence = roundEvents.stream()
                        .filter(event -> event.newStatus() == NodeStatus.DEAD
                                && deadFromDetector.contains(event.target()))
                        .map(event -> event.observer() + "->" + event.target())
                        .sorted()
                        .toList();
                membershipEvents.add(new MembershipViewEvent(
                        nowNanos,
                        viewVersion,
                        List.copyOf(deadFromDetector),
                        before,
                        assignment.members(),
                        evidence,
                        "detector-dead-events"
                ));
            }
        }

        List<TargetOutcome> outcomes = crashed.stream()
                .map(target -> targetOutcome(
                        target, detectorEvents, firstLiveObserverAt.get(target), faultAtNanos))
                .toList();
        Long membershipCompleteAt = membershipEvents.stream()
                .filter(event -> event.membersAfter().stream().noneMatch(crashed::contains))
                .map(MembershipViewEvent::atNanos)
                .findFirst()
                .orElse(null);
        return new Result(
                initialMembers.size(),
                neighborCount,
                List.copyOf(crashed),
                faultAtNanos,
                durationNanos,
                heartbeatMessages,
                heartbeatMessages * detectorConfig.payloadBytes(),
                0,
                0,
                membershipCompleteAt == null ? null : membershipCompleteAt - faultAtNanos,
                outcomes,
                detectorEvents,
                membershipEvents
        );
    }

    private static void syncTargets(
            long nowNanos,
            RingMonitorAssignment assignment,
            Map<String, FixedDeadlineFailureDetector> detectors,
            Map<String, Set<String>> registeredTargets
    ) {
        Set<String> activeObservers = Set.copyOf(assignment.members());
        for (Map.Entry<String, FixedDeadlineFailureDetector> entry : detectors.entrySet()) {
            String observer = entry.getKey();
            Set<String> current = registeredTargets.get(observer);
            Set<String> desired = activeObservers.contains(observer)
                    ? new LinkedHashSet<>(assignment.monitoredBy(observer))
                    : Set.of();
            for (String target : List.copyOf(current)) {
                if (!desired.contains(target)) {
                    entry.getValue().unregisterTarget(target);
                    current.remove(target);
                }
            }
            for (String target : desired) {
                if (current.add(target)) {
                    entry.getValue().registerTarget(nowNanos, target, 1);
                }
            }
        }
    }

    private static void recordFirstLiveObserver(
            long nowNanos,
            RingMonitorAssignment assignment,
            Set<String> crashed,
            Map<String, Long> firstLiveObserverAt
    ) {
        for (String target : crashed) {
            boolean covered = assignment.assignments().entrySet().stream()
                    .anyMatch(entry -> !crashed.contains(entry.getKey())
                            && entry.getValue().contains(target));
            if (covered) {
                firstLiveObserverAt.putIfAbsent(target, nowNanos);
            }
        }
    }

    private static TargetOutcome targetOutcome(
            String target,
            List<DetectorEvent> events,
            Long firstLiveObserverAt,
            long faultAtNanos
    ) {
        Long suspectAt = firstTransitionAt(events, target, NodeStatus.SUSPECT);
        Long deadAt = firstTransitionAt(events, target, NodeStatus.DEAD);
        return new TargetOutcome(
                target,
                latency(suspectAt, faultAtNanos),
                latency(deadAt, faultAtNanos),
                latency(firstLiveObserverAt, faultAtNanos)
        );
    }

    private static Long firstTransitionAt(
            Collection<DetectorEvent> events,
            String target,
            NodeStatus status
    ) {
        return events.stream()
                .filter(event -> event.target().equals(target) && event.newStatus() == status)
                .map(DetectorEvent::atNanos)
                .min(Long::compareTo)
                .orElse(null);
    }

    private static Long latency(Long atNanos, long faultAtNanos) {
        return atNanos == null ? null : atNanos - faultAtNanos;
    }

    public record TargetOutcome(
            String target,
            Long suspectLatencyNanos,
            Long deadLatencyNanos,
            Long firstLiveObserverLatencyNanos
    ) {
        public boolean detectedDead() {
            return deadLatencyNanos != null;
        }
    }

    public record MembershipViewEvent(
            long atNanos,
            int viewVersion,
            List<String> removedMembers,
            List<String> membersBefore,
            List<String> membersAfter,
            List<String> detectorEvidence,
            String source
    ) {
        public MembershipViewEvent {
            removedMembers = List.copyOf(removedMembers);
            membersBefore = List.copyOf(membersBefore);
            membersAfter = List.copyOf(membersAfter);
            detectorEvidence = List.copyOf(detectorEvidence);
        }
    }

    public record Result(
            int nodeCount,
            int neighborCount,
            List<String> crashedTargets,
            long faultAtNanos,
            long durationNanos,
            long heartbeatMessages,
            long heartbeatBytes,
            long membershipControlMessages,
            long membershipControlBytes,
            Long membershipCompleteLatencyNanos,
            List<TargetOutcome> outcomes,
            List<DetectorEvent> detectorEvents,
            List<MembershipViewEvent> membershipEvents
    ) {
        public Result {
            crashedTargets = List.copyOf(crashedTargets);
            outcomes = List.copyOf(outcomes);
            detectorEvents = List.copyOf(detectorEvents);
            membershipEvents = List.copyOf(membershipEvents);
        }

        public long detectedDeadCount() {
            return outcomes.stream().filter(TargetOutcome::detectedDead).count();
        }

        public boolean allDetectedDead() {
            return detectedDeadCount() == outcomes.size();
        }

        public Long lastDeadLatencyNanos() {
            if (!allDetectedDead()) {
                return null;
            }
            return outcomes.stream()
                    .map(TargetOutcome::deadLatencyNanos)
                    .max(Long::compareTo)
                    .orElse(null);
        }
    }
}
