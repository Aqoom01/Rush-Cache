package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.deadline.FixedDeadlineConfig;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.deadline.RingMonitorAssignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Delayed direct-broadcast DEAD dissemination with an independent view per node. */
public final class DistributedRingMembershipExperiment {
    public static final Duration DEFAULT_PROPAGATION_DELAY = Duration.ofSeconds(1);

    private final FixedDeadlineConfig detectorConfig;
    private final Duration faultStart;
    private final Duration duration;
    private final Duration propagationDelay;

    public DistributedRingMembershipExperiment() {
        this(
                FixedDeadlineConfig.ring(),
                RingMultiCrashExperiment.DEFAULT_FAULT_START,
                RingMultiCrashExperiment.DEFAULT_DURATION,
                DEFAULT_PROPAGATION_DELAY
        );
    }

    public DistributedRingMembershipExperiment(
            FixedDeadlineConfig detectorConfig,
            Duration faultStart,
            Duration duration,
            Duration propagationDelay
    ) {
        if (detectorConfig == null
                || detectorConfig.peerSelectionMode()
                != FixedDeadlineConfig.PeerSelectionMode.DETERMINISTIC_RING) {
            throw new IllegalArgumentException("Ring experiment requires deterministic Ring config");
        }
        if (faultStart == null || duration == null || propagationDelay == null
                || faultStart.isNegative() || duration.compareTo(faultStart) <= 0
                || propagationDelay.isNegative()) {
            throw new IllegalArgumentException("experiment durations are invalid");
        }
        this.detectorConfig = detectorConfig;
        this.faultStart = faultStart;
        this.duration = duration;
        this.propagationDelay = propagationDelay;
    }

    public Result runConsecutiveCrash(
            int nodeCount,
            int neighborCount,
            int firstCrashIndex,
            int crashCount
    ) {
        if (nodeCount < 5 || crashCount < 2 || crashCount > nodeCount - 2) {
            throw new IllegalArgumentException("experiment needs at least two surviving members");
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
        Set<String> crashed = new LinkedHashSet<>(crashedTargets);
        if (initialMembers == null || initialMembers.size() < 5
                || crashed.size() != crashedTargets.size() || crashed.size() < 2
                || !initialMembers.containsAll(crashed)
                || initialMembers.size() - crashed.size() < 2) {
            throw new IllegalArgumentException("invalid members or crash targets");
        }

        Map<String, NodeState> nodes = new LinkedHashMap<>();
        for (String member : initialMembers) {
            NodeState state = new NodeState(
                    member,
                    new FixedDeadlineFailureDetector(member, detectorConfig),
                    new VersionedMembershipView(initialMembers)
            );
            syncTargets(0, state, neighborCount);
            nodes.put(member, state);
        }

        List<DetectorEvent> detectorEvents = new ArrayList<>();
        List<NodeViewEvent> viewEvents = new ArrayList<>();
        List<ControlDeliveryEvent> deliveryEvents = new ArrayList<>();
        List<QueuedUpdate> pending = new ArrayList<>();
        Map<String, Long> firstLiveObserverAt = new LinkedHashMap<>();
        long heartbeatMessages = 0;
        long controlMessages = 0;
        long faultAtNanos = faultStart.toNanos();
        long durationNanos = duration.toNanos();
        long intervalNanos = detectorConfig.heartbeatInterval().toNanos();
        Long convergedAtNanos = null;

        for (long nowNanos = 0; nowNanos <= durationNanos; nowNanos += intervalNanos) {
            boolean faultActive = nowNanos >= faultAtNanos;
            Iterator<QueuedUpdate> iterator = pending.iterator();
            while (iterator.hasNext()) {
                QueuedUpdate update = iterator.next();
                if (update.deliverAtNanos() > nowNanos) {
                    continue;
                }
                iterator.remove();
                boolean receiverAlive = !faultActive || !crashed.contains(update.receiver());
                if (!receiverAlive) {
                    deliveryEvents.add(ControlDeliveryEvent.dropped(nowNanos, update));
                    continue;
                }
                NodeState receiver = nodes.get(update.receiver());
                receiver.logicalClock = Math.max(
                        receiver.logicalClock, update.stamp().logicalTime());
                VersionedMembershipView.ApplyOutcome outcome = receiver.view.applyRemoval(
                        update.target(), update.stamp());
                if (outcome == VersionedMembershipView.ApplyOutcome.MEMBER_REMOVED) {
                    syncTargets(nowNanos, receiver, neighborCount);
                    viewEvents.add(nodeViewEvent(
                            nowNanos, receiver, update.target(), "remote-dead-event",
                            update.stamp()));
                }
                deliveryEvents.add(ControlDeliveryEvent.delivered(nowNanos, update, outcome));
            }

            if (faultActive) {
                recordFirstLiveObserver(
                        nowNanos, nodes, crashed, neighborCount, firstLiveObserverAt);
            }

            for (NodeState state : nodes.values()) {
                boolean observerAlive = !faultActive || !crashed.contains(state.nodeId);
                RingMonitorAssignment assignment = assignment(state, neighborCount);
                for (String target : assignment.monitoredBy(state.nodeId)) {
                    boolean targetAlive = !faultActive || !crashed.contains(target);
                    if (targetAlive) {
                        heartbeatMessages++;
                        if (observerAlive) {
                            state.detector.recordHeartbeat(nowNanos, target, 1);
                        }
                    }
                }
                if (!observerAlive) {
                    continue;
                }
                state.detector.tick(nowNanos);
                List<DetectorEvent> roundEvents = state.detector.drainEvents();
                detectorEvents.addAll(roundEvents);
                for (DetectorEvent event : roundEvents) {
                    if (event.newStatus() != NodeStatus.DEAD
                            || !state.view.contains(event.target())) {
                        continue;
                    }
                    VersionedMembershipView.VersionStamp stamp =
                            new VersionedMembershipView.VersionStamp(
                                    ++state.logicalClock, state.nodeId);
                    VersionedMembershipView.ApplyOutcome outcome =
                            state.view.applyRemoval(event.target(), stamp);
                    if (outcome != VersionedMembershipView.ApplyOutcome.MEMBER_REMOVED) {
                        continue;
                    }
                    syncTargets(nowNanos, state, neighborCount);
                    viewEvents.add(nodeViewEvent(
                            nowNanos, state, event.target(), "local-detector-dead", stamp));
                    for (String receiver : initialMembers) {
                        if (receiver.equals(state.nodeId)) {
                            continue;
                        }
                        pending.add(new QueuedUpdate(
                                nowNanos + propagationDelay.toNanos(),
                                state.nodeId,
                                receiver,
                                event.target(),
                                stamp,
                                event.atNanos()
                        ));
                        controlMessages++;
                    }
                }
            }

            if (faultActive) {
                recordFirstLiveObserver(
                        nowNanos, nodes, crashed, neighborCount, firstLiveObserverAt);
            }

            if (faultActive && convergedAtNanos == null
                    && allSurvivorsRemovedAllCrashes(nodes, crashed)) {
                convergedAtNanos = nowNanos;
            }
        }

        List<TargetOutcome> outcomes = crashed.stream()
                .map(target -> targetOutcome(
                        target, detectorEvents, firstLiveObserverAt.get(target), faultAtNanos))
                .toList();
        long controlBytes = controlMessages * detectorConfig.payloadBytes();
        return new Result(
                initialMembers.size(),
                neighborCount,
                List.copyOf(crashed),
                faultAtNanos,
                durationNanos,
                propagationDelay.toNanos(),
                heartbeatMessages,
                heartbeatMessages * detectorConfig.payloadBytes(),
                controlMessages,
                controlBytes,
                convergedAtNanos == null ? null : convergedAtNanos - faultAtNanos,
                outcomes,
                detectorEvents,
                viewEvents,
                deliveryEvents
        );
    }

    private static RingMonitorAssignment assignment(NodeState state, int neighborCount) {
        return new RingMonitorAssignment(state.view.members(), neighborCount);
    }

    private static void syncTargets(long nowNanos, NodeState state, int neighborCount) {
        Set<String> desired = state.view.contains(state.nodeId)
                ? new LinkedHashSet<>(assignment(state, neighborCount).monitoredBy(state.nodeId))
                : Set.of();
        for (String target : List.copyOf(state.registeredTargets)) {
            if (!desired.contains(target)) {
                state.detector.unregisterTarget(target);
                state.registeredTargets.remove(target);
            }
        }
        for (String target : desired) {
            if (state.registeredTargets.add(target)) {
                state.detector.registerTarget(nowNanos, target, 1);
            }
        }
    }

    private static void recordFirstLiveObserver(
            long nowNanos,
            Map<String, NodeState> nodes,
            Set<String> crashed,
            int neighborCount,
            Map<String, Long> firstLiveObserverAt
    ) {
        for (String target : crashed) {
            boolean covered = nodes.values().stream()
                    .filter(state -> !crashed.contains(state.nodeId))
                    .filter(state -> state.view.contains(target))
                    .anyMatch(state -> assignment(state, neighborCount)
                            .monitoredBy(state.nodeId).contains(target));
            if (covered) {
                firstLiveObserverAt.putIfAbsent(target, nowNanos);
            }
        }
    }

    private static boolean allSurvivorsRemovedAllCrashes(
            Map<String, NodeState> nodes,
            Set<String> crashed
    ) {
        return nodes.values().stream()
                .filter(state -> !crashed.contains(state.nodeId))
                .allMatch(state -> crashed.stream().noneMatch(state.view::contains));
    }

    private static NodeViewEvent nodeViewEvent(
            long nowNanos,
            NodeState state,
            String removed,
            String source,
            VersionedMembershipView.VersionStamp stamp
    ) {
        return new NodeViewEvent(
                nowNanos,
                state.nodeId,
                state.view.viewVersion(),
                removed,
                state.view.members(),
                stamp,
                source
        );
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

    private static Long latency(Long atNanos, long faultAtNanos) {
        return atNanos == null ? null : atNanos - faultAtNanos;
    }

    private static final class NodeState {
        private final String nodeId;
        private final FixedDeadlineFailureDetector detector;
        private final VersionedMembershipView view;
        private final Set<String> registeredTargets = new LinkedHashSet<>();
        private long logicalClock;

        private NodeState(
                String nodeId,
                FixedDeadlineFailureDetector detector,
                VersionedMembershipView view
        ) {
            this.nodeId = nodeId;
            this.detector = detector;
            this.view = view;
        }
    }

    private record QueuedUpdate(
            long deliverAtNanos,
            String sender,
            String receiver,
            String target,
            VersionedMembershipView.VersionStamp stamp,
            long detectorEventAtNanos
    ) {
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

    public record NodeViewEvent(
            long atNanos,
            String node,
            int viewVersion,
            String removedMember,
            List<String> membersAfter,
            VersionedMembershipView.VersionStamp stamp,
            String source
    ) {
        public NodeViewEvent {
            membersAfter = List.copyOf(membersAfter);
        }
    }

    public record ControlDeliveryEvent(
            long atNanos,
            String sender,
            String receiver,
            String target,
            VersionedMembershipView.VersionStamp stamp,
            String outcome
    ) {
        private static ControlDeliveryEvent dropped(long now, QueuedUpdate update) {
            return from(now, update, "receiver-crashed");
        }

        private static ControlDeliveryEvent delivered(
                long now,
                QueuedUpdate update,
                VersionedMembershipView.ApplyOutcome outcome
        ) {
            return from(now, update, outcome.name().toLowerCase(LocaleHolder.ROOT));
        }

        private static ControlDeliveryEvent from(
                long now,
                QueuedUpdate update,
                String outcome
        ) {
            return new ControlDeliveryEvent(
                    now, update.sender(), update.receiver(), update.target(),
                    update.stamp(), outcome);
        }
    }

    private static final class LocaleHolder {
        private static final java.util.Locale ROOT = java.util.Locale.ROOT;
    }

    public record Result(
            int nodeCount,
            int neighborCount,
            List<String> crashedTargets,
            long faultAtNanos,
            long durationNanos,
            long propagationDelayNanos,
            long heartbeatMessages,
            long heartbeatBytes,
            long membershipControlMessages,
            long membershipControlBytes,
            Long viewConvergenceLatencyNanos,
            List<TargetOutcome> outcomes,
            List<DetectorEvent> detectorEvents,
            List<NodeViewEvent> viewEvents,
            List<ControlDeliveryEvent> deliveryEvents
    ) {
        public Result {
            crashedTargets = List.copyOf(crashedTargets);
            outcomes = List.copyOf(outcomes);
            detectorEvents = List.copyOf(detectorEvents);
            viewEvents = List.copyOf(viewEvents);
            deliveryEvents = List.copyOf(deliveryEvents);
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
            return outcomes.stream().map(TargetOutcome::deadLatencyNanos)
                    .max(Long::compareTo).orElse(null);
        }

        public long staleOrDuplicateDeliveries() {
            return deliveryEvents.stream()
                    .filter(event -> event.outcome().equals("stale_or_duplicate")
                            || event.outcome().equals("tombstone_advanced"))
                    .count();
        }
    }
}
