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
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/** Partition safety experiment for DEAD-driven distributed membership removal. */
public final class RingPartitionMembershipSafetyExperiment {
    public static final Duration DEFAULT_PARTITION_START = Duration.ofSeconds(20);
    public static final Duration DEFAULT_PARTITION_END = Duration.ofSeconds(32);
    public static final Duration DEFAULT_DURATION = Duration.ofSeconds(40);
    public static final Duration DEFAULT_PROPAGATION_DELAY = Duration.ofSeconds(1);

    private final boolean verifySuspect;
    private final boolean helperReachable;
    private final double helperPacketLoss;
    private final long seed;
    private final int failedVerificationsToDead;
    private final int helperCount;
    private final int blockedHelperCount;
    private final int helperFailureQuorum;

    public RingPartitionMembershipSafetyExperiment(boolean verifySuspect) {
        this(verifySuspect, true, 0.0, 0, 2);
    }

    public RingPartitionMembershipSafetyExperiment(
            boolean verifySuspect,
            boolean helperReachable,
            double helperPacketLoss,
            long seed
    ) {
        this(verifySuspect, helperReachable, helperPacketLoss, seed, 2);
    }

    public RingPartitionMembershipSafetyExperiment(
            boolean verifySuspect,
            boolean helperReachable,
            double helperPacketLoss,
            long seed,
            int failedVerificationsToDead
    ) {
        if (helperPacketLoss < 0.0 || helperPacketLoss > 1.0) {
            throw new IllegalArgumentException("helper packet loss must be between zero and one");
        }
        if (failedVerificationsToDead < 1) {
            throw new IllegalArgumentException("verification failure threshold must be positive");
        }
        this.verifySuspect = verifySuspect;
        this.helperReachable = helperReachable;
        this.helperPacketLoss = helperPacketLoss;
        this.seed = seed;
        this.failedVerificationsToDead = failedVerificationsToDead;
        this.helperCount = 1;
        this.blockedHelperCount = helperReachable ? 0 : 1;
        this.helperFailureQuorum = 1;
    }

    public RingPartitionMembershipSafetyExperiment(
            boolean verifySuspect,
            VerificationPolicy verificationPolicy,
            long seed
    ) {
        if (verificationPolicy == null) {
            throw new IllegalArgumentException("verification policy is required");
        }
        this.verifySuspect = verifySuspect;
        this.helperReachable = verificationPolicy.blockedHelperCount()
                < verificationPolicy.helperCount();
        this.helperPacketLoss = verificationPolicy.helperPacketLoss();
        this.seed = seed;
        this.failedVerificationsToDead = verificationPolicy.failedRoundsToDead();
        this.helperCount = verificationPolicy.helperCount();
        this.blockedHelperCount = verificationPolicy.blockedHelperCount();
        this.helperFailureQuorum = verificationPolicy.failureQuorum();
    }

    public Result run(int nodeCount, int targetIndex) {
        if (nodeCount < 5 || targetIndex < 0 || targetIndex >= nodeCount) {
            throw new IllegalArgumentException("invalid node count or target index");
        }
        List<String> members = java.util.stream.IntStream.range(0, nodeCount)
                .mapToObj(index -> "node-" + index)
                .toList();
        String target = members.get(targetIndex);
        RingMonitorAssignment ring = new RingMonitorAssignment(members, 2);
        List<String> observers = ring.assignments().entrySet().stream()
                .filter(entry -> entry.getValue().contains(target))
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
        FixedDeadlineConfig config = verifySuspect
                ? verifiedConfig(failedVerificationsToDead)
                : FixedDeadlineConfig.ring();
        Map<String, FixedDeadlineFailureDetector> detectors = new LinkedHashMap<>();
        Map<String, VersionedMembershipView> views = new LinkedHashMap<>();
        Map<String, Long> clocks = new LinkedHashMap<>();
        for (String member : members) {
            views.put(member, new VersionedMembershipView(members));
            clocks.put(member, 0L);
        }
        for (String observer : observers) {
            FixedDeadlineFailureDetector detector =
                    new FixedDeadlineFailureDetector(observer, config);
            detector.registerTarget(0, target, 1);
            detectors.put(observer, detector);
        }

        List<DetectorEvent> detectorEvents = new ArrayList<>();
        List<RemovalDelivery> deliveries = new ArrayList<>();
        List<QueuedRemoval> pending = new ArrayList<>();
        long heartbeatMessages = 0;
        long verificationMessages = 0;
        long verificationSuccesses = 0;
        long verificationFailures = 0;
        long helperProbeSuccesses = 0;
        long helperProbeFailures = 0;
        long controlMessages = 0;
        Long convergedAt = null;
        long partitionStart = DEFAULT_PARTITION_START.toNanos();
        long partitionEnd = DEFAULT_PARTITION_END.toNanos();
        long duration = DEFAULT_DURATION.toNanos();
        long interval = config.heartbeatInterval().toNanos();
        SplittableRandom verificationRandom = new SplittableRandom(seed);

        for (long now = 0; now <= duration; now += interval) {
            Iterator<QueuedRemoval> iterator = pending.iterator();
            while (iterator.hasNext()) {
                QueuedRemoval removal = iterator.next();
                if (removal.deliverAtNanos() > now) {
                    continue;
                }
                iterator.remove();
                VersionedMembershipView view = views.get(removal.receiver());
                VersionedMembershipView.ApplyOutcome outcome = view.applyRemoval(
                        target, removal.stamp());
                deliveries.add(new RemovalDelivery(
                        now, removal.sender(), removal.receiver(), outcome.name()));
            }

            boolean partitionActive = now >= partitionStart && now < partitionEnd;
            for (Map.Entry<String, FixedDeadlineFailureDetector> entry : detectors.entrySet()) {
                String observer = entry.getKey();
                FixedDeadlineFailureDetector detector = entry.getValue();
                if (views.get(observer).contains(target)) {
                    heartbeatMessages++;
                    if (!partitionActive) {
                        detector.recordHeartbeat(now, target, 1);
                    }
                }
                detector.tick(now);
                List<DetectorEvent> roundEvents = new ArrayList<>(detector.drainEvents());
                if (verifySuspect && detector.beginVerification(target)) {
                    verificationMessages += 3L * helperCount;
                    int failedProbes = 0;
                    for (int helperIndex = 0; helperIndex < helperCount; helperIndex++) {
                        boolean reachable = helperIndex >= blockedHelperCount
                                && verificationRandom.nextDouble() >= helperPacketLoss;
                        if (reachable) {
                            helperProbeSuccesses++;
                        } else {
                            helperProbeFailures++;
                            failedProbes++;
                        }
                    }
                    boolean verificationSucceeded = failedProbes < helperFailureQuorum;
                    if (verificationSucceeded) {
                        verificationSuccesses++;
                    } else {
                        verificationFailures++;
                    }
                    detector.recordVerification(now, target, verificationSucceeded);
                    roundEvents.addAll(detector.drainEvents());
                }
                detectorEvents.addAll(roundEvents);
                for (DetectorEvent event : roundEvents) {
                    if (event.newStatus() != NodeStatus.DEAD
                            || !views.get(observer).contains(target)) {
                        continue;
                    }
                    long nextClock = clocks.get(observer) + 1;
                    clocks.put(observer, nextClock);
                    VersionedMembershipView.VersionStamp stamp =
                            new VersionedMembershipView.VersionStamp(nextClock, observer);
                    views.get(observer).applyRemoval(target, stamp);
                    for (String receiver : members) {
                        if (receiver.equals(observer)) {
                            continue;
                        }
                        pending.add(new QueuedRemoval(
                                now + DEFAULT_PROPAGATION_DELAY.toNanos(),
                                observer,
                                receiver,
                                stamp
                        ));
                        controlMessages++;
                    }
                }
            }

            if (convergedAt == null
                    && views.values().stream().allMatch(view -> !view.contains(target))) {
                convergedAt = now;
            }
        }

        long falseDeadEvents = detectorEvents.stream()
                .filter(event -> event.target().equals(target)
                        && event.newStatus() == NodeStatus.DEAD)
                .count();
        long removedViews = views.values().stream()
                .filter(view -> !view.contains(target))
                .count();
        int payloadBytes = config.payloadBytes();
        return new Result(
                nodeCount,
                target,
                verifySuspect,
                helperReachable,
                helperPacketLoss,
                seed,
                failedVerificationsToDead,
                helperCount,
                blockedHelperCount,
                helperFailureQuorum,
                observers,
                falseDeadEvents,
                removedViews,
                convergedAt == null ? null : convergedAt - partitionStart,
                heartbeatMessages,
                verificationMessages,
                verificationSuccesses,
                verificationFailures,
                helperProbeSuccesses,
                helperProbeFailures,
                controlMessages,
                (heartbeatMessages + verificationMessages + controlMessages) * payloadBytes,
                detectorEvents,
                deliveries
        );
    }

    private static FixedDeadlineConfig verifiedConfig(int failureThreshold) {
        FixedDeadlineConfig base = FixedDeadlineConfig.ringVerified();
        return new FixedDeadlineConfig(
                base.heartbeatInterval(),
                base.deadline(),
                base.confirm(),
                base.warmup(),
                base.peerSelectionMode(),
                base.payloadBytes(),
                true,
                base.verificationInterval(),
                failureThreshold
        );
    }

    private record QueuedRemoval(
            long deliverAtNanos,
            String sender,
            String receiver,
            VersionedMembershipView.VersionStamp stamp
    ) {
    }

    public record RemovalDelivery(
            long atNanos,
            String sender,
            String receiver,
            String outcome
    ) {
    }

    public record VerificationPolicy(
            int helperCount,
            int blockedHelperCount,
            double helperPacketLoss,
            int failureQuorum,
            int failedRoundsToDead
    ) {
        public VerificationPolicy {
            if (helperCount < 1
                    || blockedHelperCount < 0
                    || blockedHelperCount > helperCount
                    || helperPacketLoss < 0.0
                    || helperPacketLoss > 1.0
                    || failureQuorum < 1
                    || failureQuorum > helperCount
                    || failedRoundsToDead < 1) {
                throw new IllegalArgumentException("invalid multi-helper verification policy");
            }
        }
    }

    public record Result(
            int nodeCount,
            String target,
            boolean verifySuspect,
            boolean helperReachable,
            double helperPacketLoss,
            long seed,
            int failedVerificationsToDead,
            int helperCount,
            int blockedHelperCount,
            int helperFailureQuorum,
            List<String> observers,
            long falseDeadEvents,
            long removedViewCount,
            Long falseRemovalConvergenceLatencyNanos,
            long heartbeatMessages,
            long verificationMessages,
            long verificationSuccesses,
            long verificationFailures,
            long helperProbeSuccesses,
            long helperProbeFailures,
            long membershipControlMessages,
            long totalBytes,
            List<DetectorEvent> detectorEvents,
            List<RemovalDelivery> deliveries
    ) {
        public Result {
            observers = List.copyOf(observers);
            detectorEvents = List.copyOf(detectorEvents);
            deliveries = List.copyOf(deliveries);
        }

        public long totalMessages() {
            return heartbeatMessages + verificationMessages + membershipControlMessages;
        }
    }
}
