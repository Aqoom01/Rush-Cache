package com.openkache.failuredetector.product;

import com.openkache.failuredetector.aerospike.AerospikeMeshConfig;
import com.openkache.failuredetector.aerospike.AerospikeMeshFailureDetector;
import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.ignite.IgniteTcpRingConfig;
import com.openkache.failuredetector.ignite.IgniteTcpRingFailureDetector;
import com.openkache.failuredetector.phi.GossipHeartbeat;
import com.openkache.failuredetector.phi.PhiAccrualConfig;
import com.openkache.failuredetector.phi.PhiAccrualFailureDetector;
import com.openkache.failuredetector.redis.RedisClusterConfig;
import com.openkache.failuredetector.redis.RedisClusterFailureDetector;
import com.openkache.failuredetector.swim.SwimLifeguardConfig;
import com.openkache.failuredetector.swim.SwimLifeguardDetector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/** Common deterministic fault schedule used by all five product-inspired profiles. */
public final class ProductBenchmarkRunner {
    public static final String OBSERVER = "node-0";
    private static final long SECOND = 1_000_000_000L;
    private static final long TICK = 50_000_000L;
    private static final long FAULT_START = 5 * SECOND;
    private static final long FAULT_END = 20 * SECOND;
    private static final long DURATION = 30 * SECOND;

    public ProductRunResult run(
            ProductAlgorithm algorithm,
            ProductScenario scenario,
            long seed,
            int nodeCount,
            String mode
    ) {
        if (nodeCount < 5) {
            throw new IllegalArgumentException("product comparison requires at least five nodes");
        }
        List<String> nodes = nodes(nodeCount);
        List<String> targets = targets(scenario, nodeCount);
        Harness harness = createHarness(algorithm, nodes);
        Random random = new Random(mix(seed, algorithm.ordinal(), scenario.ordinal()));
        List<BenchmarkEvent> events = new ArrayList<>();
        Set<String> unavailable = new HashSet<>();
        Map<String, Long> unavailableSince = new HashMap<>();
        long falseUnavailableNanos = 0;
        long falseUnavailableEvents = 0;
        long falseDeadEvents = 0;
        boolean faultRecorded = false;
        boolean membershipConvergenceRecorded = false;
        long observerResumeAt = -1;

        for (long now = 0; now <= DURATION; now += TICK) {
            boolean observerStalled = scenario == ProductScenario.OBSERVER_STALL
                    && now >= FAULT_START && now < FAULT_END;
            if (observerStalled) {
                observerResumeAt = FAULT_END;
                continue;
            }
            if (observerResumeAt == now) {
                harness.onObserverResume(FAULT_END - FAULT_START);
            }

            if (!faultRecorded && now >= FAULT_START && scenario != ProductScenario.NORMAL) {
                for (String target : targets) {
                    events.add(new BenchmarkEvent(
                            now,
                            BenchmarkEventType.FAULT_INJECTED,
                            OBSERVER,
                            target,
                            Map.of("scenario", scenario.id(), "seed", seed)
                    ));
                }
                faultRecorded = true;
            }

            if (now % harness.sendIntervalNanos() == 0) {
                for (String target : monitoredTargets(nodes)) {
                    NetworkOutcome outcome = outcome(
                            scenario, target, targets, now, random, algorithm);
                    harness.send(now, target, outcome.delivered(), outcome.delayNanos());
                }
            }
            harness.tick(now);
            harness.advanceAgreement(now);

            for (DetectorEvent detectorEvent : harness.drainEvents()) {
                boolean targetAlive = targetAlive(scenario, detectorEvent.target(), now, targets);
                if (detectorEvent.newStatus() == NodeStatus.SUSPECT) {
                    events.add(normalized(detectorEvent, BenchmarkEventType.LOCAL_SUSPECTED));
                    events.add(normalized(detectorEvent, BenchmarkEventType.LOCAL_UNAVAILABLE));
                    if (unavailable.add(detectorEvent.target())) {
                        unavailableSince.put(detectorEvent.target(), detectorEvent.atNanos());
                        if (targetAlive) {
                            falseUnavailableEvents++;
                        }
                    }
                } else if (detectorEvent.newStatus() == NodeStatus.DEAD) {
                    if (unavailable.add(detectorEvent.target())) {
                        events.add(normalized(detectorEvent, BenchmarkEventType.LOCAL_UNAVAILABLE));
                        unavailableSince.put(detectorEvent.target(), detectorEvent.atNanos());
                        if (targetAlive) {
                            falseUnavailableEvents++;
                        }
                    }
                    if (targetAlive) {
                        falseDeadEvents++;
                    }
                    if (algorithm == ProductAlgorithm.REDIS_CLUSTER) {
                        events.add(normalized(detectorEvent, BenchmarkEventType.CLUSTER_CONFIRMED));
                    }
                    if (algorithm == ProductAlgorithm.IGNITE_TCP_RING
                            || algorithm == ProductAlgorithm.AEROSPIKE_MESH) {
                        events.add(normalized(detectorEvent, BenchmarkEventType.CLUSTER_CONFIRMED));
                        events.add(normalized(detectorEvent, BenchmarkEventType.MEMBERSHIP_CHANGED));
                    }
                } else if (detectorEvent.newStatus() == NodeStatus.ALIVE
                        && unavailable.remove(detectorEvent.target())) {
                    Long since = unavailableSince.remove(detectorEvent.target());
                    if (since != null && targetAlive) {
                        falseUnavailableNanos += detectorEvent.atNanos() - since;
                    }
                    events.add(normalized(detectorEvent, BenchmarkEventType.RECOVERED));
                }
            }

            if (!membershipConvergenceRecorded
                    && supportsMembershipChange(algorithm)
                    && targets.stream().allMatch(target -> harness.status(target) == NodeStatus.DEAD)) {
                long convergedAt = now + 2 * TICK;
                events.add(new BenchmarkEvent(
                        convergedAt,
                        BenchmarkEventType.MEMBERSHIP_CONVERGED,
                        OBSERVER,
                        String.join(";", targets),
                        Map.of("survivors", nodeCount - targets.size())
                ));
                membershipConvergenceRecorded = true;
            }
        }

        for (String target : unavailable) {
            if (targetAlive(scenario, target, DURATION, targets)) {
                falseUnavailableNanos += DURATION - unavailableSince.getOrDefault(target, DURATION);
            }
        }

        int detectedTargets = (int) targets.stream()
                .filter(target -> events.stream().anyMatch(event -> event.target().equals(target)
                        && event.type() == BenchmarkEventType.LOCAL_UNAVAILABLE
                        && event.atNanos() >= FAULT_START))
                .count();
        boolean detectionSuccess = !scenario.expectsCrashDetection()
                || detectedTargets == targets.size();
        Long detection = scenario.expectsCrashDetection()
                ? latency(events, BenchmarkEventType.LOCAL_UNAVAILABLE, targets) : null;
        Long cluster = scenario.expectsCrashDetection()
                ? latency(events, BenchmarkEventType.CLUSTER_CONFIRMED, targets) : null;
        Long convergence = first(events, BenchmarkEventType.MEMBERSHIP_CONVERGED) == null
                ? null : first(events, BenchmarkEventType.MEMBERSHIP_CONVERGED) - FAULT_START;
        Cost cost = harness.cost();
        return new ProductRunResult(
                algorithm.id() + "-" + scenario.id() + "-n" + nodeCount + "-s" + seed,
                algorithm,
                scenario,
                seed,
                nodeCount,
                mode,
                DURATION,
                FAULT_START,
                targets,
                events,
                detection,
                cluster,
                convergence,
                falseUnavailableEvents,
                falseDeadEvents,
                falseUnavailableNanos,
                detectedTargets,
                detectionSuccess,
                cost.txMessages,
                cost.rxMessages,
                cost.txBytes,
                cost.rxBytes,
                harness.parameters(),
                "inspired",
                "simulation-observed",
                "model-estimated",
                true,
                ""
        );
    }

    private static Harness createHarness(ProductAlgorithm algorithm, List<String> nodes) {
        return switch (algorithm) {
            case SWIM_LIFEGUARD -> new SwimHarness(nodes.size());
            case REDIS_CLUSTER -> new RedisHarness(nodes);
            case PHI_GOSSIP -> new PhiHarness();
            case IGNITE_TCP_RING -> new IgniteHarness(nodes);
            case AEROSPIKE_MESH -> new AerospikeHarness(nodes);
        };
    }

    private static BenchmarkEvent normalized(DetectorEvent event, BenchmarkEventType type) {
        return new BenchmarkEvent(
                event.atNanos(), type, event.observer(), event.target(), event.details());
    }

    private static boolean supportsMembershipChange(ProductAlgorithm algorithm) {
        return algorithm == ProductAlgorithm.IGNITE_TCP_RING
                || algorithm == ProductAlgorithm.AEROSPIKE_MESH;
    }

    private static Long latency(
            List<BenchmarkEvent> events,
            BenchmarkEventType type,
            List<String> targets
    ) {
        long latest = -1;
        for (String target : targets) {
            Long targetAt = events.stream()
                    .filter(event -> event.type() == type
                            && event.target().equals(target)
                            && event.atNanos() >= FAULT_START)
                    .mapToLong(BenchmarkEvent::atNanos)
                    .min()
                    .stream().boxed().findFirst().orElse(null);
            if (targetAt == null) {
                return null;
            }
            latest = Math.max(latest, targetAt);
        }
        return latest - FAULT_START;
    }

    private static Long first(List<BenchmarkEvent> events, BenchmarkEventType type) {
        return events.stream().filter(event -> event.type() == type)
                .mapToLong(BenchmarkEvent::atNanos).min().stream().boxed().findFirst().orElse(null);
    }

    private static boolean targetAlive(
            ProductScenario scenario,
            String target,
            long now,
            List<String> targets
    ) {
        return !(scenario.expectsCrashDetection() && targets.contains(target) && now >= FAULT_START);
    }

    private static NetworkOutcome outcome(
            ProductScenario scenario,
            String target,
            List<String> faultTargets,
            long now,
            Random random,
            ProductAlgorithm algorithm
    ) {
        boolean inWindow = now >= FAULT_START && now < FAULT_END;
        boolean selectedTarget = faultTargets.contains(target);
        if ((scenario.expectsCrashDetection() && selectedTarget && now >= FAULT_START)
                || (scenario == ProductScenario.TARGET_STALL && selectedTarget && inWindow)
                || (scenario == ProductScenario.PARTITION && selectedTarget && inWindow)) {
            return new NetworkOutcome(false, 0);
        }
        double loss = scenario == ProductScenario.LOSS_15 ? 0.15
                : scenario == ProductScenario.LOSS_45 ? 0.45 : 0.0;
        int attempts = algorithm == ProductAlgorithm.SWIM_LIFEGUARD ? 4 : 1;
        boolean delivered = true;
        for (int attempt = 0; attempt < attempts; attempt++) {
            if (random.nextDouble() >= loss) {
                delivered = true;
                break;
            }
            delivered = false;
        }
        long delay = 20_000_000L;
        if (scenario == ProductScenario.DELAY_JITTER && inWindow) {
            delay = 400_000_000L + (long) (random.nextDouble() * 3_200_000_000L);
            delivered = delivered && delay < 3 * SECOND;
        }
        return new NetworkOutcome(delivered, delay);
    }

    private static long mix(long seed, int algorithm, int scenario) {
        return seed * 1_000_003L + algorithm * 10_007L + scenario * 101L;
    }

    private static List<String> nodes(int count) {
        List<String> nodes = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            nodes.add("node-" + index);
        }
        return List.copyOf(nodes);
    }

    private static List<String> monitoredTargets(List<String> nodes) {
        return nodes.subList(1, nodes.size());
    }

    private static List<String> targets(ProductScenario scenario, int nodeCount) {
        int count = scenario.expectsCrashDetection() ? scenario.crashedTargets() : 1;
        if (count >= nodeCount) {
            throw new IllegalArgumentException("not enough survivor nodes for scenario");
        }
        List<String> targets = new ArrayList<>();
        for (int index = 1; index <= count; index++) {
            targets.add("node-" + index);
        }
        return List.copyOf(targets);
    }

    private interface Harness {
        void send(long nowNanos, String target, boolean delivered, long delayNanos);

        void tick(long nowNanos);

        void advanceAgreement(long nowNanos);

        List<DetectorEvent> drainEvents();

        NodeStatus status(String target);

        Cost cost();

        Map<String, Object> parameters();

        default long sendIntervalNanos() {
            return SECOND;
        }

        default void onObserverResume(long elapsedNanos) {
        }
    }

    private abstract static class CostedHarness implements Harness {
        protected final Cost cost = new Cost();

        @Override
        public Cost cost() {
            return cost;
        }

        protected void exchange(int txFrames, int rxFrames, int frameBytes) {
            cost.txMessages += txFrames;
            cost.rxMessages += rxFrames;
            cost.txBytes += (long) txFrames * frameBytes;
            cost.rxBytes += (long) rxFrames * frameBytes;
        }
    }

    private static final class SwimHarness extends CostedHarness {
        private final SwimLifeguardDetector detector;

        private SwimHarness(int nodes) {
            detector = new SwimLifeguardDetector(OBSERVER, nodes, SwimLifeguardConfig.defaults());
        }

        @Override
        public void send(long now, String target, boolean delivered, long delay) {
            exchange(delivered ? 1 : 4, delivered ? 1 : 0, 96);
            long observedAt = delivered
                    ? now + delay
                    : now + 2 * detector.effectiveProbeTimeoutNanos();
            detector.recordProbe(observedAt, target, delivered, 1);
        }

        @Override public void tick(long now) { detector.tick(now); }
        @Override public void advanceAgreement(long now) { }
        @Override public List<DetectorEvent> drainEvents() { return detector.drainEvents(); }
        @Override public NodeStatus status(String target) { return detector.status(target); }
        @Override public void onObserverResume(long elapsed) { detector.observeLocalDelay(elapsed); }
        @Override public Map<String, Object> parameters() {
            return Map.of("probeIntervalMs", 1000, "probeTimeoutMs", 500,
                    "indirectChecks", detector.config().indirectChecks(),
                    "localHealthAwareness", true);
        }
    }

    private static final class RedisHarness extends CostedHarness {
        private final RedisClusterFailureDetector detector;
        private final List<String> masters;

        private RedisHarness(List<String> nodes) {
            masters = nodes.stream().filter(node -> !node.equals("node-1")).toList();
            detector = new RedisClusterFailureDetector(
                    OBSERVER, Set.copyOf(masters), RedisClusterConfig.defaults());
            nodes.stream().skip(1).forEach(target -> detector.registerTarget(0, target));
        }

        @Override
        public void send(long now, String target, boolean delivered, long delay) {
            detector.recordPingSent(now, target);
            exchange(1, delivered ? 1 : 0, 2256);
            if (delivered) {
                detector.recordPong(now + delay, target);
            }
        }

        @Override public void tick(long now) { detector.tick(now); }

        @Override
        public void advanceAgreement(long now) {
            for (String target : monitoredTargets(mastersWithTarget())) {
                if (detector.status(target) != NodeStatus.SUSPECT) {
                    continue;
                }
                for (String master : masters) {
                    if (!detector.reportingMasters(now, target).contains(master)) {
                        detector.recordFailureReport(now, target, master);
                        exchange(1, 1, 2256);
                        break;
                    }
                }
            }
        }

        private List<String> mastersWithTarget() {
            List<String> all = new ArrayList<>();
            all.add(OBSERVER);
            for (int index = 1; index <= masters.size(); index++) {
                all.add("node-" + index);
            }
            return all;
        }

        @Override public List<DetectorEvent> drainEvents() { return detector.drainEvents(); }
        @Override public NodeStatus status(String target) { return detector.status(target); }
        @Override public Map<String, Object> parameters() {
            return Map.of("nodeTimeoutMs", 15000, "failureReportValidityMs", 30000,
                    "masterReportsRequired", detector.requiredReports());
        }
    }

    private static final class PhiHarness extends CostedHarness {
        private final PhiAccrualFailureDetector detector = new PhiAccrualFailureDetector(OBSERVER);
        private final Map<String, Long> versions = new HashMap<>();

        @Override
        public void send(long now, String target, boolean delivered, long delay) {
            exchange(1, delivered ? 1 : 0, 160);
            if (delivered) {
                long version = versions.merge(target, 1L, Long::sum);
                detector.recordHeartbeat(new GossipHeartbeat(
                        target, 1, version, now + delay), true);
            }
        }

        @Override public void tick(long now) { detector.tick(now); }
        @Override public void advanceAgreement(long now) { }
        @Override public List<DetectorEvent> drainEvents() { return detector.drainEvents(); }
        @Override public NodeStatus status(String target) { return detector.status(target); }
        @Override public Map<String, Object> parameters() {
            PhiAccrualConfig config = detector.config();
            return Map.of("heartbeatIntervalMs", 1000,
                    "phiThreshold", config.convictThreshold(), "localDownOnly", true);
        }
    }

    private static final class IgniteHarness extends CostedHarness {
        private final IgniteTcpRingFailureDetector detector;

        private IgniteHarness(List<String> nodes) {
            detector = new IgniteTcpRingFailureDetector(
                    OBSERVER, OBSERVER, nodes, IgniteTcpRingConfig.defaults());
        }

        @Override
        public void send(long now, String target, boolean delivered, long delay) {
            exchange(1, delivered ? 1 : 0, 128);
            if (delivered) {
                detector.recordNeighborMessage(now + delay, target);
            }
        }

        @Override public void tick(long now) { detector.tick(now); }

        @Override
        public void advanceAgreement(long now) {
            String neighbor = detector.nextAliveNeighbor(OBSERVER);
            if (neighbor != null && detector.status(neighbor) == NodeStatus.SUSPECT) {
                detector.coordinatorConfirmFailure(now, OBSERVER, neighbor);
                exchange(1, Math.max(0, detector.members().size() - 1), 192);
            }
        }

        @Override public List<DetectorEvent> drainEvents() { return detector.drainEvents(); }
        @Override public NodeStatus status(String target) { return detector.status(target); }
        @Override public Map<String, Object> parameters() {
            return Map.of("neighborTimeoutMs", 5000, "topologyCoordinator", OBSERVER,
                    "transport", "modeled-tcp-ring");
        }
    }

    private static final class AerospikeHarness extends CostedHarness {
        private final AerospikeMeshFailureDetector detector;
        private final List<String> nodes;
        private final Map<String, Long> proposals = new LinkedHashMap<>();
        private long nextEpoch = 1;

        private AerospikeHarness(List<String> nodes) {
            this.nodes = nodes;
            detector = new AerospikeMeshFailureDetector(
                    OBSERVER, Set.copyOf(nodes), AerospikeMeshConfig.defaults());
        }

        @Override
        public void send(long now, String target, boolean delivered, long delay) {
            exchange(1, delivered ? 1 : 0, 96);
            if (delivered) {
                detector.recordHeartbeat(now + delay, target);
            }
        }

        @Override public void tick(long now) { detector.tick(now); }

        @Override
        public void advanceAgreement(long now) {
            for (String target : nodes.stream().skip(1).toList()) {
                if (detector.status(target) != NodeStatus.SUSPECT) {
                    continue;
                }
                long epoch = proposals.computeIfAbsent(target, ignored -> nextEpoch++);
                detector.proposeMembershipRemoval(epoch, target);
                for (String voter : nodes) {
                    if (!voter.equals(target)) {
                        detector.acknowledgeMembershipProposal(now, epoch, voter);
                        exchange(1, 1, 144);
                        if (detector.status(target) == NodeStatus.DEAD) {
                            break;
                        }
                    }
                }
            }
        }

        @Override public List<DetectorEvent> drainEvents() { return detector.drainEvents(); }
        @Override public NodeStatus status(String target) { return detector.status(target); }
        @Override public long sendIntervalNanos() { return 150_000_000L; }
        @Override public Map<String, Object> parameters() {
            return Map.of("heartbeatIntervalMs", 150, "heartbeatTimeoutIntervals", 10,
                    "heartbeatTimeoutMs", 1500,
                    "agreement", "inspired-epoch-majority-not-paxos");
        }
    }

    private record NetworkOutcome(boolean delivered, long delayNanos) {
    }

    public static final class Cost {
        private long txMessages;
        private long rxMessages;
        private long txBytes;
        private long rxBytes;
    }
}
