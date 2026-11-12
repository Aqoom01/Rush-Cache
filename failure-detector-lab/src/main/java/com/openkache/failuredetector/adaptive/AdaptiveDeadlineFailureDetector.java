package com.openkache.failuredetector.adaptive;

import com.openkache.failuredetector.core.AbstractFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * Mean-and-variance adaptive heartbeat deadline baseline.
 *
 * <p>This is a controlled Chen/Bertier-inspired experimental baseline, not a
 * source-compatible reproduction of either implementation.</p>
 */
public final class AdaptiveDeadlineFailureDetector extends AbstractFailureDetector {
    public static final String ALGORITHM = "adaptive-deadline";

    private final AdaptiveDeadlineConfig config;
    private final Map<String, PeerState> peers = new HashMap<>();

    public AdaptiveDeadlineFailureDetector(String observer) {
        this(observer, AdaptiveDeadlineConfig.defaults());
    }

    public AdaptiveDeadlineFailureDetector(String observer, AdaptiveDeadlineConfig config) {
        super(observer);
        this.config = config;
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }

    public AdaptiveDeadlineConfig config() {
        return config;
    }

    public void registerTarget(long nowNanos, String target, long incarnation) {
        peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos, incarnation));
    }

    public boolean recordHeartbeat(long nowNanos, String target, long incarnation) {
        PeerState peer = peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos, incarnation));
        if (incarnation < peer.incarnation
                || (status(target) == NodeStatus.DEAD && incarnation == peer.incarnation)) {
            return false;
        }
        if (peer.hasHeartbeat) {
            long interval = nowNanos - peer.lastSeenAtNanos;
            if (interval <= 0) {
                return false;
            }
            if (interval <= config.maximumSampleInterval().toNanos()) {
                peer.arrivals.add(interval);
            }
        }
        peer.hasHeartbeat = true;
        peer.lastSeenAtNanos = nowNanos;
        peer.incarnation = incarnation;
        peer.suspectAtNanos = -1;
        if (status(target) != NodeStatus.ALIVE) {
            transition(
                    nowNanos,
                    target,
                    NodeStatus.ALIVE,
                    "adaptive heartbeat restored",
                    Map.of("incarnation", incarnation, "deadlineNanos", deadlineNanos(target))
            );
        }
        return true;
    }

    public long deadlineNanos(String target) {
        PeerState peer = peers.get(target);
        if (peer == null || peer.arrivals.size() < config.warmupSamples()) {
            return clamp(initialDeadline());
        }
        double mean = peer.arrivals.mean();
        double deviation = Math.max(peer.arrivals.standardDeviation(),
                config.minimumStandardDeviation().toNanos());
        return clamp((long) Math.ceil(mean
                + config.standardDeviationMultiplier() * deviation
                + config.grace().toNanos()));
    }

    @Override
    public void tick(long nowNanos) {
        for (Map.Entry<String, PeerState> entry : peers.entrySet()) {
            String target = entry.getKey();
            PeerState peer = entry.getValue();
            if (nowNanos - peer.registeredAtNanos < config.warmup().toNanos()) {
                continue;
            }
            long deadline = deadlineNanos(target);
            if (status(target) == NodeStatus.ALIVE
                    && nowNanos - peer.lastSeenAtNanos >= deadline) {
                peer.suspectAtNanos = nowNanos;
                transition(
                        nowNanos,
                        target,
                        NodeStatus.SUSPECT,
                        "adaptive heartbeat deadline expired",
                        Map.of(
                                "incarnation", peer.incarnation,
                                "deadlineNanos", deadline,
                                "sampleCount", peer.arrivals.size()
                        )
                );
            }
            if (status(target) == NodeStatus.SUSPECT
                    && peer.suspectAtNanos >= 0
                    && nowNanos - peer.suspectAtNanos >= config.confirm().toNanos()) {
                transition(
                        nowNanos,
                        target,
                        NodeStatus.DEAD,
                        "adaptive deadline confirmation period expired",
                        Map.of("incarnation", peer.incarnation, "deadlineNanos", deadline)
                );
            }
        }
    }

    private long initialDeadline() {
        return (long) Math.ceil(config.initialInterval().toNanos()
                + config.standardDeviationMultiplier()
                * config.minimumStandardDeviation().toNanos()
                + config.grace().toNanos());
    }

    private long clamp(long value) {
        return Math.max(
                config.minimumDeadline().toNanos(),
                Math.min(config.maximumDeadline().toNanos(), value)
        );
    }

    private final class PeerState {
        private final long registeredAtNanos;
        private final ArrivalWindow arrivals = new ArrivalWindow(config.ewmaAlpha());
        private long lastSeenAtNanos;
        private long incarnation;
        private long suspectAtNanos = -1;
        private boolean hasHeartbeat;

        private PeerState(long registeredAtNanos, long incarnation) {
            this.registeredAtNanos = registeredAtNanos;
            this.lastSeenAtNanos = registeredAtNanos;
            this.incarnation = incarnation;
        }
    }

    private static final class ArrivalWindow {
        private final double alpha;
        private int count;
        private double mean;
        private double variance;

        private ArrivalWindow(double alpha) {
            this.alpha = alpha;
        }

        private void add(long value) {
            if (count == 0) {
                mean = value;
                variance = 0;
            } else {
                double difference = value - mean;
                mean += alpha * difference;
                variance = (1.0 - alpha) * (variance + alpha * difference * difference);
            }
            count++;
        }

        private int size() {
            return count;
        }

        private double mean() {
            return mean;
        }

        private double standardDeviation() {
            return Math.sqrt(Math.max(0.0, variance));
        }
    }
}
