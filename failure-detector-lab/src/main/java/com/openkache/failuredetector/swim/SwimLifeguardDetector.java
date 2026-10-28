package com.openkache.failuredetector.swim;

import com.openkache.failuredetector.core.AbstractFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class SwimLifeguardDetector extends AbstractFailureDetector {
    public static final String ALGORITHM = "swim-lifeguard";

    private final int clusterSize;
    private final SwimLifeguardConfig config;
    private final Map<String, PeerState> peers = new HashMap<>();
    private int awarenessScore;

    public SwimLifeguardDetector(String observer, int clusterSize) {
        this(observer, clusterSize, SwimLifeguardConfig.defaults());
    }

    public SwimLifeguardDetector(
            String observer,
            int clusterSize,
            SwimLifeguardConfig config
    ) {
        super(observer);
        if (clusterSize < 2) {
            throw new IllegalArgumentException("clusterSize must be at least two");
        }
        this.clusterSize = clusterSize;
        this.config = config;
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }

    public SwimLifeguardConfig config() {
        return config;
    }

    public int awarenessScore() {
        return awarenessScore;
    }

    public int awarenessMultiplier() {
        return config.lhaProbeEnabled() ? awarenessScore + 1 : 1;
    }

    public int suspicionAwarenessMultiplier() {
        return config.lhaSuspicionEnabled() ? awarenessScore + 1 : 1;
    }

    public long effectiveProbeIntervalNanos() {
        return Math.multiplyExact(config.probeInterval().toNanos(), awarenessMultiplier());
    }

    public long effectiveProbeTimeoutNanos() {
        return Math.multiplyExact(config.probeTimeout().toNanos(), awarenessMultiplier());
    }

    public void observeLocalDelay(long elapsedNanos) {
        if (!config.localHealthAwarenessEnabled()) {
            return;
        }
        long expected = config.probeInterval().toNanos();
        if (elapsedNanos > expected + expected / 2) {
            int missed = (int) Math.max(1, elapsedNanos / expected - 1);
            awarenessScore = Math.min(config.awarenessMaxMultiplier(), awarenessScore + missed);
        }
    }

    /**
     * Records the final SWIM probe result after direct and, when necessary,
     * indirect checks have completed.
     */
    public void recordProbe(long nowNanos, String target, boolean success, long incarnation) {
        PeerState peer = peer(target);
        if (incarnation < peer.incarnation) {
            return;
        }

        if (success) {
            peer.incarnation = incarnation;
            peer.lastSuccessAtNanos = nowNanos;
            improveLocalHealth();
            if (status(target) != NodeStatus.ALIVE) {
                peer.clearSuspicion();
                transition(
                        nowNanos,
                        target,
                        NodeStatus.ALIVE,
                        "probe succeeded or suspicion was refuted",
                        Map.of(
                                "incarnation", incarnation,
                                "awarenessScore", awarenessScore
                        )
                );
            }
            return;
        }

        degradeLocalHealth();
        suspect(nowNanos, target, observer(), incarnation);
    }

    public void suspect(
            long nowNanos,
            String target,
            String reporter,
            long incarnation
    ) {
        PeerState peer = peer(target);
        if (incarnation < peer.incarnation || status(target) == NodeStatus.DEAD) {
            return;
        }
        peer.incarnation = Math.max(peer.incarnation, incarnation);

        if (status(target) != NodeStatus.SUSPECT) {
            peer.suspicionStartedAtNanos = nowNanos;
            peer.confirmations.clear();
            peer.confirmations.add(reporter);
            peer.suspicionDeadlineNanos = nowNanos + suspicionTimeoutNanos(0);
            transition(
                    nowNanos,
                    target,
                    NodeStatus.SUSPECT,
                    "direct and indirect probes failed",
                    Map.of(
                            "incarnation", peer.incarnation,
                            "awarenessScore", awarenessScore,
                            "suspicionDeadlineNanos", peer.suspicionDeadlineNanos
                    )
            );
            return;
        }
        confirmSuspicion(nowNanos, target, reporter);
    }

    public void confirmSuspicion(long nowNanos, String target, String reporter) {
        PeerState peer = peer(target);
        if (!config.suspicionConfirmationsEnabled()
                || status(target) != NodeStatus.SUSPECT
                || !peer.confirmations.add(reporter)) {
            return;
        }
        int confirmations = Math.min(
                Math.max(0, peer.confirmations.size() - 1),
                config.maxConfirmations()
        );
        long newDeadline = peer.suspicionStartedAtNanos + suspicionTimeoutNanos(confirmations);
        peer.suspicionDeadlineNanos = Math.min(
                peer.suspicionDeadlineNanos,
                Math.max(nowNanos, newDeadline)
        );
    }

    public long refute(long nowNanos, String target, long incarnation) {
        PeerState peer = peer(target);
        long nextIncarnation = Math.max(incarnation, peer.incarnation + 1);
        recordProbe(nowNanos, target, true, nextIncarnation);
        return nextIncarnation;
    }

    public long suspicionDeadlineNanos(String target) {
        PeerState peer = peers.get(target);
        return peer == null ? 0 : peer.suspicionDeadlineNanos;
    }

    @Override
    public void tick(long nowNanos) {
        for (Map.Entry<String, PeerState> entry : peers.entrySet()) {
            String target = entry.getKey();
            PeerState peer = entry.getValue();
            if (status(target) == NodeStatus.SUSPECT
                    && peer.suspicionDeadlineNanos > 0
                    && nowNanos >= peer.suspicionDeadlineNanos) {
                transition(
                        nowNanos,
                        target,
                        NodeStatus.DEAD,
                        "suspicion timeout expired",
                        Map.of(
                                "incarnation", peer.incarnation,
                                "confirmations", peer.confirmations.size(),
                                "awarenessScore", awarenessScore
                        )
                );
                peer.suspicionDeadlineNanos = 0;
            }
        }
    }

    long suspicionTimeoutNanos(int confirmations) {
        double minimum = config.suspicionMultiplier()
                * Math.log(clusterSize + 1.0)
                * config.probeInterval().toNanos()
                * suspicionAwarenessMultiplier();
        // Vanilla SWIM has one fixed suspicion timeout. The max-to-min range is
        // a Lifeguard extension that only has meaning when independent
        // confirmations are enabled. Keeping the range while disabling
        // confirmations strands the base profile at the maximum forever.
        if (!config.suspicionConfirmationsEnabled()) {
            return (long) minimum;
        }
        double maximum = minimum * config.suspicionMaxTimeoutMultiplier();
        if (config.maxConfirmations() == 0) {
            return (long) minimum;
        }
        int bounded = Math.min(Math.max(confirmations, 0), config.maxConfirmations());
        double fraction = Math.log(bounded + 1.0) / Math.log(config.maxConfirmations() + 1.0);
        return (long) (maximum - (maximum - minimum) * fraction);
    }

    private PeerState peer(String target) {
        return peers.computeIfAbsent(target, ignored -> new PeerState());
    }

    private void degradeLocalHealth() {
        if (config.localHealthAwarenessEnabled()) {
            awarenessScore = Math.min(config.awarenessMaxMultiplier(), awarenessScore + 1);
        }
    }

    private void improveLocalHealth() {
        if (config.localHealthAwarenessEnabled()) {
            awarenessScore = Math.max(0, awarenessScore - 1);
        }
    }

    private static final class PeerState {
        private long incarnation;
        private long lastSuccessAtNanos;
        private long suspicionStartedAtNanos;
        private long suspicionDeadlineNanos;
        private final Set<String> confirmations = new HashSet<>();

        private void clearSuspicion() {
            suspicionStartedAtNanos = 0;
            suspicionDeadlineNanos = 0;
            confirmations.clear();
        }
    }
}
