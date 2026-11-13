package com.openkache.failuredetector.deadline;

import com.openkache.failuredetector.core.AbstractFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;

import java.util.HashMap;
import java.util.Map;

/** Experimental fixed heartbeat deadline baseline, not a membership protocol. */
public final class FixedDeadlineFailureDetector extends AbstractFailureDetector {
    public static final String ALGORITHM = "fixed-deadline";

    private final FixedDeadlineConfig config;
    private final Map<String, PeerState> peers = new HashMap<>();

    public FixedDeadlineFailureDetector(String observer) {
        this(observer, FixedDeadlineConfig.defaults());
    }

    public FixedDeadlineFailureDetector(String observer, FixedDeadlineConfig config) {
        super(observer);
        this.config = config;
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }

    public FixedDeadlineConfig config() {
        return config;
    }

    public void registerTarget(long nowNanos, String target, long incarnation) {
        peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos, incarnation));
    }

    /** Stops observing a target after a membership-view reassignment. */
    public void unregisterTarget(String target) {
        peers.remove(target);
        clearStatus(target);
    }

    public boolean recordHeartbeat(long nowNanos, String target, long incarnation) {
        PeerState peer = peers.computeIfAbsent(
                target,
                ignored -> new PeerState(nowNanos, incarnation)
        );
        if (incarnation < peer.incarnation) {
            return false;
        }
        if (status(target) == NodeStatus.DEAD && incarnation == peer.incarnation) {
            return false;
        }
        peer.incarnation = incarnation;
        peer.lastSeenAtNanos = nowNanos;
        peer.suspectAtNanos = -1;
        peer.failedVerifications = 0;
        peer.verificationInFlight = false;
        if (status(target) != NodeStatus.ALIVE) {
            transition(
                    nowNanos,
                    target,
                    NodeStatus.ALIVE,
                    "heartbeat restored within allowed incarnation policy",
                    Map.of("incarnation", incarnation)
            );
        }
        return true;
    }

    @Override
    public void tick(long nowNanos) {
        for (Map.Entry<String, PeerState> entry : peers.entrySet()) {
            String target = entry.getKey();
            PeerState peer = entry.getValue();
            if (nowNanos - peer.registeredAtNanos < config.warmup().toNanos()) {
                continue;
            }
            if (status(target) == NodeStatus.ALIVE
                    && nowNanos - peer.lastSeenAtNanos >= config.deadline().toNanos()) {
                peer.suspectAtNanos = nowNanos;
                transition(
                        nowNanos,
                        target,
                        NodeStatus.SUSPECT,
                        "fixed heartbeat deadline expired",
                        Map.of(
                                "incarnation", peer.incarnation,
                                "lastSeenAtNanos", peer.lastSeenAtNanos,
                                "deadlineNanos", config.deadline().toNanos()
                        )
                );
            }
            if (status(target) == NodeStatus.SUSPECT
                    && !config.verifySuspectEnabled()
                    && peer.suspectAtNanos >= 0
                    && nowNanos - peer.suspectAtNanos >= config.confirm().toNanos()) {
                transition(
                        nowNanos,
                        target,
                        NodeStatus.DEAD,
                        "fixed heartbeat confirmation period expired",
                        Map.of("incarnation", peer.incarnation)
                );
            }
        }
    }

    public boolean beginVerification(String target) {
        PeerState peer = peers.get(target);
        if (!config.verifySuspectEnabled()
                || peer == null
                || status(target) != NodeStatus.SUSPECT
                || peer.verificationInFlight) {
            return false;
        }
        peer.verificationInFlight = true;
        return true;
    }

    /** Transport received no independent evidence. Keep suspicion and its failure count unchanged. */
    public void cancelVerification(String target) {
        PeerState peer = peers.get(target);
        if (peer != null) peer.verificationInFlight = false;
    }

    public void recordVerification(long nowNanos, String target, boolean reachable) {
        PeerState peer = peers.get(target);
        if (peer == null || !peer.verificationInFlight || status(target) != NodeStatus.SUSPECT) {
            return;
        }
        peer.verificationInFlight = false;
        if (reachable) {
            peer.lastSeenAtNanos = nowNanos;
            peer.suspectAtNanos = -1;
            peer.failedVerifications = 0;
            transition(
                    nowNanos,
                    target,
                    NodeStatus.ALIVE,
                    "independent verify-suspect observation succeeded",
                    Map.of("incarnation", peer.incarnation)
            );
            return;
        }
        peer.failedVerifications++;
        if (peer.failedVerifications >= config.failedVerificationsToDead()) {
            transition(
                    nowNanos,
                    target,
                    NodeStatus.DEAD,
                    "independent verify-suspect failure threshold reached",
                    Map.of(
                            "failures", peer.failedVerifications,
                            "required", config.failedVerificationsToDead(),
                            "incarnation", peer.incarnation
                    )
            );
        }
    }

    private static final class PeerState {
        private final long registeredAtNanos;
        private long lastSeenAtNanos;
        private long incarnation;
        private long suspectAtNanos = -1;
        private int failedVerifications;
        private boolean verificationInFlight;

        private PeerState(long registeredAtNanos, long incarnation) {
            this.registeredAtNanos = registeredAtNanos;
            this.lastSeenAtNanos = registeredAtNanos;
            this.incarnation = incarnation;
        }
    }
}
