package com.openkache.failuredetector.redis;

import com.openkache.failuredetector.core.AbstractFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Redis Cluster-inspired PFAIL/FAIL detector. Replica election is deliberately
 * outside this class: FAIL is only the failure-detector result that can trigger it.
 */
public final class RedisClusterFailureDetector extends AbstractFailureDetector {
    public static final String ALGORITHM = "redis-cluster";

    private final RedisClusterConfig config;
    private final Set<String> masters;
    private final Map<String, PeerState> peers = new HashMap<>();

    public RedisClusterFailureDetector(
            String observer,
            Set<String> masters,
            RedisClusterConfig config
    ) {
        super(observer);
        if (masters == null || masters.isEmpty() || !masters.contains(observer)) {
            throw new IllegalArgumentException("masters must contain the observer");
        }
        this.masters = Set.copyOf(masters);
        this.config = config;
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }

    public void registerTarget(long nowNanos, String target) {
        peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos));
    }

    public void recordPingSent(long nowNanos, String target) {
        PeerState peer = peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos));
        if (peer.pendingPingAtNanos < 0) {
            peer.pendingPingAtNanos = nowNanos;
        }
    }

    public void recordPong(long nowNanos, String target) {
        PeerState peer = peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos));
        peer.lastPongAtNanos = nowNanos;
        peer.pendingPingAtNanos = -1;
        if (status(target) == NodeStatus.SUSPECT) {
            peer.reports.clear();
            transition(nowNanos, target, NodeStatus.ALIVE, "PONG cleared local PFAIL", Map.of());
        }
        // Redis FAIL is intentionally not cleared merely by a PONG from a slot-serving master.
    }

    public boolean recordFailureReport(
            long nowNanos,
            String target,
            String reportingMaster
    ) {
        if (!masters.contains(reportingMaster)) {
            return false;
        }
        PeerState peer = peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos));
        expireReports(nowNanos, peer);
        peer.reports.put(reportingMaster, nowNanos);
        confirmFailIfQuorum(nowNanos, target, peer);
        return true;
    }

    public void recordFailMessage(long nowNanos, String target, String sender) {
        if (!masters.contains(sender)) {
            return;
        }
        peers.computeIfAbsent(target, ignored -> new PeerState(nowNanos));
        if (status(target) != NodeStatus.DEAD) {
            transition(
                    nowNanos,
                    target,
                    NodeStatus.DEAD,
                    "received Redis Cluster FAIL message",
                    Map.of("sender", sender)
            );
        }
    }

    public int freshReportCount(long nowNanos, String target) {
        PeerState peer = peers.get(target);
        if (peer == null) {
            return 0;
        }
        expireReports(nowNanos, peer);
        return peer.reports.size();
    }

    public int requiredReports() {
        return masters.size() / 2 + 1;
    }

    @Override
    public void tick(long nowNanos) {
        for (Map.Entry<String, PeerState> entry : peers.entrySet()) {
            String target = entry.getKey();
            PeerState peer = entry.getValue();
            expireReports(nowNanos, peer);
            if (status(target) == NodeStatus.ALIVE
                    && peer.pendingPingAtNanos >= 0
                    && nowNanos - peer.pendingPingAtNanos >= config.nodeTimeout().toNanos()) {
                transition(
                        nowNanos,
                        target,
                        NodeStatus.SUSPECT,
                        "pending PING exceeded NODE_TIMEOUT (PFAIL)",
                        Map.of(
                                "pendingPingAtNanos", peer.pendingPingAtNanos,
                                "nodeTimeoutNanos", config.nodeTimeout().toNanos()
                        )
                );
                peer.reports.put(observer(), nowNanos);
                confirmFailIfQuorum(nowNanos, target, peer);
            }
        }
    }

    private void confirmFailIfQuorum(long nowNanos, String target, PeerState peer) {
        if (status(target) != NodeStatus.SUSPECT || peer.reports.size() < requiredReports()) {
            return;
        }
        transition(
                nowNanos,
                target,
                NodeStatus.DEAD,
                "fresh master failure reports confirmed FAIL",
                Map.of(
                        "freshReports", peer.reports.size(),
                        "requiredReports", requiredReports(),
                        "validityNanos", config.failureReportValidity().toNanos()
                )
        );
    }

    private void expireReports(long nowNanos, PeerState peer) {
        long cutoff = nowNanos - config.failureReportValidity().toNanos();
        peer.reports.entrySet().removeIf(entry -> entry.getValue() < cutoff);
    }

    public Set<String> reportingMasters(long nowNanos, String target) {
        freshReportCount(nowNanos, target);
        PeerState peer = peers.get(target);
        return peer == null ? Set.of() : Set.copyOf(new HashSet<>(peer.reports.keySet()));
    }

    private static final class PeerState {
        private long lastPongAtNanos;
        private long pendingPingAtNanos = -1;
        private final Map<String, Long> reports = new HashMap<>();

        private PeerState(long nowNanos) {
            lastPongAtNanos = nowNanos;
        }
    }
}
