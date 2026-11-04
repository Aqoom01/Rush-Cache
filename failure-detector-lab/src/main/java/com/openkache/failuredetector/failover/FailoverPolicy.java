package com.openkache.failuredetector.failover;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A safety gate inspired by Valkey's separation of local PFAIL evidence from
 * FAIL and replica election. It does not perform promotion itself.
 */
public final class FailoverPolicy {
    private final Config config;
    private final AtomicLong currentEpoch;
    private final Map<String, List<FailureReport>> reports = new HashMap<>();
    private final Map<String, Long> lastGrantedEpochByVoter = new HashMap<>();

    public FailoverPolicy(Config config, long initialEpoch) {
        this.config = config;
        this.currentEpoch = new AtomicLong(initialEpoch);
    }

    public void report(FailureReport report) {
        reports.computeIfAbsent(report.failedPrimary(), ignored -> new ArrayList<>()).add(report);
    }

    public Decision evaluate(
            long nowNanos,
            String failedPrimary,
            ReplicaCandidate candidate,
            Set<String> reachableVoters
    ) {
        Set<String> freshReporters = new HashSet<>();
        long validAfter = nowNanos - config.reportValidity().toNanos();
        for (FailureReport report : reports.getOrDefault(failedPrimary, List.of())) {
            if (report.reportedAtNanos() >= validAfter && report.reportedAtNanos() <= nowNanos) {
                freshReporters.add(report.reporter());
            }
        }

        int quorum = config.votingNodes() / 2 + 1;
        if (reachableVoters.size() < quorum) {
            return Decision.rejected("reachable voters are a minority", currentEpoch.get());
        }
        if (freshReporters.size() < quorum) {
            return Decision.rejected("failure evidence has not reached quorum", currentEpoch.get());
        }
        if (candidate.replicationLagBytes() > config.maxReplicationLagBytes()) {
            return Decision.rejected("replica is too stale", currentEpoch.get());
        }
        if (!candidate.healthy()) {
            return Decision.rejected("replica health check failed", currentEpoch.get());
        }

        long electionEpoch = currentEpoch.incrementAndGet();
        int votes = 0;
        for (String voter : reachableVoters.stream().sorted().toList()) {
            long lastGranted = lastGrantedEpochByVoter.getOrDefault(voter, -1L);
            if (electionEpoch > lastGranted) {
                lastGrantedEpochByVoter.put(voter, electionEpoch);
                votes++;
            }
        }
        if (votes < quorum) {
            return Decision.rejected("election did not receive quorum", electionEpoch);
        }
        return new Decision(true, "promotion authorized", electionEpoch, votes);
    }

    public record Config(
            int votingNodes,
            Duration reportValidity,
            long maxReplicationLagBytes
    ) {
        public Config {
            if (votingNodes < 1) {
                throw new IllegalArgumentException("votingNodes must be positive");
            }
            if (reportValidity == null || reportValidity.isZero() || reportValidity.isNegative()) {
                throw new IllegalArgumentException("reportValidity must be positive");
            }
            if (maxReplicationLagBytes < 0) {
                throw new IllegalArgumentException("maxReplicationLagBytes cannot be negative");
            }
        }
    }

    public record FailureReport(
            String reporter,
            String failedPrimary,
            long reportedAtNanos
    ) {
    }

    public record ReplicaCandidate(
            String nodeId,
            long replicationLagBytes,
            boolean healthy
    ) {
    }

    public record Decision(
            boolean authorized,
            String reason,
            long epoch,
            int votes
    ) {
        private static Decision rejected(String reason, long epoch) {
            return new Decision(false, reason, epoch, 0);
        }
    }
}
