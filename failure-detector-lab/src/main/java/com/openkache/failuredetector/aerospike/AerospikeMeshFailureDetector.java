package com.openkache.failuredetector.aerospike;

import com.openkache.failuredetector.core.AbstractFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Aerospike-inspired TCP mesh heartbeat plus an explicit, simplified membership
 * agreement adapter. The proposal/ack stage is not claimed to reproduce Paxos.
 */
public final class AerospikeMeshFailureDetector extends AbstractFailureDetector {
    public static final String ALGORITHM = "aerospike-mesh";

    private final AerospikeMeshConfig config;
    private final LinkedHashSet<String> members;
    private final Map<String, Long> lastHeartbeat = new HashMap<>();
    private final Map<Long, Proposal> proposals = new HashMap<>();
    private long viewVersion;

    public AerospikeMeshFailureDetector(
            String observer,
            Set<String> initialMembers,
            AerospikeMeshConfig config
    ) {
        super(observer);
        if (initialMembers == null || initialMembers.size() < 2
                || !initialMembers.contains(observer)) {
            throw new IllegalArgumentException("initialMembers must contain observer");
        }
        this.members = new LinkedHashSet<>(initialMembers);
        this.config = config;
        for (String member : members) {
            lastHeartbeat.put(member, 0L);
        }
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }

    public void recordHeartbeat(long nowNanos, String target) {
        if (!members.contains(target)) {
            return;
        }
        lastHeartbeat.put(target, nowNanos);
        if (status(target) == NodeStatus.SUSPECT) {
            transition(nowNanos, target, NodeStatus.ALIVE, "mesh heartbeat restored", Map.of());
        }
    }

    @Override
    public void tick(long nowNanos) {
        for (String target : Set.copyOf(members)) {
            if (target.equals(observer())) {
                continue;
            }
            long seenAt = lastHeartbeat.getOrDefault(target, 0L);
            if (status(target) == NodeStatus.ALIVE
                    && nowNanos - seenAt >= config.heartbeatTimeout().toNanos()) {
                transition(
                        nowNanos,
                        target,
                        NodeStatus.SUSPECT,
                        "TCP mesh heartbeat timeout",
                        Map.of("viewVersion", viewVersion)
                );
            }
        }
    }

    public boolean proposeMembershipRemoval(long epoch, String target) {
        if (epoch <= viewVersion || status(target) != NodeStatus.SUSPECT || !members.contains(target)) {
            return false;
        }
        proposals.putIfAbsent(epoch, new Proposal(target));
        proposals.get(epoch).acknowledgements.add(observer());
        return true;
    }

    public boolean acknowledgeMembershipProposal(
            long nowNanos,
            long epoch,
            String voter
    ) {
        Proposal proposal = proposals.get(epoch);
        if (proposal == null || !members.contains(voter) || epoch <= viewVersion) {
            return false;
        }
        proposal.acknowledgements.add(voter);
        int eligible = members.size() - 1;
        int required = eligible / 2 + 1;
        if (proposal.acknowledgements.size() >= required) {
            members.remove(proposal.target);
            viewVersion = epoch;
            transition(
                    nowNanos,
                    proposal.target,
                    NodeStatus.DEAD,
                    "simplified membership agreement committed removal",
                    Map.of(
                            "viewVersion", viewVersion,
                            "acknowledgements", proposal.acknowledgements.size(),
                            "required", required,
                            "agreementModel", "inspired-epoch-majority-not-paxos"
                    )
            );
            proposals.entrySet().removeIf(entry -> entry.getKey() <= viewVersion);
        }
        return true;
    }

    public long viewVersion() {
        return viewVersion;
    }

    public Set<String> members() {
        return Set.copyOf(members);
    }

    private static final class Proposal {
        private final String target;
        private final Set<String> acknowledgements = new HashSet<>();

        private Proposal(String target) {
            this.target = target;
        }
    }
}
