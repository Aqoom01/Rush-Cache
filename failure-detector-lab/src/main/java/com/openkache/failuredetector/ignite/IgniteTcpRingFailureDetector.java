package com.openkache.failuredetector.ignite;

import com.openkache.failuredetector.core.AbstractFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Ignite 2 TCP Discovery-inspired logical-ring detector and topology view. */
public final class IgniteTcpRingFailureDetector extends AbstractFailureDetector {
    public static final String ALGORITHM = "ignite-tcp-ring";

    private final IgniteTcpRingConfig config;
    private final String coordinator;
    private final LinkedHashSet<String> members;
    private final Map<String, Long> lastSeen = new HashMap<>();
    private long viewVersion;

    public IgniteTcpRingFailureDetector(
            String observer,
            String coordinator,
            List<String> initialMembers,
            IgniteTcpRingConfig config
    ) {
        super(observer);
        if (initialMembers == null || initialMembers.size() < 2
                || !initialMembers.contains(observer) || !initialMembers.contains(coordinator)) {
            throw new IllegalArgumentException("initialMembers must contain observer and coordinator");
        }
        this.coordinator = coordinator;
        this.members = new LinkedHashSet<>(initialMembers);
        this.config = config;
        for (String member : initialMembers) {
            lastSeen.put(member, 0L);
        }
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }

    public void recordNeighborMessage(long nowNanos, String sender) {
        if (!members.contains(sender)) {
            return;
        }
        lastSeen.put(sender, nowNanos);
        if (status(sender) == NodeStatus.SUSPECT) {
            transition(nowNanos, sender, NodeStatus.ALIVE, "TCP ring neighbor reconnected", Map.of());
        }
    }

    @Override
    public void tick(long nowNanos) {
        String neighbor = nextAliveNeighbor(observer());
        if (neighbor == null) {
            return;
        }
        long seenAt = lastSeen.getOrDefault(neighbor, 0L);
        if (status(neighbor) == NodeStatus.ALIVE
                && nowNanos - seenAt >= config.neighborTimeout().toNanos()) {
            transition(
                    nowNanos,
                    neighbor,
                    NodeStatus.SUSPECT,
                    "TCP connection to logical-ring neighbor timed out",
                    Map.of("viewVersion", viewVersion, "coordinator", coordinator)
            );
        }
    }

    public boolean coordinatorConfirmFailure(long nowNanos, String reporter, String target) {
        if (!observer().equals(coordinator)
                || !members.contains(reporter)
                || !members.contains(target)
                || reporter.equals(coordinator)
                || reporter.equals(target)
                || status(target) != NodeStatus.SUSPECT) {
            return false;
        }
        members.remove(target);
        viewVersion++;
        transition(
                nowNanos,
                target,
                NodeStatus.DEAD,
                "coordinator validated failure and advanced topology",
                Map.of(
                        "viewVersion", viewVersion,
                        "reporter", reporter,
                        "nextAliveNeighbor", String.valueOf(nextAliveNeighbor(reporter))
                )
        );
        return true;
    }

    /**
     * Import an actual neighbor timeout after the coordinator independently failed to reach the target.
     * The caller supplies that transport evidence; this entry does not elect a new coordinator.
     */
    public boolean recordCoordinatorValidation(long nowNanos, String reporter, String target) {
        if (!observer().equals(coordinator) || !members.contains(reporter) || !members.contains(target)
                || reporter.equals(coordinator) || reporter.equals(target) || target.equals(coordinator)
                || !target.equals(nextAliveNeighbor(reporter))) return false;
        if (status(target) == NodeStatus.ALIVE) transition(nowNanos, target, NodeStatus.SUSPECT,
                "reported neighbor timeout independently validated by coordinator", Map.of("reporter", reporter));
        return status(target) == NodeStatus.SUSPECT;
    }

    public boolean applyTopologyUpdate(long version, List<String> updatedMembers) {
        if (version <= viewVersion || updatedMembers == null || updatedMembers.isEmpty()) {
            return false;
        }
        members.clear();
        members.addAll(updatedMembers);
        viewVersion = version;
        return true;
    }

    public String nextAliveNeighbor(String node) {
        List<String> ordered = new ArrayList<>(members);
        int start = ordered.indexOf(node);
        if (start < 0 || ordered.size() < 2) {
            return null;
        }
        for (int offset = 1; offset < ordered.size(); offset++) {
            String candidate = ordered.get((start + offset) % ordered.size());
            if (status(candidate) != NodeStatus.DEAD) {
                return candidate;
            }
        }
        return null;
    }

    public long viewVersion() {
        return viewVersion;
    }

    public Set<String> members() {
        return Set.copyOf(members);
    }
}
