package com.openkache.failuredetector.health;

import com.openkache.failuredetector.core.NodeStatus;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class HealthMonitor {
    private final Duration latencyLimit;
    private final int failuresToDegrade;
    private final int successesToRecover;
    private final Map<Key, State> states = new HashMap<>();

    public HealthMonitor(Duration latencyLimit, int failuresToDegrade, int successesToRecover) {
        if (latencyLimit == null || latencyLimit.isNegative() || latencyLimit.isZero()) {
            throw new IllegalArgumentException("latencyLimit must be positive");
        }
        if (failuresToDegrade < 1 || successesToRecover < 1) {
            throw new IllegalArgumentException("consecutive thresholds must be positive");
        }
        this.latencyLimit = latencyLimit;
        this.failuresToDegrade = failuresToDegrade;
        this.successesToRecover = successesToRecover;
    }

    public Optional<HealthTransition> record(
            long nowNanos,
            String target,
            Component component,
            boolean success,
            long latencyNanos,
            String reason
    ) {
        Key key = new Key(target, component);
        State state = states.computeIfAbsent(key, ignored -> new State());
        boolean healthySample = success && latencyNanos <= latencyLimit.toNanos();

        if (healthySample) {
            state.consecutiveFailures = 0;
            state.consecutiveSuccesses++;
            if (state.status == NodeStatus.HEALTH_DEGRADED
                    && state.consecutiveSuccesses >= successesToRecover) {
                NodeStatus previous = state.status;
                state.status = NodeStatus.ALIVE;
                return Optional.of(new HealthTransition(
                        nowNanos, target, component, previous, state.status,
                        "health probe recovered: " + reason
                ));
            }
            return Optional.empty();
        }

        state.consecutiveSuccesses = 0;
        state.consecutiveFailures++;
        if (state.status != NodeStatus.HEALTH_DEGRADED
                && state.consecutiveFailures >= failuresToDegrade) {
            NodeStatus previous = state.status;
            state.status = NodeStatus.HEALTH_DEGRADED;
            return Optional.of(new HealthTransition(
                    nowNanos, target, component, previous, state.status,
                    "health probe failed: " + reason
            ));
        }
        return Optional.empty();
    }

    public NodeStatus status(String target, Component component) {
        State state = states.get(new Key(target, component));
        return state == null ? NodeStatus.ALIVE : state.status;
    }

    public enum Component {
        DATA_SERVICE,
        DISK
    }

    public record HealthTransition(
            long atNanos,
            String target,
            Component component,
            NodeStatus oldStatus,
            NodeStatus newStatus,
            String reason
    ) {
    }

    private record Key(String target, Component component) {
    }

    private static final class State {
        private NodeStatus status = NodeStatus.ALIVE;
        private int consecutiveFailures;
        private int consecutiveSuccesses;
    }
}
