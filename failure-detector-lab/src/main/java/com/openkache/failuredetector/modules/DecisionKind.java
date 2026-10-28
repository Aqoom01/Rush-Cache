package com.openkache.failuredetector.modules;

import java.util.Set;

public enum DecisionKind {
    FIXED_DEADLINE(Set.of(Capability.PERIODIC_HEARTBEAT)),
    PHI_ACCRUAL(Set.of(Capability.MONOTONIC_INTERARRIVAL_SERIES)),
    SWIM_SUSPICION(Set.of(
            Capability.DIRECT_INDIRECT_PROBE,
            Capability.MEMBERSHIP_INCARNATION
    )),
    ADAPTIVE_INTERARRIVAL(Set.of(Capability.MONOTONIC_INTERARRIVAL_SERIES));

    private final Set<Capability> requiredCapabilities;

    DecisionKind(Set<Capability> requiredCapabilities) {
        this.requiredCapabilities = Set.copyOf(requiredCapabilities);
    }

    public Set<Capability> requiredCapabilities() {
        return requiredCapabilities;
    }
}
