package com.openkache.failuredetector.modules;

import java.util.Set;

public enum ObservationKind {
    ALL_TO_ALL_HEARTBEAT(Set.of(
            Capability.PERIODIC_HEARTBEAT,
            Capability.MONOTONIC_INTERARRIVAL_SERIES
    )),
    GOSSIP_HEARTBEAT(Set.of(
            Capability.PERIODIC_HEARTBEAT,
            Capability.MONOTONIC_INTERARRIVAL_SERIES,
            Capability.GOSSIP_DISSEMINATION
    )),
    SWIM_RANDOM_PROBE(Set.of(
            Capability.DIRECT_INDIRECT_PROBE,
            Capability.MEMBERSHIP_INCARNATION,
            Capability.MONOTONIC_PROCESS_LOCAL_HEALTH,
            Capability.GOSSIP_DISSEMINATION
    )),
    RING_NEIGHBOR_HEARTBEAT(Set.of(
            Capability.PERIODIC_HEARTBEAT,
            Capability.MONOTONIC_INTERARRIVAL_SERIES,
            Capability.RING_MONITOR_ASSIGNMENT,
            Capability.VIEW_UPDATE,
            Capability.INDEPENDENT_VERIFY_PATH
    ));

    private final Set<Capability> capabilities;

    ObservationKind(Set<Capability> capabilities) {
        this.capabilities = Set.copyOf(capabilities);
    }

    public Set<Capability> capabilities() {
        return capabilities;
    }
}
