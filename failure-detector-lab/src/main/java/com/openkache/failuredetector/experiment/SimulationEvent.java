package com.openkache.failuredetector.experiment;

import java.util.Map;

/** One observed event from the simulation harness, separate from detector decisions. */
public record SimulationEvent(
        long atNanos,
        Type type,
        String observer,
        String target,
        String reason,
        Map<String, Object> details
) {
    public SimulationEvent {
        details = Map.copyOf(details);
    }

    public enum Type {
        FAULT_INJECTED,
        FAULT_CLEARED,
        PROBE_SENT,
        DIRECT_TIMEOUT,
        INDIRECT_STARTED,
        INDIRECT_TIMEOUT,
        SUSPICION_DEADLINE_SCHEDULED,
        HEARTBEAT_SENT,
        VERIFICATION_STARTED,
        VERIFICATION_TIMEOUT,
        PACKET_DELIVERED,
        PACKET_DROPPED,
        SIMULATION_ENDED
    }
}
