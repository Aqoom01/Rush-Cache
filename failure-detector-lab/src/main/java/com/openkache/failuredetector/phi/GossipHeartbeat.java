package com.openkache.failuredetector.phi;

public record GossipHeartbeat(
        String nodeId,
        long generation,
        long version,
        long observedAtNanos
) {
}
