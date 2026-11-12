package com.openkache.failuredetector.product;

import java.util.Arrays;

public enum ProductAlgorithm {
    SWIM_LIFEGUARD("swim-lifeguard"),
    REDIS_CLUSTER("redis-cluster"),
    PHI_GOSSIP("phi-gossip"),
    IGNITE_TCP_RING("ignite-tcp-ring"),
    AEROSPIKE_MESH("aerospike-mesh");

    private final String id;

    ProductAlgorithm(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static ProductAlgorithm fromId(String value) {
        return Arrays.stream(values())
                .filter(algorithm -> algorithm.id.equalsIgnoreCase(value.trim()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown algorithm: " + value));
    }
}
