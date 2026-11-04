package com.openkache.failuredetector.redis;

import java.time.Duration;

public record RedisClusterConfig(Duration nodeTimeout, Duration failureReportValidity) {
    public RedisClusterConfig {
        if (nodeTimeout.isZero() || nodeTimeout.isNegative()) {
            throw new IllegalArgumentException("nodeTimeout must be positive");
        }
        if (failureReportValidity.compareTo(nodeTimeout) < 0) {
            throw new IllegalArgumentException("failureReportValidity must be >= nodeTimeout");
        }
    }

    public static RedisClusterConfig defaults() {
        return new RedisClusterConfig(Duration.ofSeconds(15), Duration.ofSeconds(30));
    }
}
