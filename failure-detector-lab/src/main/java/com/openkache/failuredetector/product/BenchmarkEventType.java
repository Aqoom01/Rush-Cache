package com.openkache.failuredetector.product;

public enum BenchmarkEventType {
    FAULT_INJECTED,
    LOCAL_SUSPECTED,
    LOCAL_UNAVAILABLE,
    CLUSTER_CONFIRMED,
    MEMBERSHIP_CHANGED,
    MEMBERSHIP_CONVERGED,
    RECOVERED
}
