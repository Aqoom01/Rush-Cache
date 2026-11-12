package com.openkache.failuredetector.core;

import java.util.List;

public interface FailureDetector {
    String algorithm();

    String observer();

    NodeStatus status(String target);

    void tick(long nowNanos);

    List<DetectorEvent> drainEvents();
}
