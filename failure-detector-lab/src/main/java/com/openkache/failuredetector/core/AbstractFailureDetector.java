package com.openkache.failuredetector.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractFailureDetector implements FailureDetector {
    private final String observer;
    private final Map<String, NodeStatus> statuses = new HashMap<>();
    private final List<DetectorEvent> events = new ArrayList<>();

    protected AbstractFailureDetector(String observer) {
        if (observer == null || observer.isBlank()) {
            throw new IllegalArgumentException("observer must not be blank");
        }
        this.observer = observer;
    }

    @Override
    public final String observer() {
        return observer;
    }

    @Override
    public final NodeStatus status(String target) {
        return statuses.getOrDefault(target, NodeStatus.ALIVE);
    }

    protected final void transition(
            long nowNanos,
            String target,
            NodeStatus next,
            String reason,
            Map<String, Object> details
    ) {
        NodeStatus previous = status(target);
        if (previous == next) {
            return;
        }
        statuses.put(target, next);
        events.add(new DetectorEvent(
                nowNanos,
                observer,
                target,
                previous,
                next,
                reason,
                algorithm(),
                details
        ));
    }

    protected final void clearStatus(String target) {
        statuses.remove(target);
    }

    @Override
    public final List<DetectorEvent> drainEvents() {
        List<DetectorEvent> copy = List.copyOf(events);
        events.clear();
        return copy;
    }
}
