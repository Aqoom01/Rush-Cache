package com.openkache.failuredetector.experiment;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Monotonic single-threaded scheduler used by the discrete-event experiments.
 * Events at the same timestamp are ordered by phase and then insertion id.
 */
public final class DeterministicEventScheduler {
    public enum Phase {
        FAULT(0),
        DELIVERY(1),
        TIMEOUT(2),
        SEND(3),
        TICK(4);

        private final int order;

        Phase(int order) {
            this.order = order;
        }
    }

    private final PriorityQueue<ScheduledEvent> events = new PriorityQueue<>(Comparator
            .comparingLong(ScheduledEvent::atNanos)
            .thenComparingInt(event -> event.phase().order)
            .thenComparingLong(ScheduledEvent::id));
    private long nextId;
    private long nowNanos;

    public void schedule(long atNanos, Phase phase, Runnable action) {
        if (atNanos < nowNanos) {
            throw new IllegalArgumentException("cannot schedule an event in the past");
        }
        events.add(new ScheduledEvent(atNanos, phase, nextId++, action));
    }

    public void runUntil(long endNanos) {
        if (endNanos < nowNanos) {
            throw new IllegalArgumentException("time must be monotonic");
        }
        while (!events.isEmpty() && events.peek().atNanos() <= endNanos) {
            ScheduledEvent event = events.remove();
            nowNanos = event.atNanos();
            event.action().run();
        }
        nowNanos = endNanos;
    }

    public long nowNanos() {
        return nowNanos;
    }

    private record ScheduledEvent(long atNanos, Phase phase, long id, Runnable action) {
    }
}
