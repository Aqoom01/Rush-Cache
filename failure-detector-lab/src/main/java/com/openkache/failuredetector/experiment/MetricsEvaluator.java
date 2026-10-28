package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.GroundTruthEvent;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.core.ProtocolCost;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

final class MetricsEvaluator {
    private MetricsEvaluator() {
    }

    static RunMetrics evaluate(
            long durationNanos,
            String target,
            List<DetectorEvent> detectorEvents,
            List<GroundTruthEvent> truthEvents,
            ProtocolCost cost
    ) {
        List<TimelineEvent> timeline = new ArrayList<>();
        for (GroundTruthEvent event : truthEvents) {
            timeline.add(TimelineEvent.truth(event));
        }
        for (DetectorEvent event : detectorEvents) {
            if (event.target().equals(target)) {
                timeline.add(TimelineEvent.detector(event));
            }
        }
        timeline.sort(Comparator
                .comparingLong(TimelineEvent::atNanos)
                .thenComparingInt(TimelineEvent::order));

        boolean actuallyAlive = true;
        boolean reachable = true;
        NodeStatus detectorStatus = NodeStatus.ALIVE;
        long lastAt = 0;
        long falseDuration = 0;
        long falseDeadDuration = 0;
        long falsePositiveEvents = 0;
        long falseDeadEvents = 0;
        long unreachableButAliveEvents = 0;
        long unreachableButAliveDuration = 0;
        long flapCount = 0;
        Long failureAt = null;
        Long suspicionAt = null;
        Long detectionAt = null;

        for (TimelineEvent item : timeline) {
            long at = Math.min(durationNanos, Math.max(0, item.atNanos()));
            if (actuallyAlive && detectorStatus != NodeStatus.ALIVE) {
                falseDuration += at - lastAt;
            }
            if (actuallyAlive && detectorStatus == NodeStatus.DEAD) {
                falseDeadDuration += at - lastAt;
            }
            if (actuallyAlive && !reachable && detectorStatus != NodeStatus.ALIVE) {
                unreachableButAliveDuration += at - lastAt;
            }
            lastAt = at;

            if (item.truth != null) {
                actuallyAlive = item.truth.alive();
                reachable = item.truth.reachable();
                if (!actuallyAlive && failureAt == null) {
                    failureAt = at;
                }
                if (actuallyAlive) {
                    failureAt = null;
                    suspicionAt = null;
                    detectionAt = null;
                }
            } else {
                NodeStatus previous = detectorStatus;
                detectorStatus = item.detector.newStatus();
                boolean previousFailed = previous != NodeStatus.ALIVE;
                boolean nowFailed = detectorStatus != NodeStatus.ALIVE;
                if (actuallyAlive && !previousFailed && nowFailed) {
                    falsePositiveEvents++;
                }
                if (actuallyAlive
                        && previous != NodeStatus.DEAD
                        && detectorStatus == NodeStatus.DEAD) {
                    falseDeadEvents++;
                }
                if (actuallyAlive && !reachable && !previousFailed && nowFailed) {
                    unreachableButAliveEvents++;
                }
                if (previousFailed != nowFailed) {
                    flapCount++;
                }
                if (!actuallyAlive
                        && detectorStatus != NodeStatus.ALIVE
                        && suspicionAt == null) {
                    suspicionAt = at;
                }
                if (!actuallyAlive
                        && detectorStatus == NodeStatus.DEAD
                        && detectionAt == null) {
                    detectionAt = at;
                }
            }
        }
        if (actuallyAlive && detectorStatus != NodeStatus.ALIVE) {
            falseDuration += durationNanos - lastAt;
        }
        if (actuallyAlive && detectorStatus == NodeStatus.DEAD) {
            falseDeadDuration += durationNanos - lastAt;
        }
        if (actuallyAlive && !reachable && detectorStatus != NodeStatus.ALIVE) {
            unreachableButAliveDuration += durationNanos - lastAt;
        }

        long falseNegative = failureAt != null && detectionAt == null ? 1 : 0;
        Long latency = failureAt != null && detectionAt != null
                ? Math.max(0L, detectionAt - failureAt)
                : null;
        Long suspicionLatency = failureAt != null && suspicionAt != null
                ? Math.max(0L, suspicionAt - failureAt)
                : null;
        return new RunMetrics(
                falsePositiveEvents,
                falseDuration,
                falseDeadEvents,
                falseDeadDuration,
                unreachableButAliveEvents,
                unreachableButAliveDuration,
                falseNegative,
                suspicionLatency,
                latency,
                flapCount,
                cost.messages(),
                cost.bytes(),
                actuallyAlive && detectorStatus != NodeStatus.ALIVE
        );
    }

    private static final class TimelineEvent {
        private final long atNanos;
        private final int order;
        private final GroundTruthEvent truth;
        private final DetectorEvent detector;

        private TimelineEvent(
                long atNanos,
                int order,
                GroundTruthEvent truth,
                DetectorEvent detector
        ) {
            this.atNanos = atNanos;
            this.order = order;
            this.truth = truth;
            this.detector = detector;
        }

        private static TimelineEvent truth(GroundTruthEvent event) {
            return new TimelineEvent(event.atNanos(), 0, event, null);
        }

        private static TimelineEvent detector(DetectorEvent event) {
            return new TimelineEvent(event.atNanos(), 1, null, event);
        }

        private long atNanos() {
            return atNanos;
        }

        private int order() {
            return order;
        }
    }
}
