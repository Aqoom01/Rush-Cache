package com.openkache.failuredetector.phi;

import com.openkache.failuredetector.core.AbstractFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Cassandra-style Phi Accrual detector.
 *
 * Cassandra's current implementation calculates an unscaled phi as elapsed /
 * mean and multiplies it by 1/log(10) before comparing with the configured
 * threshold. Oversized inter-arrival samples are ignored so a long partition
 * does not permanently make the detector insensitive.
 */
public final class PhiAccrualFailureDetector extends AbstractFailureDetector {
    public static final String ALGORITHM = "phi-gossip";
    private static final double PHI_FACTOR = 1.0 / Math.log(10.0);

    private final PhiAccrualConfig config;
    private final String algorithm;
    private final Map<String, ArrivalWindow> windows = new HashMap<>();
    private final Map<String, GossipHeartbeat> latestHeartbeats = new HashMap<>();
    private long lastTickNanos = -1;
    private long pauseGuardUntilNanos = -1;

    public PhiAccrualFailureDetector(String observer) {
        this(observer, PhiAccrualConfig.defaults());
    }

    public PhiAccrualFailureDetector(String observer, PhiAccrualConfig config) {
        this(observer, config, ALGORITHM);
    }

    public PhiAccrualFailureDetector(
            String observer,
            PhiAccrualConfig config,
            String algorithm
    ) {
        super(observer);
        if (algorithm == null || algorithm.isBlank()) {
            throw new IllegalArgumentException("algorithm must not be blank");
        }
        this.config = config;
        this.algorithm = algorithm;
    }

    @Override
    public String algorithm() {
        return algorithm;
    }

    public PhiAccrualConfig config() {
        return config;
    }

    public boolean recordHeartbeat(GossipHeartbeat heartbeat, boolean channelReachable) {
        if (heartbeat.nodeId().equals(observer())) {
            return false;
        }
        GossipHeartbeat current = latestHeartbeats.get(heartbeat.nodeId());
        if (current != null && compare(heartbeat, current) <= 0) {
            return false;
        }
        latestHeartbeats.put(heartbeat.nodeId(), heartbeat);
        ArrivalWindow window = windows.computeIfAbsent(
                heartbeat.nodeId(),
                ignored -> new ArrivalWindow(config)
        );
        window.add(heartbeat.observedAtNanos());

        if (channelReachable && status(heartbeat.nodeId()) != NodeStatus.ALIVE) {
            transition(
                    heartbeat.observedAtNanos(),
                    heartbeat.nodeId(),
                    NodeStatus.ALIVE,
                    "new heartbeat and messaging channel are reachable",
                    Map.of(
                            "generation", heartbeat.generation(),
                            "version", heartbeat.version()
                    )
            );
        }
        return true;
    }

    public double phi(String target, long nowNanos) {
        ArrivalWindow window = windows.get(target);
        if (window == null || !window.ready()) {
            return 0.0;
        }
        return PHI_FACTOR * window.unscaledPhi(nowNanos);
    }

    @Override
    public void tick(long nowNanos) {
        if (lastTickNanos >= 0) {
            long elapsed = nowNanos - lastTickNanos;
            if (!config.maxLocalPause().isZero()
                    && elapsed > config.maxLocalPause().toNanos()) {
                pauseGuardUntilNanos = nowNanos + config.maxLocalPause().toNanos();
                lastTickNanos = nowNanos;
                return;
            }
        }
        lastTickNanos = nowNanos;
        if (pauseGuardUntilNanos >= nowNanos) {
            return;
        }

        for (Map.Entry<String, ArrivalWindow> entry : windows.entrySet()) {
            String target = entry.getKey();
            double currentPhi = PHI_FACTOR * entry.getValue().unscaledPhi(nowNanos);
            if (currentPhi > config.convictThreshold() && status(target) != NodeStatus.DEAD) {
                transition(
                        nowNanos,
                        target,
                        NodeStatus.DEAD,
                        "phi exceeded convict threshold",
                        Map.of(
                                "phi", currentPhi,
                                "threshold", config.convictThreshold(),
                                "meanIntervalNanos", entry.getValue().mean()
                        )
                );
            }
        }
    }

    private static int compare(GossipHeartbeat left, GossipHeartbeat right) {
        int generation = Long.compare(left.generation(), right.generation());
        return generation != 0 ? generation : Long.compare(left.version(), right.version());
    }

    private static final class ArrivalWindow {
        private final PhiAccrualConfig config;
        private final Deque<Long> samples = new ArrayDeque<>();
        private long sum;
        private long lastArrivalNanos;

        private ArrivalWindow(PhiAccrualConfig config) {
            this.config = config;
        }

        private void add(long nowNanos) {
            if (lastArrivalNanos == 0) {
                append(config.initialInterval().toNanos());
            } else {
                long interval = nowNanos - lastArrivalNanos;
                if (interval <= 0) {
                    return;
                }
                if (interval <= config.maxSampleInterval().toNanos()) {
                    append(interval);
                }
            }
            lastArrivalNanos = nowNanos;
        }

        private void append(long interval) {
            if (samples.size() == config.sampleSize()) {
                sum -= samples.removeFirst();
            }
            samples.addLast(interval);
            sum += interval;
        }

        private boolean ready() {
            return lastArrivalNanos > 0 && !samples.isEmpty();
        }

        private double mean() {
            return samples.isEmpty() ? 0.0 : (double) sum / samples.size();
        }

        private double unscaledPhi(long nowNanos) {
            if (!ready()) {
                return 0.0;
            }
            return Math.max(0L, nowNanos - lastArrivalNanos) / mean();
        }
    }
}
