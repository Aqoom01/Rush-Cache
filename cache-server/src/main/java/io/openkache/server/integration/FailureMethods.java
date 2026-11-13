package io.openkache.server.integration;

import com.openkache.failuredetector.core.FailureDetector;
import com.openkache.failuredetector.deadline.*;
import com.openkache.failuredetector.adaptive.*;
import com.openkache.failuredetector.phi.*;
import com.openkache.failuredetector.swim.*;
import com.openkache.failuredetector.redis.*;
import com.openkache.failuredetector.ignite.*;
import com.openkache.failuredetector.aerospike.*;
import java.time.Duration;
import java.util.*;

/** Source-backed A3 catalogue. F codes retain Lab modular profiles; product Phi retains its separate defaults. */
final class FailureMethods {
    static final Set<String> LEGACY = Set.of("FIXED_TIMEOUT", "SWIM", "SWIM_LIFEGUARD");
    static final Set<String> LAB = Set.of("F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7A", "F7B", "F7C",
            "REDIS_CLUSTER", "PHI_GOSSIP", "IGNITE_TCP_RING", "AEROSPIKE_MESH");
    static final Set<String> ALL;
    static final Set<String> KEYS = Set.of("fd.heartbeat.ms", "fd.deadline.ms", "fd.confirm.ms", "fd.probe.timeout.ms",
            "fd.phi.threshold", "fd.ring.successors", "fd.verify.failures", "fd.verify.helpers", "fd.verify.quorum",
            "fd.gossip.fanout", "fd.coordinator.index", "fd.native.watchdog.ms", "fd.repair.timeout.ms", "fd.report.validity.ms");
    static { var all = new TreeSet<>(LEGACY); all.addAll(LAB); ALL = Collections.unmodifiableSet(all); }
    private FailureMethods() { }
    static String canonical(String method) {
        String name = method.toUpperCase(Locale.ROOT).replace('-', '_');
        name = switch (name) {
            case "FIXED_DEADLINE" -> "F0"; case "SWIM_BASE" -> "F2";
            case "LAB_SWIM_LIFEGUARD", "PRODUCT_SWIM_LIFEGUARD" -> "F3";
            case "ADAPTIVE_DEADLINE" -> "F4"; case "FIXED_DEADLINE_RING" -> "F5";
            case "FIXED_DEADLINE_RING_VERIFY" -> "F6"; case "SWIM_LHA_PROBE" -> "F7A";
            case "SWIM_LHA_SUSPICION" -> "F7B"; case "SWIM_BUDDY" -> "F7C"; default -> name;
        };
        if (!ALL.contains(name)) throw new IllegalArgumentException("unsupported failure detector: " + method);
        return name;
    }
    static boolean legacy(String name) { return LEGACY.contains(name); }
    static boolean swim(String name) { return Set.of("F2", "F3", "F7A", "F7B", "F7C").contains(name); }
    static boolean ring(String name) { return Set.of("F5", "F6").contains(name); }
    static int heartbeat(Properties p) {
        String m = canonical(p.getProperty("failure.detector", "FIXED_TIMEOUT"));
        return Integer.parseInt(p.getProperty("fd.heartbeat.ms", "" + (Set.of("REDIS_CLUSTER", "PHI_GOSSIP", "IGNITE_TCP_RING").contains(m) ? 1000 : m.equals("AEROSPIKE_MESH") ? 150 : 200)));
    }
    static int deadline(Properties p) {
        String m = canonical(p.getProperty("failure.detector", "FIXED_TIMEOUT"));
        return Integer.parseInt(p.getProperty("fd.deadline.ms", "" + (m.equals("REDIS_CLUSTER") ? 15000 : m.equals("IGNITE_TCP_RING") ? 5000 : m.equals("AEROSPIKE_MESH") ? 1500 : m.equals("PHI_GOSSIP") ? 3000 : 600)));
    }
    static void validate(Properties p) {
        String method = canonical(p.getProperty("failure.detector", "FIXED_TIMEOUT"));
        for (String key : KEYS) if (p.containsKey(key) && !key.equals("fd.phi.threshold")) {
            int min = Set.of("fd.confirm.ms", "fd.coordinator.index").contains(key) ? 0 : 1;
            int max = key.equals("fd.coordinator.index") ? 2 : Set.of("fd.ring.successors","fd.gossip.fanout").contains(key) ? 2 : Set.of("fd.verify.helpers", "fd.verify.quorum").contains(key) ? 1 : 3600000;
            IntegrationConfig.integer(p, key, min, min, max);
        }
        if (method.equals("REDIS_CLUSTER") && Integer.parseInt(p.getProperty("fd.report.validity.ms",""+(deadline(p)*2))) < deadline(p)) throw new IllegalArgumentException("Redis failure report validity must cover its timeout");
        double phi = Double.parseDouble(p.getProperty("fd.phi.threshold", "8"));
        if (!Double.isFinite(phi) || phi <= 0 || phi > 100) throw new IllegalArgumentException("invalid phi threshold");
        if (heartbeat(p) < 10 || heartbeat(p) > 60000 || deadline(p) <= heartbeat(p)) throw new IllegalArgumentException("FD deadline must exceed heartbeat; heartbeat >=10ms");
        if (!legacy(method)) {
            if (method.equals("F6") && Integer.parseInt(p.getProperty("fd.ring.successors", "2")) < 1) throw new IllegalArgumentException("ring observer required");
            if (Integer.parseInt(p.getProperty("fd.native.watchdog.ms", "300000")) < 60000) throw new IllegalArgumentException("Lab detector watchdog must be >=60000ms; it must not win the detector experiment");
        } else for (String key : KEYS) if (p.containsKey(key) && !Set.of("fd.native.watchdog.ms", "fd.repair.timeout.ms").contains(key))
            throw new IllegalArgumentException("Lab FD setting used with legacy detector: " + key);
    }
    static FailureDetector create(IntegrationConfig c, int self, List<String> members, String method) {
        Properties p = c.values(); String observer = members.get(self); Duration interval = Duration.ofMillis(heartbeat(p));
        Duration confirm = Duration.ofMillis(c.number("fd.confirm.ms", 400));
        if (swim(method)) {
            SwimLifeguardConfig source = switch (method) { case "F2" -> SwimLifeguardConfig.swimBase(); case "F7A" -> SwimLifeguardConfig.lhaProbeOnly();
                case "F7B" -> SwimLifeguardConfig.lhaSuspicionOnly(); case "F7C" -> SwimLifeguardConfig.buddyOnly(); default -> SwimLifeguardConfig.defaults(); };
            return new SwimLifeguardDetector(observer, members.size(), new SwimLifeguardConfig(interval, Duration.ofMillis(c.number("fd.probe.timeout.ms", 100)), 1,
                    source.suspicionMultiplier(), method.equals("F3") ? 2 : source.suspicionMaxTimeoutMultiplier(), source.suspicionConfirmationsEnabled() ? Math.min(1, source.maxConfirmations()) : 0,
                    source.awarenessMaxMultiplier(), source.lhaProbeEnabled(), source.lhaSuspicionEnabled(), source.suspicionConfirmationsEnabled(), source.buddySystemEnabled()));
        }
        return switch (method) {
            case "F0", "F5", "F6" -> new FixedDeadlineFailureDetector(observer, new FixedDeadlineConfig(interval, Duration.ofMillis(deadline(p)), confirm, Duration.ZERO,
                    ring(method) ? FixedDeadlineConfig.PeerSelectionMode.DETERMINISTIC_RING : FixedDeadlineConfig.PeerSelectionMode.FULL_MESH,
                    128, method.equals("F6"), Duration.ofMillis(c.number("fd.probe.timeout.ms", 100)), c.number("fd.verify.failures", 2)));
            case "F1", "PHI_GOSSIP" -> new PhiAccrualFailureDetector(observer, new PhiAccrualConfig(interval, Double.parseDouble(p.getProperty("fd.phi.threshold", "8")),
                    1000, interval.multipliedBy(2), interval.multipliedBy(2), interval.multipliedBy(5)));
            case "F4" -> new AdaptiveDeadlineFailureDetector(observer, new AdaptiveDeadlineConfig(interval, 5, .2, interval, interval.dividedBy(2), 4,
                    Duration.ZERO, interval.multipliedBy(2), interval.multipliedBy(10), interval.multipliedBy(10), confirm, Duration.ZERO, 128));
            case "REDIS_CLUSTER" -> new RedisClusterFailureDetector(observer, Set.copyOf(members), new RedisClusterConfig(Duration.ofMillis(deadline(p)), Duration.ofMillis(c.number("fd.report.validity.ms", deadline(p) * 2))));
            case "IGNITE_TCP_RING" -> new IgniteTcpRingFailureDetector(observer, members.get(c.number("fd.coordinator.index", 2)), members, new IgniteTcpRingConfig(Duration.ofMillis(deadline(p))));
            case "AEROSPIKE_MESH" -> new AerospikeMeshFailureDetector(observer, Set.copyOf(members), new AerospikeMeshConfig(Duration.ofMillis(deadline(p))));
            default -> throw new IllegalArgumentException("not a Lab FD: " + method);
        };
    }
}
