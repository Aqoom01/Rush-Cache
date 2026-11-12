package com.openkache.failuredetector.validation;

import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.experiment.ExperimentResult;
import com.openkache.failuredetector.experiment.ExperimentRunner;
import com.openkache.failuredetector.experiment.ScenarioConfig;
import com.openkache.failuredetector.experiment.ScenarioType;
import com.openkache.failuredetector.failover.FailoverPolicy;
import com.openkache.failuredetector.health.HealthMonitor;
import com.openkache.failuredetector.phi.GossipHeartbeat;
import com.openkache.failuredetector.phi.PhiAccrualConfig;
import com.openkache.failuredetector.phi.PhiAccrualFailureDetector;
import com.openkache.failuredetector.swim.SwimLifeguardDetector;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/** Dependency-free verification for environments where Maven/JUnit is absent. */
public final class SelfTest {
    private SelfTest() {
    }

    public static void main(String[] args) {
        verifySwimLocalHealth();
        verifyPhiConviction();
        verifyHealthDebounce();
        verifyFailoverSafety();
        verifyEndToEndSimulation();
        System.out.println("All failure-detector self tests passed.");
    }

    private static void verifySwimLocalHealth() {
        SwimLifeguardDetector detector = new SwimLifeguardDetector("observer", 5);
        long normalTimeout = detector.effectiveProbeTimeoutNanos();
        detector.observeLocalDelay(Duration.ofSeconds(4).toNanos());
        require(detector.effectiveProbeTimeoutNanos() > normalTimeout,
                "Lifeguard did not scale timeout after a local delay");
        detector.recordProbe(1, "target", false, 1);
        require(detector.status("target") == NodeStatus.SUSPECT,
                "SWIM did not enter SUSPECT after failed probe");
        detector.refute(2, "target", 2);
        require(detector.status("target") == NodeStatus.ALIVE,
                "new incarnation did not refute suspicion");
    }

    private static void verifyPhiConviction() {
        PhiAccrualConfig config = new PhiAccrualConfig(
                Duration.ofSeconds(1),
                3,
                100,
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                Duration.ZERO
        );
        PhiAccrualFailureDetector detector = new PhiAccrualFailureDetector("observer", config);
        for (int version = 1; version <= 10; version++) {
            long now = Duration.ofSeconds(version).toNanos();
            detector.recordHeartbeat(new GossipHeartbeat("target", 1, version, now), true);
            detector.tick(now);
        }
        detector.tick(Duration.ofSeconds(18).toNanos());
        require(detector.status("target") == NodeStatus.DEAD,
                "Phi detector did not convict missing peer");
    }

    private static void verifyHealthDebounce() {
        HealthMonitor monitor = new HealthMonitor(Duration.ofMillis(100), 3, 2);
        monitor.record(1, "node-1", HealthMonitor.Component.DISK, false, 200_000_000, "slow");
        monitor.record(2, "node-1", HealthMonitor.Component.DISK, false, 200_000_000, "slow");
        monitor.record(3, "node-1", HealthMonitor.Component.DISK, false, 200_000_000, "slow");
        require(monitor.status("node-1", HealthMonitor.Component.DISK) == NodeStatus.HEALTH_DEGRADED,
                "disk health did not degrade after threshold");
    }

    private static void verifyFailoverSafety() {
        FailoverPolicy policy = new FailoverPolicy(
                new FailoverPolicy.Config(5, Duration.ofSeconds(10), 1_000),
                3
        );
        policy.report(new FailoverPolicy.FailureReport("v1", "primary", 100));
        policy.report(new FailoverPolicy.FailureReport("v2", "primary", 100));
        policy.report(new FailoverPolicy.FailureReport("v3", "primary", 100));
        FailoverPolicy.Decision minority = policy.evaluate(
                101,
                "primary",
                new FailoverPolicy.ReplicaCandidate("replica", 0, true),
                Set.of("v1", "v2")
        );
        require(!minority.authorized(), "minority partition authorized promotion");
        FailoverPolicy.Decision quorum = policy.evaluate(
                101,
                "primary",
                new FailoverPolicy.ReplicaCandidate("replica", 0, true),
                Set.of("v1", "v2", "v3")
        );
        require(quorum.authorized(), "healthy fresh replica did not receive quorum");
    }

    private static void verifyEndToEndSimulation() {
        List<ExperimentResult> results = new ExperimentRunner().runBoth(
                ScenarioConfig.defaults(ScenarioType.CRASH),
                42
        );
        require(results.size() == 2, "both algorithms were not executed");
        for (ExperimentResult result : results) {
            require(result.metrics().falseNegativeEvents() == 0,
                    result.algorithm() + " missed crash failure");
            require(result.metrics().detectionLatencyNanos() != null,
                    result.algorithm() + " has no detection latency");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
