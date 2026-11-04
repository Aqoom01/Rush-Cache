package com.openkache.failuredetector.failover;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailoverPolicyTest {
    @Test
    void requiresQuorumAndFreshReplica() {
        FailoverPolicy policy = new FailoverPolicy(
                new FailoverPolicy.Config(5, Duration.ofSeconds(10), 1_000),
                7
        );
        policy.report(new FailoverPolicy.FailureReport("v1", "primary", 100));
        policy.report(new FailoverPolicy.FailureReport("v2", "primary", 100));
        policy.report(new FailoverPolicy.FailureReport("v3", "primary", 100));

        FailoverPolicy.Decision stale = policy.evaluate(
                101,
                "primary",
                new FailoverPolicy.ReplicaCandidate("replica", 2_000, true),
                Set.of("v1", "v2", "v3")
        );
        assertFalse(stale.authorized());

        FailoverPolicy.Decision accepted = policy.evaluate(
                101,
                "primary",
                new FailoverPolicy.ReplicaCandidate("replica", 100, true),
                Set.of("v1", "v2", "v3")
        );
        assertTrue(accepted.authorized());
        assertTrue(accepted.epoch() > 7);
    }

    @Test
    void minorityCannotAuthorizePromotion() {
        FailoverPolicy policy = new FailoverPolicy(
                new FailoverPolicy.Config(5, Duration.ofSeconds(10), 1_000),
                0
        );
        policy.report(new FailoverPolicy.FailureReport("v1", "primary", 100));
        policy.report(new FailoverPolicy.FailureReport("v2", "primary", 100));
        policy.report(new FailoverPolicy.FailureReport("v3", "primary", 100));

        assertFalse(policy.evaluate(
                101,
                "primary",
                new FailoverPolicy.ReplicaCandidate("replica", 0, true),
                Set.of("v1", "v2")
        ).authorized());
    }
}
