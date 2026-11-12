package com.openkache.failuredetector.product;

import com.openkache.failuredetector.aerospike.AerospikeMeshConfig;
import com.openkache.failuredetector.aerospike.AerospikeMeshFailureDetector;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.ignite.IgniteTcpRingConfig;
import com.openkache.failuredetector.ignite.IgniteTcpRingFailureDetector;
import com.openkache.failuredetector.redis.RedisClusterConfig;
import com.openkache.failuredetector.redis.RedisClusterFailureDetector;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProductFailureDetectorSemanticsTest {
    @Test
    void redisRequiresFreshMasterMajorityAfterLocalPfail() {
        RedisClusterFailureDetector detector = new RedisClusterFailureDetector(
                "node-0",
                Set.of("node-0", "node-2", "node-3", "node-4", "node-5"),
                new RedisClusterConfig(Duration.ofSeconds(3), Duration.ofSeconds(6))
        );
        detector.registerTarget(0, "node-1");
        detector.recordPingSent(1, "node-1");
        detector.tick(Duration.ofSeconds(3).toNanos() + 1);
        assertEquals(NodeStatus.SUSPECT, detector.status("node-1"));

        detector.recordFailureReport(4_000_000_000L, "node-1", "node-2");
        assertEquals(NodeStatus.SUSPECT, detector.status("node-1"));
        detector.recordFailureReport(4_100_000_000L, "node-1", "node-3");
        assertEquals(NodeStatus.DEAD, detector.status("node-1"));
    }

    @Test
    void redisConfirmsFailWhenMajorityReportsArriveBeforeLocalPfail() {
        RedisClusterFailureDetector detector = new RedisClusterFailureDetector(
                "node-0",
                Set.of("node-0", "node-2", "node-3", "node-4", "node-5"),
                new RedisClusterConfig(Duration.ofSeconds(3), Duration.ofSeconds(6))
        );
        detector.registerTarget(0, "node-1");
        detector.recordPingSent(1, "node-1");
        detector.recordFailureReport(2_000_000_000L, "node-1", "node-2");
        detector.recordFailureReport(2_100_000_000L, "node-1", "node-3");
        detector.recordFailureReport(2_200_000_000L, "node-1", "node-4");
        assertEquals(NodeStatus.ALIVE, detector.status("node-1"));

        detector.tick(Duration.ofSeconds(3).toNanos() + 1);

        assertEquals(NodeStatus.DEAD, detector.status("node-1"));
        assertEquals(Set.of("node-0", "node-2", "node-3", "node-4"),
                detector.reportingMasters(Duration.ofSeconds(3).toNanos() + 1, "node-1"));
    }

    @Test
    void redisDiscardsReportsOutsideValidityWindow() {
        RedisClusterFailureDetector detector = new RedisClusterFailureDetector(
                "node-0",
                Set.of("node-0", "node-2", "node-3", "node-4", "node-5"),
                new RedisClusterConfig(Duration.ofSeconds(3), Duration.ofSeconds(6))
        );
        detector.registerTarget(0, "node-1");
        detector.recordPingSent(1, "node-1");
        detector.tick(Duration.ofSeconds(3).toNanos() + 1);
        detector.recordFailureReport(4_000_000_000L, "node-1", "node-2");
        detector.recordFailureReport(11_000_000_000L, "node-1", "node-3");

        assertEquals(NodeStatus.SUSPECT, detector.status("node-1"));
        assertEquals(Set.of("node-3"), detector.reportingMasters(
                11_000_000_000L, "node-1"));
    }

    @Test
    void igniteCoordinatorRemovesFailedNeighborAndReconnectsToNextAlive() {
        IgniteTcpRingFailureDetector detector = new IgniteTcpRingFailureDetector(
                "node-0",
                "node-0",
                List.of("node-0", "node-1", "node-2", "node-3"),
                IgniteTcpRingConfig.defaults()
        );
        detector.tick(Duration.ofSeconds(5).toNanos());
        assertEquals(NodeStatus.SUSPECT, detector.status("node-1"));
        assertFalse(detector.coordinatorConfirmFailure(
                Duration.ofSeconds(5).toNanos(), "node-0", "node-1"));
        assertTrue(detector.coordinatorConfirmFailure(
                Duration.ofSeconds(5).toNanos(), "node-2", "node-1"));
        assertEquals("node-2", detector.nextAliveNeighbor("node-0"));
        assertFalse(detector.applyTopologyUpdate(0, List.of("node-0", "node-1")));
    }

    @Test
    void aerospikeHeartbeatSuspicionDoesNotChangeMembershipBeforeAgreement() {
        AerospikeMeshFailureDetector detector = new AerospikeMeshFailureDetector(
                "node-0",
                Set.of("node-0", "node-1", "node-2", "node-3", "node-4"),
                AerospikeMeshConfig.defaults()
        );
        detector.tick(Duration.ofSeconds(3).toNanos());
        assertEquals(NodeStatus.SUSPECT, detector.status("node-1"));
        assertTrue(detector.members().contains("node-1"));
        assertTrue(detector.proposeMembershipRemoval(1, "node-1"));
        detector.acknowledgeMembershipProposal(3_100_000_000L, 1, "node-2");
        detector.acknowledgeMembershipProposal(3_200_000_000L, 1, "node-3");
        assertEquals(NodeStatus.DEAD, detector.status("node-1"));
        assertFalse(detector.members().contains("node-1"));
        assertFalse(detector.proposeMembershipRemoval(1, "node-2"));
    }

    @Test
    void aerospikeRejectsTargetVoteAndConflictingProposalInSameEpoch() {
        AerospikeMeshFailureDetector detector = new AerospikeMeshFailureDetector(
                "node-0",
                Set.of("node-0", "node-1", "node-2", "node-3", "node-4"),
                AerospikeMeshConfig.defaults()
        );
        detector.tick(Duration.ofSeconds(3).toNanos());

        assertTrue(detector.proposeMembershipRemoval(1, "node-1"));
        assertFalse(detector.proposeMembershipRemoval(1, "node-2"));
        assertFalse(detector.acknowledgeMembershipProposal(
                Duration.ofSeconds(3).toNanos(), 1, "node-1"));
        assertEquals(NodeStatus.SUSPECT, detector.status("node-1"));
    }

    @Test
    void aerospikeRecoveryCancelsStaleRemovalProposal() {
        AerospikeMeshFailureDetector detector = new AerospikeMeshFailureDetector(
                "node-0",
                Set.of("node-0", "node-1", "node-2", "node-3", "node-4"),
                AerospikeMeshConfig.defaults()
        );
        detector.tick(Duration.ofSeconds(3).toNanos());
        assertTrue(detector.proposeMembershipRemoval(1, "node-1"));

        detector.recordHeartbeat(Duration.ofSeconds(3).toNanos(), "node-1");

        assertFalse(detector.acknowledgeMembershipProposal(
                Duration.ofSeconds(3).toNanos(), 1, "node-2"));
        assertEquals(NodeStatus.ALIVE, detector.status("node-1"));
    }
}
