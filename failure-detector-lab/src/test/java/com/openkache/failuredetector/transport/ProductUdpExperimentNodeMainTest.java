package com.openkache.failuredetector.transport;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.product.ProductAlgorithm;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProductUdpExperimentNodeMainTest {
    @Test
    void productAdaptersKeepTheirNativeInspiredHeartbeatCadence() {
        assertEquals(1_000, ProductUdpExperimentNodeMain.protocolIntervalMillis(
                ProductAlgorithm.REDIS_CLUSTER));
        assertEquals(1_000, ProductUdpExperimentNodeMain.protocolIntervalMillis(
                ProductAlgorithm.PHI_GOSSIP));
        assertEquals(1_000, ProductUdpExperimentNodeMain.protocolIntervalMillis(
                ProductAlgorithm.IGNITE_TCP_RING));
        assertEquals(150, ProductUdpExperimentNodeMain.protocolIntervalMillis(
                ProductAlgorithm.AEROSPIKE_MESH));
    }

    @Test
    void swimRemainsOnTheDedicatedDirectIndirectUdpHarness() {
        assertThrows(IllegalArgumentException.class,
                () -> ProductUdpExperimentNodeMain.protocolIntervalMillis(
                        ProductAlgorithm.SWIM_LIFEGUARD));
    }

    @Test
    void tcpHeartbeatUsesConfiguredObserverAddressInsteadOfDockerOnlyHostname() {
        InetSocketAddress address = ProductUdpExperimentNodeMain.tcpObserverAddress(
                Map.of(0, InetSocketAddress.createUnresolved("10.42.1.17", 19_000)),
                19_001);

        assertEquals("10.42.1.17", address.getHostString());
        assertEquals(19_001, address.getPort());
        assertFalse(address.isUnresolved(),
                "Socket.connect cannot use an unresolved TCP heartbeat address");
    }

    @Test
    void tcpReadinessRequiresEveryNonObserverPeer() {
        assertEquals(Set.of(1, 2, 3, 4),
                ProductUdpExperimentNodeMain.expectedTcpHeartbeatSenders(
                        ProductAlgorithm.AEROSPIKE_MESH,
                        Set.of(0, 1, 2, 3, 4)));
        assertEquals(Set.of(1),
                ProductUdpExperimentNodeMain.expectedTcpHeartbeatSenders(
                        ProductAlgorithm.IGNITE_TCP_RING,
                        Set.of(0, 1, 2, 3, 4)));
    }

    @Test
    void tcpHeartbeatRejectsPeerMapWithoutObserver() {
        assertThrows(IllegalArgumentException.class,
                () -> ProductUdpExperimentNodeMain.tcpObserverAddress(
                        Map.of(1, new InetSocketAddress("127.0.0.1", 19_000)),
                        19_001));
    }

    @Test
    void igniteUsesRingPredecessorWhileAerospikeUsesFullMesh() {
        Set<Integer> members = Set.of(0, 1, 2, 3, 4);

        assertEquals(Set.of(0), ProductUdpExperimentNodeMain.tcpHeartbeatRecipients(
                ProductAlgorithm.IGNITE_TCP_RING, 1, members));
        assertEquals(Set.of(4), ProductUdpExperimentNodeMain.tcpHeartbeatRecipients(
                ProductAlgorithm.IGNITE_TCP_RING, 0, members));
        assertEquals(Set.of(0, 2, 3, 4),
                ProductUdpExperimentNodeMain.tcpHeartbeatRecipients(
                        ProductAlgorithm.AEROSPIKE_MESH, 1, members));
    }

    @Test
    void phiUsesAThirdNodeAsGossipRelayAndValidationIsIndependent() {
        Set<Integer> members = Set.of(0, 1, 2, 3, 4);

        assertEquals(2, ProductUdpExperimentNodeMain.phiGossipRecipient(1, members));
        assertEquals(2, ProductUdpExperimentNodeMain.independentValidator(0, 1, members));
        assertEquals(Set.of(2, 3, 4),
                ProductUdpExperimentNodeMain.independentValidators(0, 1, members));
    }

    @Test
    void targetLatencyCannotBeSatisfiedByAnotherPeersTransition() {
        List<DetectorEvent> events = List.of(
                event(110, "node-2", NodeStatus.DEAD),
                event(200, "node-1", NodeStatus.DEAD));

        assertEquals(200L, ProductUdpExperimentNodeMain.firstTargetEventAtOrAfter(
                events, "node-1", 100L, NodeStatus.DEAD));
    }

    private static DetectorEvent event(long atNanos, String target, NodeStatus status) {
        return new DetectorEvent(
                atNanos,
                "node-0",
                target,
                NodeStatus.ALIVE,
                status,
                "test",
                "test",
                Map.of());
    }
}
