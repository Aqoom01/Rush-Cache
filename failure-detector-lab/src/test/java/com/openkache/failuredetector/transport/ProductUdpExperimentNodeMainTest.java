package com.openkache.failuredetector.transport;

import com.openkache.failuredetector.product.ProductAlgorithm;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
