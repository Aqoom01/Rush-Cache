package io.openkache.core.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class IdentifierTest {

    @Test
    void rejectsNegativeValues() {
        assertThrows(IllegalArgumentException.class, () -> new NodeId(-1));
        assertThrows(IllegalArgumentException.class, () -> new TabletId(-1));
        assertThrows(IllegalArgumentException.class, () -> new WorkerId(-1));
        assertThrows(IllegalArgumentException.class, () -> new ClientId(-1));
        assertThrows(IllegalArgumentException.class, () -> new OperationId(-1));
        assertThrows(IllegalArgumentException.class, () -> new OwnershipEpoch(-1));
        assertThrows(IllegalArgumentException.class, () -> new RoutingVersion(-1));
    }

    @Test
    void epochIncreasesMonotonically() {
        OwnershipEpoch next = OwnershipEpoch.ZERO.next();

        assertEquals(1, next.value());
        assertTrue(next.isNewerThan(OwnershipEpoch.ZERO));
        assertFalse(OwnershipEpoch.ZERO.isNewerThan(next));
        assertTrue(next.compareTo(OwnershipEpoch.ZERO) > 0);
    }

    @Test
    void epochOverflowIsRejected() {
        OwnershipEpoch max = new OwnershipEpoch(Long.MAX_VALUE);

        assertThrows(ArithmeticException.class, max::next);
    }

    @Test
    void routingVersionIncreasesMonotonically() {
        assertTrue(RoutingVersion.ZERO.next().isNewerThan(RoutingVersion.ZERO));
    }
}
