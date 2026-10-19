package io.openkache.core.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ResultTest {

    private static final RoutingVersion ROUTING = RoutingVersion.ZERO;
    private static final OwnershipEpoch EPOCH = OwnershipEpoch.ZERO;

    @Test
    void okCarriesValue() {
        Result result = Result.ok(Bytes.utf8("v"), 3, ROUTING, EPOCH);

        assertEquals(ResultStatus.OK, result.status());
        assertEquals(Optional.of(Bytes.utf8("v")), result.value());
        assertEquals(3, result.valueVersion());
    }

    @Test
    void missHasNoValue() {
        Result result = Result.of(ResultStatus.MISS, ROUTING, EPOCH);

        assertTrue(result.value().isEmpty());
        assertTrue(result.redirect().isEmpty());
    }

    @Test
    void valueIsOnlyAllowedWithOk() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new Result(
                                ResultStatus.MISS,
                                Optional.of(Bytes.utf8("v")),
                                0,
                                ROUTING,
                                EPOCH,
                                Optional.empty()));
    }

    @Test
    void redirectIsOnlyAllowedWithMovedOrNotPrimary() {
        Redirect redirect = new Redirect(new NodeId(2));

        Result moved = Result.redirect(ResultStatus.MOVED, redirect, ROUTING, EPOCH);
        assertEquals(Optional.of(redirect), moved.redirect());

        assertThrows(
                IllegalArgumentException.class,
                () -> Result.redirect(ResultStatus.BUSY, redirect, ROUTING, EPOCH));
    }

    @Test
    void negativeValueVersionIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Result.ok(Bytes.empty(), -1, ROUTING, EPOCH));
    }
}
