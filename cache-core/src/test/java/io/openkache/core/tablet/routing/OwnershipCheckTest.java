package io.openkache.core.tablet.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.OwnershipCheck.Decision;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OwnershipCheckTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final RoutingTable T1 = new FixedSlotScheme(4).initial(List.of(N1, N2)); // 노드1: 슬롯 0·1, 노드2: 2·3
    private static final long HASH_SLOT0 = T1.get(new TabletId(0)).range().start();
    private static final long HASH_SLOT3 = T1.get(new TabletId(3)).range().start();

    @Test
    void ownerIsAllowedAndPiggybacksItsVersion() {
        Decision d = OwnershipCheck.decide(N1, T1, HASH_SLOT0, RoutingVersion.ZERO, OwnershipEpoch.ZERO);

        assertTrue(d.allowed());
        assertEquals(T1.version(), d.serverVersion());
        assertTrue(d.rejection().isEmpty());
        assertFalse(d.myTableIsStale());
    }

    @Test
    void nonOwnerIsMovedWithRedirectToOwner() {
        Decision d = OwnershipCheck.decide(N1, T1, HASH_SLOT3, T1.version(), OwnershipEpoch.ZERO);

        assertEquals(ResultStatus.MOVED, d.status());
        assertEquals(Optional.of(N2), d.redirect());
        Result r = d.rejection().orElseThrow();
        assertEquals(ResultStatus.MOVED, r.status());
        assertEquals(N2, r.redirect().orElseThrow().target());
        assertEquals(T1.version(), r.routingVersion());
        assertEquals(T1.get(new TabletId(3)).epoch(), r.ownershipEpoch());
    }

    @Test
    void olderEpochIsStaleEpochAndUnknownEpochIsNotChecked() {
        RoutingTable t2 = T1.apply(new Change(new TabletId(0), N1, N2)).apply(new Change(new TabletId(0), N2, N1)); // epoch 3
        OwnershipEpoch current = t2.get(new TabletId(0)).epoch();
        assertEquals(3, current.value());

        Decision old = OwnershipCheck.decide(N1, t2, HASH_SLOT0, t2.version(), new OwnershipEpoch(1));
        assertEquals(ResultStatus.STALE_EPOCH, old.status());
        assertTrue(old.redirect().isEmpty());
        assertEquals(ResultStatus.STALE_EPOCH, old.rejection().orElseThrow().status());

        assertTrue(OwnershipCheck.decide(N1, t2, HASH_SLOT0, t2.version(), current).allowed());
        assertTrue(OwnershipCheck.decide(N1, t2, HASH_SLOT0, t2.version(), OwnershipEpoch.ZERO).allowed());
    }

    @Test
    void newerClientEpochMeansMyTableIsStaleAndIsRefusedWithoutRedirect() {
        Decision d = OwnershipCheck.decide(N1, T1, HASH_SLOT0, T1.version(), new OwnershipEpoch(5));

        assertEquals(ResultStatus.MOVED, d.status());
        assertTrue(d.redirect().isEmpty());
        assertTrue(d.myTableIsStale());
        assertTrue(d.rejection().orElseThrow().redirect().isEmpty());
    }

    @Test
    void newerClientVersionOnlySetsTheStaleFlag() {
        Decision d = OwnershipCheck.decide(N1, T1, HASH_SLOT0, new RoutingVersion(9), OwnershipEpoch.ZERO);

        assertTrue(d.allowed());
        assertTrue(d.myTableIsStale());
        Decision moved = OwnershipCheck.decide(N1, T1, HASH_SLOT3, new RoutingVersion(9), OwnershipEpoch.ZERO);
        assertEquals(ResultStatus.MOVED, moved.status());
        assertTrue(moved.myTableIsStale());
    }
}
