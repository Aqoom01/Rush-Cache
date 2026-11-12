package io.openkache.core.tablet.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Redirect;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.RoutingClient.Route;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RoutingClientTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final RoutingTable T1 = new FixedSlotScheme(4).initial(List.of(N1, N2));
    private static final long HASH_SLOT3 = T1.get(new TabletId(3)).range().start();

    @Test
    void routeUsesTableOwnerVersionAndEpoch() {
        RoutingClient c = new RoutingClient(T1, Optional::empty);

        Route r = c.route(HASH_SLOT3);

        assertEquals(N2, r.node());
        assertEquals(new TabletId(3), r.tablet());
        assertEquals(T1.version(), r.version());
        assertEquals(T1.get(new TabletId(3)).epoch(), r.epoch());
    }

    @Test
    void givesUpAfterMaxRedirectsWhenNodesPingPong() {
        RoutingClient c = new RoutingClient(T1, Optional::empty);
        List<NodeId> visited = new ArrayList<>();

        Result r = c.call(HASH_SLOT3, route -> {
            visited.add(route.node());
            NodeId other = route.node().equals(N1) ? N2 : N1;
            return Result.redirect(ResultStatus.MOVED, new Redirect(other), T1.version(), OwnershipEpoch.ZERO);
        });

        assertEquals(ResultStatus.UNAVAILABLE, r.status());
        assertEquals(RoutingClient.MAX_REDIRECTS + 1, visited.size());
        assertEquals(List.of(N2, N1, N2, N1), visited);
    }

    @Test
    void movedWithoutRedirectAndNoNewerTableIsReturnedAsIs() {
        RoutingClient c = new RoutingClient(T1, Optional::empty);

        Result r = c.call(HASH_SLOT3, route -> Result.of(ResultStatus.MOVED, T1.version(), OwnershipEpoch.ZERO));

        assertEquals(ResultStatus.MOVED, r.status());
    }

    @Test
    void piggybackedNewerVersionIsFetchedLazilyBeforeNextCall() {
        RoutingTable t2 = T1.apply(new Change(new TabletId(3), N2, N1));
        RoutingClient c = new RoutingClient(T1, () -> Optional.of(t2));
        List<NodeId> visited = new ArrayList<>();

        Result first = c.call(HASH_SLOT3, route -> {
            visited.add(route.node());
            return Result.of(ResultStatus.OK, t2.version(), OwnershipEpoch.ZERO); // 처리는 됐지만 더 새 표가 있다고 알림
        });
        assertEquals(ResultStatus.OK, first.status());
        assertTrue(c.refreshNeeded());
        assertEquals(1, c.version().value(), "not fetched yet");

        c.call(HASH_SLOT3, route -> {
            visited.add(route.node());
            return Result.of(ResultStatus.OK, t2.version(), OwnershipEpoch.ZERO);
        });
        assertEquals(List.of(N2, N1), visited, "second call routed with the refreshed table");
        assertFalse(c.refreshNeeded());
        assertEquals(2, c.version().value());
    }

    @Test
    void staleEpochRetriesOnceAfterRefresh() {
        RoutingTable t2 = T1.apply(new Change(new TabletId(3), N2, N1)).apply(new Change(new TabletId(3), N1, N2)); // epoch 3
        RoutingClient c = new RoutingClient(T1, () -> Optional.of(t2));
        List<OwnershipEpoch> seen = new ArrayList<>();

        Result r = c.call(HASH_SLOT3, route -> {
            seen.add(route.epoch());
            return route.epoch().value() < 3
                    ? Result.of(ResultStatus.STALE_EPOCH, t2.version(), new OwnershipEpoch(3))
                    : Result.of(ResultStatus.OK, t2.version(), new OwnershipEpoch(3));
        });

        assertEquals(ResultStatus.OK, r.status());
        assertEquals(List.of(new OwnershipEpoch(1), new OwnershipEpoch(3)), seen);
    }

    @Test
    void staleEpochIsReturnedAsIsWhenNoNewerTableCanBeFetched() {
        RoutingClient c = new RoutingClient(T1, Optional::empty);
        List<NodeId> visited = new ArrayList<>();

        Result r = c.call(HASH_SLOT3, route -> {
            visited.add(route.node());
            return Result.of(ResultStatus.STALE_EPOCH, T1.version(), new OwnershipEpoch(3));
        });

        assertEquals(ResultStatus.STALE_EPOCH, r.status());
        assertEquals(1, visited.size(), "no retry without a newer table");
    }

    @Test
    void acceptIgnoresOlderTables() {
        RoutingTable t2 = T1.apply(new Change(new TabletId(3), N2, N1));
        RoutingClient c = new RoutingClient(t2, Optional::empty);

        assertFalse(c.accept(T1));
        assertEquals(2, c.version().value());
        assertEquals(RoutingVersion.ZERO.next().next(), c.version());
    }
}
