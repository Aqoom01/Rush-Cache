package io.openkache.core.tablet.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.TableMessage.GetTable;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.routing.TableMessage.TableReply;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LocalRoutingAndControllerTest {

    private static final NodeId C = new NodeId(0);
    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final TabletId SLOT3 = new TabletId(3);
    private static final RoutingTable T1 = new FixedSlotScheme(4).initial(List.of(N1, N2));
    private static final long HASH_SLOT3 = T1.get(SLOT3).range().start();

    @Test
    void localRoutingAcceptsOnlyNewerTables() {
        LocalRouting r = new LocalRouting(N2, T1, new FakeTransport());
        RoutingTable t2 = T1.apply(new Change(SLOT3, N2, N1));

        assertTrue(r.accept(t2));
        assertFalse(r.accept(T1));
        assertFalse(r.accept(t2));
        assertEquals(2, r.version().value());
    }

    @Test
    void stopServingRequiresOwnershipAndIsReleasedWhenOwnerOrEpochChanges() {
        LocalRouting r = new LocalRouting(N2, T1, new FakeTransport());
        assertThrows(IllegalArgumentException.class, () -> r.stopServing(new TabletId(0), Optional.empty()));

        r.stopServing(SLOT3, Optional.of(N1));
        assertTrue(r.isRefusing(SLOT3));
        assertEquals(ResultStatus.MOVED, r.check(HASH_SLOT3, T1.version(), OwnershipEpoch.ZERO).status());
        assertEquals(Optional.of(N1), r.check(HASH_SLOT3, T1.version(), OwnershipEpoch.ZERO).redirect());

        // 주인이 안 바뀐 새 표(다른 슬롯 이동)는 플래그를 유지한다
        RoutingTable other = T1.apply(new Change(new TabletId(0), N1, N2));
        r.accept(other);
        assertTrue(r.isRefusing(SLOT3));

        // 슬롯 3의 주인이 바뀐 표는 플래그를 푼다
        RoutingTable moved = other.apply(new Change(SLOT3, N2, N1));
        r.accept(moved);
        assertFalse(r.isRefusing(SLOT3));
        assertEquals(ResultStatus.MOVED, r.check(HASH_SLOT3, moved.version(), OwnershipEpoch.ZERO).status(), "now N1 owns it");
    }

    @Test
    void localRoutingAnswersGetTableAndIgnoresForeignMessages() {
        FakeTransport t = new FakeTransport();
        LocalRouting r = new LocalRouting(N2, T1, t);

        assertTrue(r.onPeerMessage(N1, new GetTable(RoutingVersion.ZERO)));
        assertTrue(r.onPeerMessage(N1, new GetTable(T1.version())));
        List<PeerEnvelope> sent = t.sent();
        assertEquals(2, sent.size());
        assertEquals(Optional.of(T1), ((TableReply) sent.get(0).message()).table());
        assertTrue(((TableReply) sent.get(1).message()).table().isEmpty(), "no change since the asked version");
        assertFalse(r.onPeerMessage(N1, new PeerMessage() {}));
    }

    @Test
    void controllerPushesToMembersExceptItselfAndRejectsNonAdvancingChange() {
        FakeTransport t = new FakeTransport();
        Controller c = new Controller(N1, T1, t); // 노드1이 Controller 역할을 겸함

        c.move(T1.version(), new Change(SLOT3, N2, N1));

        assertEquals(1, t.sent().size(), "only N2 is pushed; N1 is itself");
        assertEquals(N2, t.sent().get(0).to());
        assertEquals(2, ((TablePush) t.sent().get(0).message()).table().version().value());
        assertThrows(IllegalArgumentException.class, () -> c.apply(c.version(), table -> table));
    }

    @Test
    void unregisteredNodeIsNoLongerPushed() {
        FakeTransport t = new FakeTransport();
        Controller c = new Controller(C, T1, t);

        c.unregister(N2);
        c.move(c.version(), new Change(SLOT3, N2, N1));

        assertEquals(1, t.sent().size());
        assertEquals(N1, t.sent().get(0).to());
        assertFalse(c.members().contains(N2));
    }

    @Test
    void controllerSplitAndMergeGoThroughTheSameCas() {
        Controller c = new Controller(C, T1, new FakeTransport());
        long mid = T1.get(SLOT3).range().start() + 1;

        RoutingTable split = c.split(c.version(), SLOT3, mid).orElseThrow();
        assertEquals(5, split.size());
        assertTrue(c.split(T1.version(), SLOT3, mid + 1).isEmpty(), "stale expected version");
        RoutingTable merged = c.merge(c.version(), SLOT3, new TabletId(4)).orElseThrow();
        assertEquals(4, merged.size());
        assertEquals(3, c.version().value());
    }

    @Test
    void controllerAnswersGetTableWithNewerTableOrNoChange() {
        FakeTransport t = new FakeTransport();
        Controller c = new Controller(C, T1, t);

        assertTrue(c.onPeerMessage(N1, new GetTable(RoutingVersion.ZERO)));
        assertTrue(c.onPeerMessage(N1, new GetTable(T1.version())));
        assertFalse(c.onPeerMessage(N1, new TablePush(T1)), "controller does not take pushes");
        assertEquals(Optional.of(T1), ((TableReply) t.sent().get(0).message()).table());
        assertTrue(((TableReply) t.sent().get(1).message()).table().isEmpty());
    }

    @Test
    void tableReplyRejectsMismatchedVersion() {
        assertThrows(IllegalArgumentException.class, () -> new TableReply(Optional.of(T1), RoutingVersion.ZERO));
    }
}
