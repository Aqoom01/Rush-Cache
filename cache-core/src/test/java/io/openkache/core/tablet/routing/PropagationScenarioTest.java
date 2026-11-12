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
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.RoutingClient.Route;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Controller 1 + 노드 3 + 클라이언트 1을 한 프로세스에 띄우고 표 전파·소유권 판정의 규칙을 끝까지 돌려 본다.
 *
 * <p>슬롯 8개, 노드 3대: 노드1 = 슬롯 0~2, 노드2 = 3~5, 노드3 = 6~7. 시나리오마다 슬롯 7을 노드3 → 노드1로 옮긴다.
 */
class PropagationScenarioTest {

    private static final NodeId CONTROLLER = new NodeId(0);
    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);
    private static final TabletId SLOT7 = new TabletId(7);
    private static final TabletId SLOT6 = new TabletId(6);

    private FakeTransport transport;
    private Controller controller;
    private Map<NodeId, FakeNode> nodes;
    private RoutingClient client;
    private RoutingTable v1;
    private long hash7; // 슬롯 7에 떨어지는 해시
    private long hash6;

    @BeforeEach
    void setUp() {
        transport = new FakeTransport();
        v1 = new FixedSlotScheme(8).initial(List.of(N1, N2, N3));
        hash7 = v1.get(SLOT7).range().start();
        hash6 = v1.get(SLOT6).range().start();
        controller = new Controller(CONTROLLER, v1, transport);
        transport.register(CONTROLLER, controller::onPeerMessage);
        nodes = new LinkedHashMap<>();
        for (NodeId id : List.of(N1, N2, N3)) {
            FakeNode n = new FakeNode(id, v1, transport);
            nodes.put(id, n);
            transport.register(id, n.routing()::onPeerMessage);
        }
        client = new RoutingClient(v1, () -> Optional.of(controller.table()));
    }

    private Result send(long keyHash, boolean write) {
        return client.call(keyHash, route -> nodes.get(route.node()).handle(keyHash, write, route));
    }

    private Change move7ToN1() {
        return new Change(SLOT7, N3, N1);
    }

    @Test
    void pushReachesEveryNodeAndAllAnswerTheSame() {
        RoutingTable v2 = controller.move(v1.version(), move7ToN1()).orElseThrow();

        assertEquals(3, transport.deliverAll(), "push to 3 nodes");
        for (FakeNode n : nodes.values()) {
            assertEquals(v2.version(), n.routing().version());
            assertEquals(N1, n.routing().table().lookup(hash7).owner());
        }
        assertEquals(2, v2.get(SLOT7).epoch().value(), "owner change bumps the tablet epoch");
    }

    @Test
    void lostPushIsRecoveredThroughPiggybackAndGetTable() {
        controller.move(v1.version(), move7ToN1());
        transport.drop(N3, e -> e.message() instanceof TablePush); // 노드3만 push를 못 받음
        transport.deliverAll();
        assertEquals(1, nodes.get(N3).routing().version().value());

        // 클라이언트는 최신 표(v2)로 슬롯 6(여전히 노드3 소유)에 요청 → 노드3은 처리하되 "나보다 새 표"를 본다
        client.refresh();
        Result r = send(hash6, true);
        assertEquals(ResultStatus.OK, r.status());
        assertEquals(1, r.routingVersion().value(), "piggyback carries the node's own (stale) version");
        assertTrue(nodes.get(N3).routing().needsRefresh());

        nodes.get(N3).routing().requestTable(CONTROLLER);
        transport.deliverAll(); // GetTable → Controller → TableReply → 노드3
        assertEquals(2, nodes.get(N3).routing().version().value());
        assertFalse(nodes.get(N3).routing().needsRefresh());
    }

    @Test
    void staleClientGetsMovedAndRecoversByFetchingTheTable() {
        controller.move(v1.version(), move7ToN1());
        transport.deliverAll();
        AtomicInteger attempts = new AtomicInteger();

        Result r = client.call(hash7, route -> {
            attempts.incrementAndGet();
            return nodes.get(route.node()).handle(hash7, true, route);
        });

        assertEquals(ResultStatus.OK, r.status());
        assertEquals(2, attempts.get(), "first try hits the old owner (MOVED), second the new one");
        assertEquals(2, client.version().value());
        assertEquals(1, nodes.get(N1).acceptedWrites(SLOT7));
        assertEquals(0, nodes.get(N3).acceptedWrites(SLOT7));
    }

    @Test
    void staleClientFollowsRedirectHintWhenControllerIsUnreachable() {
        controller.move(v1.version(), move7ToN1());
        transport.deliverAll();
        RoutingClient noFetch = new RoutingClient(v1, Optional::empty);

        Result r = noFetch.call(hash7, route -> nodes.get(route.node()).handle(hash7, true, route));

        assertEquals(ResultStatus.OK, r.status());
        assertEquals(1, noFetch.version().value(), "table could not be fetched");
        assertTrue(noFetch.refreshNeeded(), "piggyback from the new owner says a newer table exists");
        assertEquals(1, nodes.get(N1).acceptedWrites(SLOT7));
    }

    @Test
    void cutoverOrderNeverLetsTwoNodesAcceptWritesForTheSameTablet() {
        FakeNode old = nodes.get(N3);
        FakeNode next = nodes.get(N1);
        assertEquals(ResultStatus.OK, send(hash7, true).status());
        assertEquals(1, old.acceptedWrites(SLOT7));

        // (1) 옛 주인이 먼저 거절을 시작한다. 표는 아직 그대로(v1)
        old.routing().stopServing(SLOT7, Optional.of(N1));
        Result during = send(hash7, true);
        assertEquals(ResultStatus.UNAVAILABLE, during.status(), "old owner refuses, new owner is not owner yet → gives up");
        assertEquals(1, old.acceptedWrites(SLOT7), "no write accepted by the old owner after stopServing");
        assertEquals(0, next.acceptedWrites(SLOT7), "no write accepted by the new owner before cutover");

        // (2) 마지막 복사는 B3 몫. (3) Controller가 주인을 바꾼다. (4) push가 퍼진다
        controller.move(v1.version(), move7ToN1()).orElseThrow();
        transport.deliverAll();
        assertFalse(old.routing().isRefusing(SLOT7), "refusal is released by the table that changed the owner");

        Result after = send(hash7, true);
        assertEquals(ResultStatus.OK, after.status());
        assertEquals(1, old.acceptedWrites(SLOT7));
        assertEquals(1, next.acceptedWrites(SLOT7));
        // 옛 표를 든 요청이 옛 주인에게 직접 와도 거절된다
        Result direct = old.handle(hash7, true, new Route(N3, SLOT7, v1.version(), OwnershipEpoch.ZERO));
        assertEquals(ResultStatus.MOVED, direct.status());
        assertEquals(N1, direct.redirect().orElseThrow().target());
        assertEquals(1, old.acceptedWrites(SLOT7));
    }

    @Test
    void cancelledMoveResumesServingWithoutTableChange() {
        FakeNode old = nodes.get(N3);
        old.routing().stopServing(SLOT7, Optional.empty());
        assertEquals(ResultStatus.MOVED, old.handle(hash7, true, new Route(N3, SLOT7, v1.version(), OwnershipEpoch.ZERO)).status());

        old.routing().resumeServing(SLOT7);

        assertEquals(ResultStatus.OK, send(hash7, true).status());
        assertEquals(1, v1.version().value());
    }

    @Test
    void staleExpectedVersionIsRejectedSoOnlyOneCommitterWins() {
        RoutingVersion seen = controller.version();
        Change a = move7ToN1();
        Change b = new Change(SLOT6, N3, N2);

        assertTrue(controller.move(seen, a).isPresent());
        assertTrue(controller.move(seen, b).isEmpty(), "second committer saw an old table");
        assertEquals(2, controller.version().value());
        assertEquals(N3, controller.table().get(SLOT6).owner());
        // 새 버전으로 다시 시도하면 된다
        assertTrue(controller.move(controller.version(), b).isPresent());
        assertEquals(3, controller.version().value());
    }

    @Test
    void requestWithOldEpochIsRejectedAsStaleEpoch() {
        controller.move(v1.version(), move7ToN1());
        transport.deliverAll();
        OwnershipEpoch oldEpoch = v1.get(SLOT7).epoch();
        RoutingVersion current = controller.version();

        Result r = nodes.get(N1).handle(hash7, true, new Route(N1, SLOT7, current, oldEpoch));

        assertEquals(ResultStatus.STALE_EPOCH, r.status());
        assertEquals(0, nodes.get(N1).acceptedWrites(SLOT7));
        // 클라이언트 경로로는 표를 받아 와서 새 epoch로 다시 보낸다
        assertEquals(ResultStatus.OK, send(hash7, true).status());
    }

    @Test
    void latePushOfAnOlderTableDoesNotRollBack() {
        controller.move(v1.version(), move7ToN1()); // v2
        controller.move(controller.version(), new Change(SLOT6, N3, N2)); // v3

        transport.deliverNext(N1, e -> pushVersion(e) == 3);
        transport.deliverNext(N1, e -> pushVersion(e) == 2);

        assertEquals(3, nodes.get(N1).routing().version().value());
        assertEquals(N2, nodes.get(N1).routing().table().lookup(hash6).owner());
    }

    @Test
    void registeredNodeGetsTheTableBeforeOwningAnything() {
        NodeId n4 = new NodeId(4);
        FakeNode joined = new FakeNode(n4, v1, transport);
        transport.register(n4, joined.routing()::onPeerMessage);
        controller.move(v1.version(), move7ToN1()); // n4는 아직 모른다
        transport.deliverAll();

        controller.register(n4);
        transport.deliverAll();

        assertEquals(2, joined.routing().version().value());
        controller.move(controller.version(), new Change(SLOT6, N3, n4));
        transport.deliverAll();
        assertEquals(3, joined.routing().version().value());
        assertEquals(ResultStatus.OK, joined.handle(hash6, true, new Route(n4, SLOT6, controller.version(), OwnershipEpoch.ZERO)).status());
    }

    private static long pushVersion(PeerEnvelope e) {
        return e.message() instanceof TablePush p ? p.table().version().value() : -1;
    }
}
