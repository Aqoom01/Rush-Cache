package io.openkache.core.tablet.routing;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.TableMessage.GetTable;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.routing.TableMessage.TableReply;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 2노드(Controller 겸 N1, N2) + 클라이언트 in-process 시뮬레이션.
 *
 * <p>FakeTransport로 메시지 큐를 제어하며 아래를 검증한다:
 * <ul>
 *   <li>표 변경 → N2에 TablePush 전송
 *   <li>RoutingClient MOVED 추적 → 표 갱신 후 올바른 노드로 재시도
 *   <li>GetTable 요청 → Controller가 TableReply로 응답
 *   <li>TableMessageCodec 왕복 (encode → decode → 동일한 메시지)
 * </ul>
 */
class TwoNodeRoutingTest {

    private static final NodeId C  = new NodeId(0); // Controller(= N1 역할 겸임)
    private static final NodeId N2 = new NodeId(2);

    private FakeTransport transport;
    private Controller controller;
    private LocalRouting localN2;
    private RoutingTable initialTable;

    @BeforeEach
    void setUp() {
        transport = new FakeTransport();
        initialTable = new FixedSlotScheme(4).initial(List.of(C, N2));

        controller = new Controller(C, initialTable, transport);
        localN2 = new LocalRouting(N2, initialTable, transport);

        // FakeTransport 수신자 등록
        transport.register(C,  (from, msg) -> controller.onPeerMessage(from, msg));
        transport.register(N2, (from, msg) -> localN2.onPeerMessage(from, msg));
    }

    @Test
    void controllerPushesNewTableToN2OnMove() {
        // Tablet 0을 C→N2로 이동
        Change change = firstTabletOwnedBy(C, N2);
        controller.move(initialTable.version(), change);

        // N2에게 TablePush가 전송됐는지 확인
        var pending = transport.pending(N2);
        assertEquals(1, pending.size());
        assertInstanceOf(TablePush.class, pending.get(0).message());

        // 전달 후 N2의 표가 갱신됐는지 확인
        transport.deliverAll(N2);
        assertEquals(2, localN2.version().value());
    }

    @Test
    void routingClientFollowsMovedThenSucceeds() {
        // 클라이언트는 초기 표로 시작
        RoutingClient client = new RoutingClient(initialTable, () -> Optional.of(controller.table()));

        // Tablet 0 소유자를 C → N2로 변경
        Change change = firstTabletOwnedBy(C, N2);
        controller.move(initialTable.version(), change);
        transport.deliverAll(); // N2에게 push 전달

        // 초기 표에서 C로 라우팅되는 key 하나를 찾는다
        long keyHash = initialTable.tablets().get(0).range().start();

        // sender: C에 보내면 MOVED 반환, N2에 보내면 OK 반환
        Result result = client.call(keyHash, route -> {
            if (route.node().equals(C)) {
                // MOVED: 새 주인은 N2 (클라이언트가 표를 갱신하도록)
                return Result.of(ResultStatus.MOVED, controller.table().version(), OwnershipEpoch.ZERO);
            }
            // N2에 도달 → OK
            return Result.of(ResultStatus.OK, controller.table().version(), OwnershipEpoch.ZERO);
        });

        assertEquals(ResultStatus.OK, result.status());
        // 클라이언트가 갱신된 표를 쓰는지 확인
        assertEquals(controller.table().version(), client.version());
    }

    @Test
    void getTableRequestIsAnsweredByController() {
        // N2가 Controller에게 GetTable 요청
        localN2.requestTable(C);
        // Controller가 응답
        transport.deliverAll(C);  // GetTable 처리
        transport.deliverAll(N2); // TableReply 수신

        // N2의 표가 여전히 최신인지 확인 (변경 없음 → 버전 그대로)
        assertEquals(initialTable.version(), localN2.version());
    }

    @Test
    void tablePushCodecRoundTripInFakeTransport() {
        // Controller가 N2에게 보낸 TablePush를 codec으로 직렬화→역직렬화해도 동일한 표가 나와야 한다
        Change change = firstTabletOwnedBy(C, N2);
        controller.move(initialTable.version(), change);

        TablePush push = (TablePush) transport.pending(N2).get(0).message();
        byte[] encoded = TableMessageCodec.encode(push);
        TableMessage decoded = TableMessageCodec.decode(encoded);

        assertInstanceOf(TablePush.class, decoded);
        RoutingTable restoredTable = ((TablePush) decoded).table();
        assertEquals(push.table().version(), restoredTable.version());
        assertEquals(push.table().tablets(), restoredTable.tablets());
    }

    @Test
    void simpleNodeDirectoryLooksUpAddress() {
        SimpleNodeDirectory dir = SimpleNodeDirectory.builder()
                .add(C,  "127.0.0.1", 7000)
                .add(N2, "127.0.0.1", 7002)
                .build();

        assertTrue(dir.lookup(C).isPresent());
        assertEquals(7000, dir.lookup(C).get().port());
        assertEquals(7002, dir.lookup(N2).get().port());
        assertTrue(dir.lookup(new NodeId(99)).isEmpty());
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    /** initialTable에서 from이 주인인 첫 번째 Tablet을 to로 옮기는 Change. */
    private Change firstTabletOwnedBy(NodeId from, NodeId to) {
        return initialTable.tablets().stream()
                .filter(t -> t.owner().equals(from))
                .findFirst()
                .map(t -> new Change(t.id(), from, to))
                .orElseThrow(() -> new IllegalStateException("no tablet owned by " + from));
    }
}
