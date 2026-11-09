package io.openkache.core.tablet.routing;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.balance.split.HeatSplitPolicy;
import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * B1+B2+B3 통합 시나리오(S6): 쏠린 Tablet을 B2 정책으로 쪼개고, 뜨거운 반쪽을 B3 프로토콜로
 * 복사한 뒤 B1 Controller로 cutover한다.
 *
 * <ol>
 *   <li>B2: 쏠린 관찰(TabletLoad) → {@link HeatSplitPolicy} → {@link SplitRunner} → Controller.split
 *   <li>B3: 뜨거운 반쪽 Change → {@link StopAndCopyProtocol} begin/step → readyToCommit
 *   <li>B1: {@link ControllerCommitter}.commit → 표 버전·epoch 증가 → push → 클라이언트 MOVED 추적
 * </ol>
 */
class SplitAndMigrateScenarioTest {

    private static final NodeId C  = new NodeId(0);
    private static final NodeId N2 = new NodeId(2);

    private FakeTransport transport;
    private Controller controller;
    private LocalRouting localN2;
    private RoutingTable initialTable;
    private Tablet hotTablet;
    private SplitRunner runner;

    @BeforeEach
    void setUp() {
        transport = new FakeTransport();
        initialTable = new HashRangeScheme(2).initial(List.of(C, N2));
        controller = new Controller(C, initialTable, transport);
        localN2 = new LocalRouting(N2, initialTable, transport);
        transport.register(C,  (from, msg) -> controller.onPeerMessage(from, msg));
        transport.register(N2, (from, msg) -> localN2.onPeerMessage(from, msg));
        hotTablet = initialTable.tablets().stream()
                .filter(t -> t.owner().equals(C))
                .findFirst().orElseThrow();
        runner = new SplitRunner(controller, new HeatSplitPolicy());
    }

    /** hotTablet 안 네 key에 요청이 고르게 몰린 관찰. 가운데(start+2)에서 나뉜다. */
    private TabletLoad evenHotLoad() {
        long start = hotTablet.range().start();
        return new TabletLoad(hotTablet.range(), 0, List.of(
                new KeyLoad(start,     25, 0),
                new KeyLoad(start + 1, 25, 0),
                new KeyLoad(start + 2, 25, 0),
                new KeyLoad(start + 3, 25, 0)));
    }

    // -----------------------------------------------------------------------
    // B2 → B1: SplitRunner
    // -----------------------------------------------------------------------

    @Test
    void splitRunnerSplitsHotTabletAndPushesTable() {
        Optional<RoutingTable> after = runner.trySplit(hotTablet.id(), evenHotLoad());

        assertTrue(after.isPresent());
        assertEquals(initialTable.size() + 1, after.get().size());
        long at = hotTablet.range().start() + 2;
        Tablet right = after.get().tablets().stream()
                .filter(t -> t.range().start() == at)
                .findFirst().orElseThrow();
        assertEquals(C, right.owner());

        transport.deliverAll();
        assertEquals(controller.version(), localN2.version());
    }

    @Test
    void singleHotKeyLoadIsHeld() {
        long start = hotTablet.range().start();
        TabletLoad skewed = new TabletLoad(hotTablet.range(), 0, List.of(
                new KeyLoad(start,     95, 0),
                new KeyLoad(start + 1,  5, 0)));

        assertTrue(runner.trySplit(hotTablet.id(), skewed).isEmpty());
        assertEquals(initialTable.version(), controller.version());
    }

    @Test
    void staleLoadIsRejected() {
        TabletLoad load = evenHotLoad();
        assertTrue(runner.trySplit(hotTablet.id(), load).isPresent());
        // 같은 관찰로 다시 시도: 구간이 이미 바뀌어 거절된다
        assertTrue(runner.trySplit(hotTablet.id(), load).isEmpty());
    }

    // -----------------------------------------------------------------------
    // B3 → B1: ControllerCommitter
    // -----------------------------------------------------------------------

    @Test
    void splitThenMigrateCommitsThroughController() {
        RoutingTable afterSplit = runner.trySplit(hotTablet.id(), evenHotLoad()).orElseThrow();
        transport.deliverAll();

        long at = hotTablet.range().start() + 2;
        Tablet moving = afterSplit.tablets().stream()
                .filter(t -> t.range().start() == at)
                .findFirst().orElseThrow();
        Change change = new Change(moving.id(), C, N2);

        TestSource source = new TestSource();
        TestDestination destination = new TestDestination();
        StopAndCopyProtocol protocol = new StopAndCopyProtocol(source, destination, afterSplit);
        protocol.begin(MigrationJob.pending(change, protocol.name()));

        int guard = 0;
        while (!protocol.readyToCommit() && guard++ < 100) {
            protocol.step(1 << 20);
        }
        assertTrue(protocol.readyToCommit());

        ControllerCommitter committer = new ControllerCommitter(controller);
        assertTrue(committer.commit(protocol.job(), protocol.expectedTable()));
        protocol.committed(protocol.expectedTable(), controller.table());
        while (protocol.state() != JobState.DONE && guard++ < 100) {
            protocol.step(0);
        }
        assertEquals(JobState.DONE, protocol.state());
        assertTrue(source.cleaned);

        // cutover 결과: 주인 N2, epoch +1, N2도 push로 같은 표를 본다
        assertEquals(N2, controller.table().get(moving.id()).owner());
        assertEquals(moving.epoch().value() + 1, controller.table().get(moving.id()).epoch().value());
        transport.deliverAll();
        assertEquals(controller.version(), localN2.version());
    }

    @Test
    void commitWithStaleTableIsRejected() {
        RoutingTable afterSplit = runner.trySplit(hotTablet.id(), evenHotLoad()).orElseThrow();
        long at = hotTablet.range().start() + 2;
        Tablet moving = afterSplit.tablets().stream()
                .filter(t -> t.range().start() == at)
                .findFirst().orElseThrow();
        Change change = new Change(moving.id(), C, N2);

        StopAndCopyProtocol protocol =
                new StopAndCopyProtocol(new TestSource(), new TestDestination(), afterSplit);
        protocol.begin(MigrationJob.pending(change, protocol.name()));
        int guard = 0;
        while (!protocol.readyToCommit() && guard++ < 100) {
            protocol.step(1 << 20);
        }

        ControllerCommitter committer = new ControllerCommitter(controller);
        // 낡은 표(분할 전)로 commit → 버전 CAS 거절, 표는 그대로
        assertFalse(committer.commit(protocol.job(), initialTable));
        assertEquals(afterSplit.version(), controller.version());
        // 올바른 표로 다시 → 성공
        assertTrue(committer.commit(protocol.job(), protocol.expectedTable()));
    }

    @Test
    void commitBeforeSwitchingIsRejected() {
        RoutingTable afterSplit = runner.trySplit(hotTablet.id(), evenHotLoad()).orElseThrow();
        long at = hotTablet.range().start() + 2;
        Tablet moving = afterSplit.tablets().stream()
                .filter(t -> t.range().start() == at)
                .findFirst().orElseThrow();
        Change change = new Change(moving.id(), C, N2);

        ControllerCommitter committer = new ControllerCommitter(controller);
        assertFalse(committer.commit(MigrationJob.pending(change, "stop-and-copy"), afterSplit));
        assertEquals(afterSplit.version(), controller.version());
    }

    // -----------------------------------------------------------------------
    // 끝에서 끝: 클라이언트가 MOVED를 따라간다
    // -----------------------------------------------------------------------

    @Test
    void clientFollowsMovedAfterSplitAndMigration() {
        // 분할 + 이동(cutover는 Controller.move로 축약: committer 경로는 위에서 검증)
        RoutingTable afterSplit = runner.trySplit(hotTablet.id(), evenHotLoad()).orElseThrow();
        long at = hotTablet.range().start() + 2;
        Tablet moving = afterSplit.tablets().stream()
                .filter(t -> t.range().start() == at)
                .findFirst().orElseThrow();
        controller.move(afterSplit.version(), new Change(moving.id(), C, N2));
        transport.deliverAll();

        // 분할 전 표로 시작한 클라이언트가 뜨거운 반쪽의 key를 요청한다
        RoutingClient client = new RoutingClient(initialTable, () -> Optional.of(controller.table()));
        Result result = client.call(at, route -> {
            if (route.node().equals(C)) {
                return Result.of(ResultStatus.MOVED, controller.table().version(), OwnershipEpoch.ZERO);
            }
            return Result.of(ResultStatus.OK, controller.table().version(), OwnershipEpoch.ZERO);
        });

        assertEquals(ResultStatus.OK, result.status());
        assertEquals(controller.table().version(), client.version());
    }

    // -----------------------------------------------------------------------
    // 최소 Source/Destination (fixture 없이 복사 흐름만 모사)
    // -----------------------------------------------------------------------

    private static final class TestSource implements StopAndCopyProtocol.Source {
        private Tablet frozen;
        boolean cleaned;

        @Override public boolean freeze(Tablet tablet) {
            if (frozen != null) return false;
            frozen = tablet;
            return true;
        }
        @Override public boolean frozen(Tablet tablet) { return tablet.equals(frozen); }
        @Override public void unfreeze(Tablet tablet) {
            if (tablet.equals(frozen)) frozen = null;
        }
        @Override public List<TransferBatch.Record> snapshot(Tablet tablet) {
            return List.of(
                    new TransferBatch.Record(Bytes.utf8("hot-key-1"), Bytes.utf8("value-1")),
                    new TransferBatch.Record(Bytes.utf8("hot-key-2"), Bytes.utf8("value-2")));
        }
        @Override public boolean cleanup(Tablet tablet) {
            cleaned = true;
            return true;
        }
    }

    private static final class TestDestination implements StopAndCopyProtocol.Destination {
        private final Map<Long, TransferBatch> received = new LinkedHashMap<>();
        private boolean sawLast;

        @Override public boolean prepare(MigrationJob session, Tablet tablet) { return true; }
        @Override public void receive(MigrationJob session, OwnershipEpoch epoch, TransferBatch batch) {
            received.put(batch.batchId(), batch);
            if (batch.last()) sawLast = true;
        }
        @Override public Optional<StopAndCopyProtocol.Acknowledgement> acknowledgement(
                MigrationJob session, long batchId) {
            TransferBatch batch = received.get(batchId);
            return batch == null
                    ? Optional.empty()
                    : Optional.of(StopAndCopyProtocol.Acknowledgement.of(batch));
        }
        @Override public boolean complete(MigrationJob session) { return sawLast; }
        @Override public void discard(MigrationJob session) {
            received.clear();
            sawLast = false;
        }
    }
}
