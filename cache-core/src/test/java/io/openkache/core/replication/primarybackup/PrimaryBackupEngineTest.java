package io.openkache.core.replication.primarybackup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.Redirect;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.WorkerId;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.InMemoryTransport;
import io.openkache.core.replication.VirtualTimeSource;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage.Replicate;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage.ReplicateAck;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.TimeSource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 고정 3노드(Primary 1 + Backup 2)에서 SET 동기 복제와 ACK 대기 조건을 검증한다.
 *
 * <p>세 노드는 각자 다른 {@link KvEngine} 인스턴스를 가지며, 테스트 thread가 {@link DirectMailbox}로 세 노드의 owner 역할을
 * 모두 맡아 메시지 전달 순서를 직접 정한다. 시간은 {@link VirtualTimeSource}로 진행하고 timer 이벤트는 테스트가 넣는다.
 */
class PrimaryBackupEngineTest {

    private static final NodeId PRIMARY = new NodeId(1);
    private static final NodeId BACKUP_1 = new NodeId(2);
    private static final NodeId BACKUP_2 = new NodeId(3);
    private static final List<NodeId> BACKUPS = List.of(BACKUP_1, BACKUP_2);
    private static final OwnershipEpoch EPOCH = new OwnershipEpoch(7);
    private static final Duration CLIENT_TIMEOUT = Duration.ofMillis(500);

    private final Cluster cluster = new Cluster();

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    // ---- 필수 테스트 ----

    @Test
    void normalReplication_okOnlyAfterBothBackupsAcked() {
        List<String> trace = new ArrayList<>();

        CompletableFuture<Result> set = cluster.set("k", "v");
        trace.add("1. Primary가 SET 수신 → op 1 부여, Backup 1·2에 Replicate 전송");
        assertFalse(set.isDone());
        for (NodeId backup : BACKUPS) {
            List<PeerEnvelope> queued = cluster.transport.pending(backup);
            assertEquals(1, queued.size());
            Replicate replicate = assertInstanceOf(Replicate.class, queued.getFirst().message());
            assertEquals(EPOCH, replicate.epoch());
            assertEquals(1, replicate.opNumber());
            assertEquals(PRIMARY, queued.getFirst().from());
        }

        cluster.transport.deliverAll(BACKUP_1);
        trace.add("2. Backup 1 적용 및 ACK 전송");
        assertEquals(Optional.of("v"), cluster.valueAt(BACKUP_1, "k"));
        assertEquals(1, cluster.transport.pendingCount(PRIMARY));

        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_1));
        trace.add("3. Primary ACK 1/2 → 응답 대기 (클라이언트 future 미완료, Primary KV 미적용)");
        assertFalse(set.isDone());
        assertEquals(Optional.empty(), cluster.valueAt(PRIMARY, "k"));

        cluster.transport.deliverAll(BACKUP_2);
        trace.add("4. Backup 2 적용 및 ACK 전송");
        assertEquals(Optional.of("v"), cluster.valueAt(BACKUP_2, "k"));
        assertFalse(set.isDone());

        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_2));
        trace.add("5. Primary ACK 2/2");
        trace.add("6. Primary 로컬 KV 적용");
        trace.add("7. 클라이언트 OK");
        assertTrue(set.isDone());
        Result result = set.join();
        assertEquals(ResultStatus.OK, result.status());
        assertEquals(1, result.valueVersion());
        assertEquals(EPOCH, result.ownershipEpoch());

        for (NodeId node : List.of(PRIMARY, BACKUP_1, BACKUP_2)) {
            assertEquals(Optional.of("v"), cluster.valueAt(node, "k"), "node " + node.value());
        }
        assertEquals(0, cluster.transport.pendingCount(PRIMARY));

        System.out.println("[정상 복제 흐름]");
        trace.forEach(System.out::println);
    }

    @Test
    void ackDelay_responseStaysPendingUntilSecondAck() {
        CompletableFuture<Result> set = cluster.set("k", "v");
        cluster.transport.deliverAll(BACKUP_1);
        cluster.transport.deliverAll(BACKUP_2);

        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_1));
        assertFalse(set.isDone(), "Backup 1의 ACK만으로는 응답하지 않는다");
        assertEquals(Optional.empty(), cluster.valueAt(PRIMARY, "k"));

        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_2));
        assertEquals(ResultStatus.OK, set.join().status());
        assertEquals(Optional.of("v"), cluster.valueAt(PRIMARY, "k"));
    }

    @Test
    void ackMissing_timesOutAtDeadlineWithoutRollback() {
        CompletableFuture<Result> set = cluster.set("k", "v");
        cluster.transport.deliverAll(BACKUP_1);
        cluster.transport.deliverAll(BACKUP_2);
        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_1));
        // Backup 2의 ACK는 전달하지 않고 큐에 둔다.
        assertEquals(1, cluster.transport.pendingCount(PRIMARY));

        cluster.time.advance(CLIENT_TIMEOUT.minusMillis(1));
        cluster.fireDeadlineTimer();
        assertFalse(set.isDone(), "deadline 전의 timer는 아무 일도 하지 않는다");

        cluster.time.advance(Duration.ofMillis(1));
        cluster.fireDeadlineTimer();
        assertEquals(ResultStatus.TIMEOUT, set.join().status());

        // TIMEOUT은 롤백이 아니다. 두 Backup에는 이미 적용됐고 Primary에는 적용되지 않았다.
        assertEquals(Optional.empty(), cluster.valueAt(PRIMARY, "k"));
        assertEquals(Optional.of("v"), cluster.valueAt(BACKUP_1, "k"));
        assertEquals(Optional.of("v"), cluster.valueAt(BACKUP_2, "k"));
    }

    @Test
    void duplicateAck_fromSameBackupCountsOnce() {
        CompletableFuture<Result> set = cluster.set("k", "v");
        cluster.transport.deliverAll(BACKUP_1);
        cluster.transport.deliverAll(BACKUP_2);

        PeerEnvelope firstAck = cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_1));
        cluster.transport.deliver(firstAck); // 같은 ACK를 한 번 더 전달
        assertFalse(set.isDone(), "Backup 1의 ACK 두 개를 2/2로 세지 않는다");
        assertEquals(Optional.empty(), cluster.valueAt(PRIMARY, "k"));

        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_2));
        assertEquals(ResultStatus.OK, set.join().status());
    }

    @Test
    void lateAck_afterTimeoutIsIgnoredAndStateIsCleared() {
        CompletableFuture<Result> first = cluster.set("k", "v");
        cluster.transport.deliverAll(BACKUP_1);
        cluster.transport.deliverAll(BACKUP_2);
        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_1));
        cluster.time.advance(CLIENT_TIMEOUT);
        cluster.fireDeadlineTimer();
        assertEquals(ResultStatus.TIMEOUT, first.join().status());

        // 보류했던 Backup 2의 ACK를 뒤늦게 전달한다.
        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_2));
        assertEquals(ResultStatus.TIMEOUT, first.join().status(), "응답은 TIMEOUT 그대로다");
        assertEquals(Optional.empty(), cluster.valueAt(PRIMARY, "k"), "늦은 ACK로 로컬 적용이 일어나지 않는다");

        // 진행 중 상태가 비워졌으므로 다음 SET은 BUSY가 아니라 새 op-number로 복제를 시작한다.
        CompletableFuture<Result> second = cluster.set("k2", "v2");
        assertFalse(second.isDone());
        Replicate replicate = assertInstanceOf(Replicate.class, cluster.transport.pending(BACKUP_1).getFirst().message());
        assertEquals(2, replicate.opNumber());
    }

    // ---- 보조 테스트 ----

    @Test
    void lateAckArrivingWhileSecondRequestInFlightDoesNotCountForIt() {
        CompletableFuture<Result> first = cluster.set("k", "v");
        cluster.transport.deliverAll(BACKUP_1);
        cluster.transport.deliverAll(BACKUP_2);
        cluster.time.advance(CLIENT_TIMEOUT);
        cluster.fireDeadlineTimer();
        assertEquals(ResultStatus.TIMEOUT, first.join().status());
        // op 1의 ACK 두 개가 아직 Primary 큐에 남아 있다.

        CompletableFuture<Result> second = cluster.set("k2", "v2");
        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_1)); // op 1 ACK
        cluster.transport.deliverNext(PRIMARY, ackFrom(BACKUP_2)); // op 1 ACK
        assertFalse(second.isDone(), "op-number가 다른 ACK는 op 2의 ACK로 세지 않는다");
    }

    @Test
    void secondSetWhileOneIsInFlightIsBusy() {
        CompletableFuture<Result> first = cluster.set("k", "v");
        CompletableFuture<Result> second = cluster.set("k2", "v2");

        assertFalse(first.isDone());
        assertEquals(ResultStatus.BUSY, second.join().status());
        assertEquals(1, cluster.transport.pendingCount(BACKUP_1), "두 번째 SET은 복제를 시작하지 않는다");
    }

    @Test
    void forgedOrMismatchedAcksAreIgnored() {
        CompletableFuture<Result> set = cluster.set("k", "v");
        PrimaryBackupEngine primary = cluster.engines.get(PRIMARY);

        primary.onPeerMessage(new NodeId(99), new ReplicateAck(EPOCH, 1)); // 지정되지 않은 노드
        primary.onPeerMessage(BACKUP_1, new ReplicateAck(EPOCH.next(), 1)); // 다른 epoch
        primary.onPeerMessage(BACKUP_1, new ReplicateAck(EPOCH, 2)); // 다른 op-number
        primary.onPeerMessage(BACKUP_2, new ReplicateAck(EPOCH, 2));
        assertFalse(set.isDone());

        primary.onPeerMessage(BACKUP_1, new ReplicateAck(EPOCH, 1));
        primary.onPeerMessage(BACKUP_2, new ReplicateAck(EPOCH, 1));
        assertEquals(ResultStatus.OK, set.join().status());
    }

    @Test
    void backupAppliesOnlyReplicateFromDesignatedPrimaryWithCurrentEpoch() {
        PrimaryBackupEngine backup = cluster.engines.get(BACKUP_1);
        Command.Set set = new Command.Set(Bytes.utf8("k"), Bytes.utf8("v"));

        backup.onPeerMessage(BACKUP_2, new Replicate(EPOCH, 1, set)); // Primary가 아닌 노드
        backup.onPeerMessage(PRIMARY, new Replicate(EPOCH.next(), 1, set)); // 다른 epoch
        assertEquals(Optional.empty(), cluster.valueAt(BACKUP_1, "k"));
        assertEquals(0, cluster.transport.pendingCount(PRIMARY), "ACK를 보내지 않는다");

        backup.onPeerMessage(PRIMARY, new Replicate(EPOCH, 1, set));
        assertEquals(Optional.of("v"), cluster.valueAt(BACKUP_1, "k"));
        PeerEnvelope ack = cluster.transport.pending(PRIMARY).getFirst();
        assertEquals(BACKUP_1, ack.from());
        assertEquals(new ReplicateAck(EPOCH, 1), ack.message());
    }

    @Test
    void backupRejectsClientRequestsWithNotPrimaryRedirect() {
        PrimaryBackupEngine backup = cluster.engines.get(BACKUP_2);

        Result set = backup.execute(new Command.Set(Bytes.utf8("k"), Bytes.utf8("v")), cluster.context()).toCompletableFuture().join();
        assertEquals(ResultStatus.NOT_PRIMARY, set.status());
        assertEquals(Optional.of(new Redirect(PRIMARY)), set.redirect());
        assertEquals(EPOCH, set.ownershipEpoch());
        assertEquals(Optional.empty(), cluster.valueAt(BACKUP_2, "k"));
        assertEquals(0, cluster.transport.pendingCount(BACKUP_1), "Backup은 복제를 시작하지 않는다");

        Result get = backup.execute(new Command.Get(Bytes.utf8("k")), cluster.context()).toCompletableFuture().join();
        assertEquals(ResultStatus.NOT_PRIMARY, get.status());
    }

    @Test
    void primaryReadsDelegateToLocalKvAndDeleteIsNotImplemented() {
        PrimaryBackupEngine primary = cluster.engines.get(PRIMARY);
        Command.Get get = new Command.Get(Bytes.utf8("k"));

        assertEquals(ResultStatus.MISS, primary.execute(get, cluster.context()).toCompletableFuture().join().status());

        cluster.set("k", "v");
        cluster.transport.deliverAll(BACKUP_1);
        cluster.transport.deliverAll(BACKUP_2);
        cluster.transport.deliverAll(PRIMARY);

        Result read = primary.execute(get, cluster.context()).toCompletableFuture().join();
        assertEquals(ResultStatus.OK, read.status());
        assertEquals(Bytes.utf8("v"), read.value().orElseThrow());
        assertEquals(EPOCH, read.ownershipEpoch());

        Result delete = primary.execute(new Command.Delete(Bytes.utf8("k")), cluster.context()).toCompletableFuture().join();
        assertEquals(ResultStatus.NOT_IMPLEMENTED, delete.status());
        assertEquals(Optional.of("v"), cluster.valueAt(PRIMARY, "k"));
    }

    @Test
    void expiredDeadlineAtSubmitIsTimeoutWithoutReplicating() {
        RequestContext expired =
                new RequestContext(
                        new ClientId(1),
                        new OperationId(1),
                        RoutingVersion.ZERO,
                        EPOCH,
                        new Deadline(cluster.time.monotonicNanos() - 1));

        Result result =
                cluster.engines.get(PRIMARY)
                        .execute(new Command.Set(Bytes.utf8("k"), Bytes.utf8("v")), expired)
                        .toCompletableFuture()
                        .join();

        assertEquals(ResultStatus.TIMEOUT, result.status());
        assertEquals(0, cluster.transport.pendingCount(BACKUP_1));
    }

    @Test
    void closeCompletesInFlightSetWithUnavailable() {
        CompletableFuture<Result> set = cluster.set("k", "v");
        PrimaryBackupEngine primary = cluster.engines.get(PRIMARY);

        primary.close();
        primary.close();

        assertEquals(ResultStatus.UNAVAILABLE, set.join().status());
        Result after = primary.execute(new Command.Set(Bytes.utf8("k2"), Bytes.utf8("v")), cluster.context()).toCompletableFuture().join();
        assertEquals(ResultStatus.UNAVAILABLE, after.status());
    }

    @Test
    void storesAreDistinctInstancesPerNode() {
        assertNotSame(cluster.stores.get(PRIMARY), cluster.stores.get(BACKUP_1));
        assertNotSame(cluster.stores.get(BACKUP_1), cluster.stores.get(BACKUP_2));
        assertNotSame(cluster.stores.get(PRIMARY), cluster.stores.get(BACKUP_2));
    }

    /** 실제 OwnerWorker thread 위에서도 같은 흐름이 동작하는지 확인한다. 노드마다 worker 하나를 KV와 엔진이 함께 쓴다. */
    @Test
    void normalReplicationOnRealOwnerWorkers() throws Exception {
        TimeSource time = TimeSource.system();
        InMemoryTransport transport = new InMemoryTransport();
        List<OwnerWorker> workers = new ArrayList<>();
        Map<NodeId, KvEngine> stores = new LinkedHashMap<>();
        Map<NodeId, PrimaryBackupEngine> engines = new LinkedHashMap<>();
        try {
            for (NodeId node : List.of(PRIMARY, BACKUP_1, BACKUP_2)) {
                OwnerWorker worker = new OwnerWorker(new WorkerId((int) node.value()), 64);
                workers.add(worker);
                KvEngine store = new KvEngine(node, List.of(worker), time);
                PrimaryBackupEngine engine =
                        new PrimaryBackupEngine(
                                new PrimaryBackupConfig(node, PRIMARY, BACKUPS, EPOCH), store, worker, transport, time);
                stores.put(node, store);
                engines.put(node, engine);
                transport.register(node, engine);
            }
            RequestContext context =
                    new RequestContext(
                            new ClientId(1),
                            new OperationId(1),
                            RoutingVersion.ZERO,
                            EPOCH,
                            Deadline.after(time, Duration.ofSeconds(5)));
            Command.Set set = new Command.Set(Bytes.utf8("k"), Bytes.utf8("v"));

            CompletableFuture<Result> result = engines.get(PRIMARY).execute(set, context).toCompletableFuture();
            transport.awaitPending(BACKUP_1, 1, Duration.ofSeconds(5));
            transport.awaitPending(BACKUP_2, 1, Duration.ofSeconds(5));
            assertFalse(result.isDone());

            transport.deliverAll(BACKUP_1);
            transport.deliverAll(BACKUP_2);
            transport.awaitPending(PRIMARY, 2, Duration.ofSeconds(5));
            assertFalse(result.isDone(), "ACK가 큐에만 있고 전달되기 전에는 응답하지 않는다");

            transport.deliverAll(PRIMARY);
            assertEquals(ResultStatus.OK, result.get(5, TimeUnit.SECONDS).status());

            for (NodeId node : stores.keySet()) {
                Result get =
                        stores.get(node)
                                .execute(new Command.Get(Bytes.utf8("k")), context)
                                .toCompletableFuture()
                                .get(5, TimeUnit.SECONDS);
                assertEquals(Bytes.utf8("v"), get.value().orElseThrow(), "node " + node.value());
            }
        } finally {
            engines.values().forEach(PrimaryBackupEngine::close);
            stores.values().forEach(KvEngine::close);
            workers.forEach(OwnerWorker::close);
        }
    }

    private static Predicate<PeerEnvelope> ackFrom(NodeId backup) {
        return envelope -> envelope.from().equals(backup) && envelope.message() instanceof ReplicateAck;
    }

    /** 3노드 in-process 클러스터. 테스트 thread가 모든 노드의 owner다. */
    private static final class Cluster implements AutoCloseable {
        final VirtualTimeSource time = new VirtualTimeSource();
        final InMemoryTransport transport = new InMemoryTransport();
        final Map<NodeId, KvEngine> stores = new LinkedHashMap<>();
        final Map<NodeId, PrimaryBackupEngine> engines = new LinkedHashMap<>();
        private long nextOperation = 1;

        Cluster() {
            for (NodeId node : List.of(PRIMARY, BACKUP_1, BACKUP_2)) {
                PrimaryBackupConfig config = new PrimaryBackupConfig(node, PRIMARY, BACKUPS, EPOCH);
                KvEngine store = new KvEngine(node, List.of(new DirectMailbox()), time); // 노드마다 별도 저장소
                PrimaryBackupEngine engine = new PrimaryBackupEngine(config, store, new DirectMailbox(), transport, time);
                stores.put(node, store);
                engines.put(node, engine);
                transport.register(node, engine);
            }
        }

        CompletableFuture<Result> set(String key, String value) {
            return engines.get(PRIMARY)
                    .execute(new Command.Set(Bytes.utf8(key), Bytes.utf8(value)), context())
                    .toCompletableFuture();
        }

        /** {@code node}의 로컬 KV에서 직접 읽는다. 복제 엔진을 거치지 않는다. */
        Optional<String> valueAt(NodeId node, String key) {
            Result result =
                    stores.get(node).execute(new Command.Get(Bytes.utf8(key)), context()).toCompletableFuture().getNow(null);
            assertNotNull(result, "DirectMailbox에서는 KV 연산이 즉시 완료된다");
            return result.value().map(bytes -> new String(bytes.toByteArray(), StandardCharsets.UTF_8));
        }

        void fireDeadlineTimer() {
            engines.get(PRIMARY)
                    .onTimer(new TimerEvent(PrimaryBackupEngine.REPLICATE_DEADLINE_TIMER, time.monotonicNanos()));
        }

        RequestContext context() {
            return new RequestContext(
                    new ClientId(1),
                    new OperationId(nextOperation++),
                    RoutingVersion.ZERO,
                    EPOCH,
                    Deadline.after(time, CLIENT_TIMEOUT));
        }

        @Override
        public void close() {
            engines.values().forEach(PrimaryBackupEngine::close);
            stores.values().forEach(KvEngine::close);
        }
    }
}
