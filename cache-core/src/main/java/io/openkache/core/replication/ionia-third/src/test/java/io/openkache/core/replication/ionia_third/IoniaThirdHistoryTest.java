package io.openkache.core.replication.ionia_third;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_third.IoniaThirdRegressionTest.*;
import static io.openkache.core.replication.ionia_third.IoniaThirdIntegrationTest.fresh;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerEnvelope;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;

/** 완료 이력과 결과 불명확 호출의 유한 completion 확장을 검사한다. 전체 프로토콜 증명은 아니다. */
@Timeout(30)
class IoniaThirdHistoryTest {
    /** end=0/result=null은 결과 불명확 호출. logicalId 하나를 retry attempt마다 중복 삽입하지 않는다. */
    private record Operation(long logicalId, Command command, long start, long end, Result result) { }
    private record SearchState(int used, Map<Bytes, Bytes> values) { }

    @Test void V02_14_seededCrudWithTrimUsesFollowerMetaAndLegalRealtimeOrder() {
        for (int target : new int[]{0, 1, 2, Integer.MAX_VALUE}) for (int seed = 0; seed < 64; seed++) {
            Random random = new Random(seed); AtomicLong events = new AtomicLong(); List<Operation> history = new ArrayList<>();
            try (Cluster c = new Cluster(3, target);
                 IoniaClient client = new IoniaClient(CLIENT, c.nodes, A, 0, c, c.clock, new IoniaClient.Options(C, true, 10_000_000))) {
                for (int i = 0; i < 10; i++) {
                    Bytes key = Bytes.utf8("k" + random.nextInt(2)); long id = i + 1; long start = events.incrementAndGet();
                    Command command = switch (random.nextInt(3)) {
                        case 0 -> new Command.Set(key, Bytes.utf8("v" + i)); case 1 -> new Command.Get(key); default -> new Command.Delete(key);
                    };
                    client.execute(command, fresh(c, id)).whenComplete((result, error) -> {
                        assertNull(error); history.add(new Operation(id, command, start, events.incrementAndGet(), result));
                    });
                    for (int step = 0; step < random.nextInt(12); step++) drive(c, random);
                    if (i == 4) { c.clock.now += 1_000_000_000L; c.tick(); }
                }
                int limit = 100000;
                while (!c.tasks.isEmpty() || !c.messages.isEmpty()) { assertTrue(limit-- > 0); drive(c, random); }
                assertEquals(10, history.size()); assertTrue(check(history), "N=" + target + " seed=" + seed + " " + history);
            }
        }
        System.out.println("V02a: N=0/1/2/MAX, 64 seeds each, 10 completed CRUD/history, trim+Follower/Meta path; bounded only");
    }
    private static void drive(Cluster c, Random random) {
        if (!c.tasks.isEmpty() && (c.messages.isEmpty() || random.nextBoolean())) c.runOne();
        else if (!c.messages.isEmpty()) {
            PeerEnvelope envelope = c.messages.remove(random.nextInt(c.messages.size()));
            c.trace.add(envelope.toString()); c.engines.get(envelope.to()).onPeerMessage(envelope.from(), envelope.message());
        }
    }
    @Test void V02_14_timedOutWriteSurvivesViewChangeAndRetryRetainsOneLogicalId() {
        for (int n : new int[]{0, 1, 2}) try (Cluster c = new Cluster(3, n)) {
            c.hiddenAcks.addAll(c.nodes);
            Command.Set write = set("uncertain"); CompletionStage<Result> timeout = c.client.execute(write, context(1)); c.runOwners();
            assertFalse(timeout.toCompletableFuture().isDone());
            c.clock.now = 2_000_000_000L; c.tick(); c.pump(); assertEquals(ResultStatus.TIMEOUT, result(timeout).status());
            c.hiddenAcks.clear(); c.crash(A); c.engines.get(B).startViewChange(1); c.engines.get(C).startViewChange(1); c.pump(); c.client.updateView(1, B);
            CompletionStage<Result> get = c.client.readAtFollower(C, new Command.Get(K), fresh(c, 2)); c.pump();
            assertEquals(Bytes.utf8("uncertain"), result(get).value().orElseThrow());
            List<Operation> uncertain = List.of(new Operation(1, write, 1, 0, null), new Operation(2, new Command.Get(K), 3, 4, result(get)));
            assertTrue(check(uncertain));
            long before = c.engines.get(B).progress().lastLogIndex();
            CompletionStage<Result> retry = c.client.execute(write, fresh(c, 1)); c.pump();
            assertEquals(ResultStatus.OK, result(retry).status()); assertEquals(before, c.engines.get(B).progress().lastLogIndex());
            assertEquals(ResultStatus.TIMEOUT, result(timeout).status());
            // One logical write spans the first invocation through its final retry response.
            assertTrue(check(List.of(new Operation(1, write, 1, 6, result(retry)), uncertain.get(1))));
        }
    }
    @Test void V02_14_checkerAllowsPendingWriteToExplainObservedData() {
        assertTrue(check(List.of(new Operation(1, set("v"), 1, 0, null),
                new Operation(2, new Command.Get(K), 2, 3, Result.ok(Bytes.utf8("v"), 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO)))));
    }
    @Test void V02_14_checkerRejectsStaleMissAfterCompletedSet() {
        assertFalse(check(List.of(new Operation(1, set("v"), 1, 2, plain(ResultStatus.OK)),
                new Operation(2, new Command.Get(K), 3, 4, plain(ResultStatus.MISS)))));
    }
    @Test void V02_14_checkerRejectsStaleValueAfterCompletedDelete() {
        assertFalse(check(List.of(new Operation(1, set("v"), 1, 2, plain(ResultStatus.OK)),
                new Operation(2, new Command.Delete(K), 3, 4, plain(ResultStatus.OK)),
                new Operation(3, new Command.Get(K), 5, 6, Result.ok(Bytes.utf8("v"), 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO)))));
    }
    @Test void V02_14_checkerRejectsTwoSuccessfulDeletesOfOneValueAndDuplicateLogicalIds() {
        assertFalse(check(List.of(new Operation(1, set("v"), 1, 2, plain(ResultStatus.OK)),
                new Operation(2, new Command.Delete(K), 3, 6, plain(ResultStatus.OK)),
                new Operation(3, new Command.Delete(K), 4, 5, plain(ResultStatus.OK)))));
        assertThrows(IllegalArgumentException.class, () -> check(List.of(new Operation(1, set("v"), 1, 0, null), new Operation(1, set("v"), 2, 3, plain(ResultStatus.OK)))));
    }
    @Test void V02_14_checkerComparesFullBytesRatherThanTruncatedLogStrings() {
        byte[] first = new byte[40], second = new byte[40]; first[39] = 1; second[39] = 2;
        Bytes a = Bytes.copyOf(first), b = Bytes.copyOf(second);
        assertEquals(a.toString(), b.toString()); assertNotEquals(a, b);
        assertTrue(check(List.of(new Operation(1, new Command.Set(K, a), 1, 3, plain(ResultStatus.OK)),
                new Operation(2, new Command.Set(K, b), 2, 4, plain(ResultStatus.OK)),
                new Operation(3, new Command.Get(K), 5, 6, Result.ok(a, 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO)))));
    }
    private static boolean check(List<Operation> history) {
        if (history.size() > 20 || history.stream().map(Operation::logicalId).distinct().count() != history.size())
            throw new IllegalArgumentException("bounded logical histories only");
        for (Operation op : history) if (op.start() < 1 || (op.result() == null) != (op.end() == 0) || op.end() != 0 && op.end() <= op.start())
            throw new IllegalArgumentException("malformed history");
        return search(history, 0, new HashMap<>(), new HashSet<>());
    }
    private static boolean search(List<Operation> h, int used, Map<Bytes, Bytes> state, Set<SearchState> visited) {
        if (Integer.bitCount(used) == h.size()) return true;
        if (!visited.add(new SearchState(used, Map.copyOf(state)))) return false;
        for (int i = 0; i < h.size(); i++) {
            if ((used & (1 << i)) != 0) continue;
            Operation op = h.get(i);
            if (op.result() == null && search(h, used | (1 << i), state, visited)) return true; // omit this pending invocation
            boolean eligible = true;
            for (int j = 0; j < h.size(); j++) if ((used & (1 << j)) == 0 && h.get(j).end() != 0 && h.get(j).end() < op.start()) eligible = false;
            if (!eligible) continue;
            Map<Bytes, Bytes> next = new HashMap<>(state); Bytes old = next.get(op.command().key()); boolean valid;
            if (op.command() instanceof Command.Get) valid = op.result() == null || (old == null ? op.result().status() == ResultStatus.MISS
                    : op.result().status() == ResultStatus.OK && op.result().value().equals(Optional.of(old)));
            else if (op.command() instanceof Command.Set set) { valid = op.result() == null || op.result().status() == ResultStatus.OK; next.put(set.key(), set.value()); }
            else { valid = op.result() == null || op.result().status() == (old == null ? ResultStatus.MISS : ResultStatus.OK); next.remove(op.command().key()); }
            if (valid && search(h, used | (1 << i), next, visited)) return true;
        }
        return false;
    }
}
