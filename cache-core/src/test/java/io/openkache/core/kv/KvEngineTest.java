package io.openkache.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.WorkerId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.TabletRange;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KvEngineTest {

    private final List<OwnerWorker> workers =
            List.of(new OwnerWorker(new WorkerId(0), 4), new OwnerWorker(new WorkerId(1), 4));
    private final KvEngine engine = newEngine(workers);

    /** 시험할 엔진. 하위 클래스가 저장소를 바꿔 같은 테스트를 다시 돌린다. */
    KvEngine newEngine(List<OwnerWorker> workers) {
        return new KvEngine(new NodeId(0), workers, TimeSource.system());
    }

    @AfterEach
    void tearDown() {
        engine.close();
        workers.forEach(OwnerWorker::close);
    }

    @Test
    void getOnMissingKeyIsMiss() {
        Result result = execute(new Command.Get(Bytes.utf8("absent")));

        assertEquals(ResultStatus.MISS, result.status());
        assertTrue(result.value().isEmpty());
    }

    @Test
    void setThenGetReturnsStoredValueWithIncrementingVersion() {
        Bytes key = Bytes.utf8("k");

        Result firstSet = execute(new Command.Set(key, Bytes.utf8("v1")));
        assertEquals(ResultStatus.OK, firstSet.status());
        assertEquals(1, firstSet.valueVersion());

        Result secondSet = execute(new Command.Set(key, Bytes.utf8("v2")));
        assertEquals(2, secondSet.valueVersion());

        Result get = execute(new Command.Get(key));
        assertEquals(ResultStatus.OK, get.status());
        assertEquals(Bytes.utf8("v2"), get.value().orElseThrow());
        assertEquals(2, get.valueVersion());
    }

    @Test
    void deleteExistingKeyRemovesItAndMissingDeleteIsMiss() {
        Bytes key = Bytes.utf8("k");
        execute(new Command.Set(key, Bytes.utf8("v")));

        Result delete = execute(new Command.Delete(key));
        assertEquals(ResultStatus.OK, delete.status());

        Result deleteAgain = execute(new Command.Delete(key));
        assertEquals(ResultStatus.MISS, deleteAgain.status());

        Result get = execute(new Command.Get(key));
        assertEquals(ResultStatus.MISS, get.status());
    }

    @Test
    void existsOnMissingKeyIsMiss() {
        Result result = execute(new Command.Exists(Bytes.utf8("absent")));

        assertEquals(ResultStatus.MISS, result.status());
        assertTrue(result.value().isEmpty());
    }

    @Test
    void existsReturnsOkWithVersionButNoValue() {
        Bytes key = Bytes.utf8("k");
        execute(new Command.Set(key, Bytes.utf8("v1")));
        execute(new Command.Set(key, Bytes.utf8("v2")));

        Result exists = execute(new Command.Exists(key));
        assertEquals(ResultStatus.OK, exists.status());
        assertEquals(2, exists.valueVersion());
        assertTrue(exists.value().isEmpty(), "EXISTS는 값을 복사하지 않는다");

        execute(new Command.Delete(key));
        assertEquals(ResultStatus.MISS, execute(new Command.Exists(key)).status());
    }

    @Test
    void countOnEmptyEngineIsOkWithZero() {
        Result result = execute(new Command.Count());

        assertEquals(ResultStatus.OK, result.status());
        assertEquals(0, decodeCount(result));
        assertEquals(0, result.valueVersion());
    }

    @Test
    void countSumsKeysAcrossAllWorkers() {
        // 두 worker에 Key가 나뉘어 들어가도 합계여야 한다. 같은 Key를 다시 SET해도 개수는 늘지 않는다.
        for (int i = 0; i < 20; i++) {
            execute(new Command.Set(Bytes.utf8("key-" + i), Bytes.utf8("v")));
        }
        execute(new Command.Set(Bytes.utf8("key-0"), Bytes.utf8("overwritten")));
        assertEquals(20, decodeCount(execute(new Command.Count())));

        execute(new Command.Delete(Bytes.utf8("key-3")));
        execute(new Command.Delete(Bytes.utf8("absent")));
        assertEquals(19, decodeCount(execute(new Command.Count())));
    }

    @Test
    void countIsBusyWhenAnyWorkerMailboxIsFull() throws InterruptedException {
        try (OwnerWorker solo = new OwnerWorker(new WorkerId(9), 1)) {
            KvEngine soloEngine = newEngine(List.of(solo));
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            assertTrue(
                    solo.offer(
                            () -> {
                                started.countDown();
                                awaitUninterruptibly(release);
                            }));
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue(solo.offer(() -> {})); // 대기열(capacity 1)을 채운다.

            CompletionStage<Result> rejected = soloEngine.execute(new Command.Count(), context());
            assertEquals(ResultStatus.BUSY, waitFor(rejected).status());

            release.countDown();
            soloEngine.close();
        }
    }

    @Test
    void scanOnEmptyEngineIsOkWithNoEntries() {
        ScanResult result = waitFor(engine.scan(key -> true, 10, context()));

        assertEquals(ResultStatus.OK, result.status());
        assertTrue(result.entries().isEmpty());
        assertFalse(result.truncated());
    }

    @Test
    void scanReturnsAllEntriesAcrossWorkersSortedByKey() {
        // 입력 순서와 worker 분배에 상관없이 Key 바이트 순서로 정렬돼야 한다.
        for (String k : List.of("delta", "alpha", "charlie", "bravo", "echo")) {
            execute(new Command.Set(Bytes.utf8(k), Bytes.utf8("v-" + k)));
        }
        execute(new Command.Set(Bytes.utf8("alpha"), Bytes.utf8("v-alpha-2")));

        ScanResult result = waitFor(engine.scan(key -> true, 100, context()));

        assertEquals(ResultStatus.OK, result.status());
        assertEquals(List.of("alpha", "bravo", "charlie", "delta", "echo"), keysOf(result));
        assertFalse(result.truncated());
        KvEntry alpha = result.entries().get(0);
        assertEquals(Bytes.utf8("v-alpha-2"), alpha.value());
        assertEquals(2, alpha.version());
    }

    @Test
    void scanAppliesKeyFilter() {
        for (String k : List.of("user:1", "user:2", "order:1", "user:3", "order:2")) {
            execute(new Command.Set(Bytes.utf8(k), Bytes.utf8("v")));
        }

        ScanResult result =
                waitFor(engine.scan(key -> new String(key.toByteArray(), StandardCharsets.UTF_8).startsWith("user:"), 100, context()));

        assertEquals(List.of("user:1", "user:2", "user:3"), keysOf(result));
    }

    @Test
    void scanLimitKeepsSmallestKeysGloballyAndMarksTruncated() {
        // worker별로 자른 뒤 합쳐도 전체 기준 앞 limit개여야 한다.
        for (int i = 0; i < 20; i++) {
            execute(new Command.Set(Bytes.utf8(String.format("k%02d", i)), Bytes.utf8("v")));
        }

        ScanResult result = waitFor(engine.scan(key -> true, 3, context()));

        assertEquals(List.of("k00", "k01", "k02"), keysOf(result));
        assertTrue(result.truncated());

        ScanResult exact = waitFor(engine.scan(key -> true, 20, context()));
        assertEquals(20, exact.entries().size());
        assertFalse(exact.truncated());
    }

    @Test
    void rangeIsStartInclusiveEndExclusiveAndOpenEndedWhenEmpty() {
        for (String k : List.of("a", "b", "c", "d", "e")) {
            execute(new Command.Set(Bytes.utf8(k), Bytes.utf8("v")));
        }

        ScanResult middle = waitFor(engine.range(Optional.of(Bytes.utf8("b")), Optional.of(Bytes.utf8("d")), 10, context()));
        assertEquals(List.of("b", "c"), keysOf(middle));

        ScanResult fromC = waitFor(engine.range(Optional.of(Bytes.utf8("c")), Optional.empty(), 10, context()));
        assertEquals(List.of("c", "d", "e"), keysOf(fromC));

        ScanResult untilC = waitFor(engine.range(Optional.empty(), Optional.of(Bytes.utf8("c")), 10, context()));
        assertEquals(List.of("a", "b"), keysOf(untilC));

        ScanResult all = waitFor(engine.range(Optional.empty(), Optional.empty(), 10, context()));
        assertEquals(List.of("a", "b", "c", "d", "e"), keysOf(all));
    }

    @Test
    void rangeRejectsStartNotBeforeEnd() {
        assertThrows(
                IllegalArgumentException.class,
                () -> engine.range(Optional.of(Bytes.utf8("b")), Optional.of(Bytes.utf8("a")), 10, context()));
        assertThrows(IllegalArgumentException.class, () -> engine.scan(key -> true, 0, context()));
    }

    @Test
    void scanCanSelectTabletByHashRangeWithoutKvKnowingTablets() {
        // B3 사용 예: Tablet(해시 범위)에 속한 항목만 고른다. KV는 KeyHash·TabletRange를 모르고 predicate만 받는다.
        List<TabletRange> halves = TabletRange.uniform(2);
        List<Bytes> keys = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Bytes key = Bytes.utf8("key-" + i);
            keys.add(key);
            execute(new Command.Set(key, Bytes.utf8("v")));
        }

        ScanResult left = waitFor(engine.scan(key -> halves.get(0).contains(KeyHash.of(key)), 100, context()));
        ScanResult right = waitFor(engine.scan(key -> halves.get(1).contains(KeyHash.of(key)), 100, context()));

        assertEquals(50, left.entries().size() + right.entries().size());
        for (KvEntry entry : left.entries()) {
            assertTrue(halves.get(0).contains(KeyHash.of(entry.key())));
        }
        for (KvEntry entry : right.entries()) {
            assertTrue(halves.get(1).contains(KeyHash.of(entry.key())));
        }
        assertTrue(keys.stream().allMatch(k -> halves.get(0).contains(KeyHash.of(k)) || halves.get(1).contains(KeyHash.of(k))));
    }

    @Test
    void scanIsBusyWhenAnyWorkerMailboxIsFullAndUnavailableAfterClose() throws InterruptedException {
        try (OwnerWorker solo = new OwnerWorker(new WorkerId(9), 1)) {
            KvEngine soloEngine = newEngine(List.of(solo));
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            assertTrue(
                    solo.offer(
                            () -> {
                                started.countDown();
                                awaitUninterruptibly(release);
                            }));
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue(solo.offer(() -> {})); // 대기열(capacity 1)을 채운다.

            assertEquals(ResultStatus.BUSY, waitFor(soloEngine.scan(key -> true, 10, context())).status());

            release.countDown();
            soloEngine.close();
            assertEquals(ResultStatus.UNAVAILABLE, waitFor(soloEngine.scan(key -> true, 10, context())).status());
        }
    }

    @Test
    void mailboxFullReturnsBusyWithoutBlockingTheCaller() throws InterruptedException {
        try (OwnerWorker solo = new OwnerWorker(new WorkerId(9), 1)) {
            KvEngine soloEngine = newEngine(List.of(solo));
            Bytes key = Bytes.utf8("only-worker-key");
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            assertTrue(
                    solo.offer(
                            () -> {
                                started.countDown();
                                awaitUninterruptibly(release);
                            }));
            assertTrue(started.await(5, TimeUnit.SECONDS));

            // capacity 1: 이 요청은 대기열에 들어간다 (아직 실행되지 않음).
            CompletionStage<Result> queued = soloEngine.execute(new Command.Get(key), context());
            // 대기열이 이미 가득 차 있으므로 이 요청은 즉시 BUSY로 끝난다.
            CompletionStage<Result> rejected = soloEngine.execute(new Command.Get(key), context());
            assertEquals(ResultStatus.BUSY, waitFor(rejected).status());

            release.countDown();
            assertEquals(ResultStatus.MISS, waitFor(queued).status());

            soloEngine.close();
        }
    }

    @Test
    void expiredDeadlineIsTimeoutInsteadOfExecuting() {
        RequestContext expired =
                new RequestContext(
                        new ClientId(1),
                        new OperationId(1),
                        RoutingVersion.ZERO,
                        OwnershipEpoch.ZERO,
                        new Deadline(TimeSource.system().monotonicNanos() - 1));

        Result result = waitFor(engine.execute(new Command.Get(Bytes.utf8("k")), expired));

        assertEquals(ResultStatus.TIMEOUT, result.status());
    }

    @Test
    void closeCompletesPendingAndRejectsNewRequestsWithUnavailable() {
        engine.close();
        engine.close(); // 여러 번 호출해도 안전하다.

        Result result = waitFor(engine.execute(new Command.Get(Bytes.utf8("k")), context()));

        assertEquals(ResultStatus.UNAVAILABLE, result.status());
    }

    private Result execute(Command command) {
        return waitFor(engine.execute(command, context()));
    }

    private static List<String> keysOf(ScanResult result) {
        assertEquals(ResultStatus.OK, result.status());
        return result.entries().stream()
                .map(entry -> new String(entry.key().toByteArray(), StandardCharsets.UTF_8))
                .toList();
    }

    /** COUNT 결과의 value(8-byte big-endian)를 개수로 바꾼다. */
    private static long decodeCount(Result result) {
        assertEquals(ResultStatus.OK, result.status());
        return ByteBuffer.wrap(result.value().orElseThrow().toByteArray()).getLong();
    }

    private static <T> T waitFor(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static RequestContext context() {
        return new RequestContext(
                new ClientId(1),
                new OperationId(1),
                RoutingVersion.ZERO,
                OwnershipEpoch.ZERO,
                Deadline.after(TimeSource.system(), Duration.ofSeconds(5)));
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
