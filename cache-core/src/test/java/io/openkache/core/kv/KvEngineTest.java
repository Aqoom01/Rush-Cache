package io.openkache.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KvEngineTest {

    private final List<OwnerWorker> workers =
            List.of(new OwnerWorker(new WorkerId(0), 4), new OwnerWorker(new WorkerId(1), 4));
    private final KvEngine engine = new KvEngine(new NodeId(0), workers, TimeSource.system());

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
            KvEngine soloEngine = new KvEngine(new NodeId(0), List.of(solo), TimeSource.system());
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
    void mailboxFullReturnsBusyWithoutBlockingTheCaller() throws InterruptedException {
        try (OwnerWorker solo = new OwnerWorker(new WorkerId(9), 1)) {
            KvEngine soloEngine = new KvEngine(new NodeId(0), List.of(solo), TimeSource.system());
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

    /** COUNT 결과의 value(8-byte big-endian)를 개수로 바꾼다. */
    private static long decodeCount(Result result) {
        assertEquals(ResultStatus.OK, result.status());
        return ByteBuffer.wrap(result.value().orElseThrow().toByteArray()).getLong();
    }

    private static Result waitFor(CompletionStage<Result> stage) {
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
