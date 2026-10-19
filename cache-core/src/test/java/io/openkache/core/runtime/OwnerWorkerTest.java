package io.openkache.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.WorkerId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OwnerWorkerTest {

    @Test
    void runsTasksInOrderOnOwnerThread() throws InterruptedException {
        try (OwnerWorker worker = new OwnerWorker(new WorkerId(0), 16)) {
            List<Integer> order = Collections.synchronizedList(new ArrayList<>());
            AtomicBoolean allOnOwner = new AtomicBoolean(true);
            CountDownLatch done = new CountDownLatch(10);

            for (int i = 0; i < 10; i++) {
                int n = i;
                assertTrue(
                        worker.offer(
                                () -> {
                                    allOnOwner.compareAndSet(true, worker.isOwnerThread());
                                    order.add(n);
                                    done.countDown();
                                }));
            }

            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), order);
            assertTrue(allOnOwner.get());
            assertFalse(worker.isOwnerThread());
        }
    }

    @Test
    void offerFailsImmediatelyWhenFull() throws InterruptedException {
        try (OwnerWorker worker = new OwnerWorker(new WorkerId(0), 1)) {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            assertTrue(worker.offer(() -> blockUntil(started, release))); // 실행 중
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue(worker.offer(() -> {})); // 대기 1개 (capacity)
            assertFalse(worker.offer(() -> {})); // 가득 참

            release.countDown();
        }
    }

    @Test
    void workerSurvivesFailingTask() throws InterruptedException {
        try (OwnerWorker worker = new OwnerWorker(new WorkerId(0), 4)) {
            CountDownLatch done = new CountDownLatch(1);

            worker.offer(
                    () -> {
                        throw new IllegalStateException("expected by test");
                    });
            worker.offer(done::countDown);

            assertTrue(done.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void closeIsIdempotentAndStopsThread() {
        OwnerWorker worker = new OwnerWorker(new WorkerId(7), 4);

        worker.close();
        worker.close();

        assertFalse(worker.offer(() -> {}));
        boolean alive =
                Thread.getAllStackTraces().keySet().stream()
                        .anyMatch(t -> t.getName().equals("openkache-worker-7") && t.isAlive());
        assertFalse(alive);
    }

    private static void blockUntil(CountDownLatch started, CountDownLatch release) {
        started.countDown();
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
