package io.openkache.core.runtime;

import io.openkache.core.model.WorkerId;
import java.lang.System.Logger.Level;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 전용 thread 하나와 크기 제한 {@link Mailbox}를 가진 worker.
 *
 * <p>이 worker가 소유한 상태는 이 worker의 thread에서만 변경한다. 다른 thread는 상태를 직접 건드리지 않고
 * {@link #offer(Runnable)}로 작업을 넘긴다. 작업이 예외를 던져도 thread는 유지되며 예외는 로그로 남는다.
 */
public final class OwnerWorker implements Mailbox, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(OwnerWorker.class.getName());
    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private final WorkerId id;
    private final int capacity;
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Thread ownerThread;

    /**
     * thread를 시작한다.
     *
     * @param capacity 대기 작업 최대 수. 1 이상
     */
    public OwnerWorker(WorkerId id, int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1: " + capacity);
        }
        this.id = id;
        this.capacity = capacity;
        this.executor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(capacity),
                        runnable -> {
                            Thread thread = new Thread(runnable, "openkache-worker-" + id.value());
                            ownerThread = thread;
                            return thread;
                        });
        executor.prestartCoreThread();
    }

    public WorkerId id() {
        return id;
    }

    @Override
    public boolean offer(Runnable task) {
        if (closed.get()) {
            return false;
        }
        try {
            executor.execute(() -> runSafely(task));
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    /** 현재 thread가 이 worker의 owner thread이면 {@code true}. 상태 변경 전 검사에 쓴다. */
    public boolean isOwnerThread() {
        return Thread.currentThread() == ownerThread;
    }

    /**
     * 새 작업을 거절하고, 대기 중인 작업은 버리고, thread가 끝날 때까지 최대 5초 기다린다.
     *
     * <p>여러 번 호출해도 안전하며 두 번째 호출부터는 아무 일도 하지 않는다. owner thread 안에서 호출하면 종료를 기다리지 않는다.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        executor.shutdownNow();
        if (isOwnerThread()) {
            return;
        }
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOG.log(Level.WARNING, "worker {0} did not stop within timeout", id.value());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runSafely(Runnable task) {
        try {
            task.run();
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, "task failed on worker " + id.value(), e);
        }
    }
}
