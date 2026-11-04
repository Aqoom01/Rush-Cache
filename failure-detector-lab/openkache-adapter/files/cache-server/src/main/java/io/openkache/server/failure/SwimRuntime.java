package io.openkache.server.failure;

import io.openkache.core.failure.MemberRecord;
import io.openkache.core.failure.MembershipListener;
import io.openkache.core.failure.SwimConfig;
import io.openkache.core.failure.SwimFailureDetector;
import io.openkache.core.failure.SwimMessage;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.WorkerId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.spi.Transport;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SWIM detector, 전용 owner worker와 tick scheduler의 lifecycle을 묶는다. */
public final class SwimRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SwimRuntime.class);
    private static final int MAILBOX_CAPACITY = 1_024;
    private static final long LIFECYCLE_TIMEOUT_SECONDS = 5;

    private final NodeId localNode;
    private final Set<NodeId> members;
    private final SwimFailureDetector detector;
    private final Transport transport;
    private final OwnerWorker owner;
    private final ScheduledExecutorService scheduler;
    private final long initialDelayNanos;
    private final long tickPeriodNanos;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public SwimRuntime(
            NodeId localNode,
            Collection<NodeId> members,
            SwimConfig config,
            Transport transport,
            MembershipListener listener,
            int workerId,
            long randomSeed) {
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.members = Set.copyOf(Objects.requireNonNull(members, "members"));
        this.transport = Objects.requireNonNull(transport, "transport");
        this.owner = new OwnerWorker(new WorkerId(workerId), MAILBOX_CAPACITY);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "openkache-swim-scheduler");
            thread.setDaemon(false);
            return thread;
        });
        this.detector = new SwimFailureDetector(
                localNode,
                this.members,
                Objects.requireNonNull(config, "config"),
                transport,
                TimeSource.system(),
                Objects.requireNonNull(listener, "listener"),
                randomSeed);
        this.initialDelayNanos = config.probeInterval().toNanos();
        this.tickPeriodNanos = minimum(
                        config.probeInterval(),
                        config.directProbeTimeout(),
                        config.indirectProbeTimeout(),
                        config.suspicionTimeout())
                .toNanos();
    }

    /** detector를 owner thread에서 시작하고 주기적인 tick 등록을 시작한다. 여러 번 호출해도 안전하다. */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("SWIM runtime is closed");
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture<Void> activation = new CompletableFuture<>();
        if (!owner.offer(() -> {
            detector.start();
            activation.complete(null);
        })) {
            started.set(false);
            throw new IllegalStateException("SWIM owner mailbox rejected start");
        }
        await(activation, "start SWIM detector");
        scheduler.scheduleAtFixedRate(
                this::enqueueTick, initialDelayNanos, tickPeriodNanos, TimeUnit.NANOSECONDS);
    }

    /** Netty EventLoop 등 외부 thread에서 받은 메시지를 owner mailbox로 넘긴다. */
    public boolean onMessage(NodeId from, SwimMessage message) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(message, "message");
        return started.get() && !closed.get() && owner.offer(() -> detector.onMessage(from, message));
    }

    public NodeId localNode() {
        return localNode;
    }

    public boolean isMember(NodeId nodeId) {
        return members.contains(nodeId);
    }

    /** owner thread에서 일관된 멤버십 스냅샷을 가져온다. */
    public CompletionStage<Map<NodeId, MemberRecord>> snapshot() {
        CompletableFuture<Map<NodeId, MemberRecord>> result = new CompletableFuture<>();
        if (closed.get() || !owner.offer(() -> result.complete(detector.snapshot()))) {
            result.completeExceptionally(new IllegalStateException("SWIM runtime is not available"));
        }
        return result;
    }

    private void enqueueTick() {
        if (!closed.get() && !owner.offer(detector::tick)) {
            LOG.warn("SWIM tick was dropped because the owner mailbox is unavailable");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(LIFECYCLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (transport instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.warn("failed to close SWIM transport", e);
            }
        }
        owner.close();
    }

    private static Duration minimum(Duration first, Duration... rest) {
        Duration result = first;
        for (Duration candidate : rest) {
            if (candidate.compareTo(result) < 0) {
                result = candidate;
            }
        }
        return result;
    }

    private static void await(CompletableFuture<Void> future, String action) {
        try {
            future.get(LIFECYCLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while attempting to " + action, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("failed to " + action, e);
        }
    }
}
