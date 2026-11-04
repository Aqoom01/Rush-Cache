package io.openkache.server.failure;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.failure.SwimConfig;
import io.openkache.core.failure.SwimMessage;
import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.Transport;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class SwimRuntimeTest {

    @Test
    void schedulerAndInboundMessagesRunDetectorOnOwnerThread() throws Exception {
        NodeId local = new NodeId(1);
        NodeId peer = new NodeId(2);
        RecordingTransport transport = new RecordingTransport();
        SwimConfig config = new SwimConfig(
                Duration.ofMillis(50),
                Duration.ofMillis(20),
                Duration.ofMillis(20),
                Duration.ofMillis(100),
                1,
                1,
                4);
        SwimRuntime runtime =
                new SwimRuntime(local, List.of(local, peer), config, transport, event -> {}, 7, 42);

        try {
            runtime.start();
            runtime.start();
            await(() -> !transport.threadNames.isEmpty());

            assertTrue(runtime.onMessage(peer, new SwimMessage.Ping(99, peer, List.of())));
            await(() -> transport.threadNames.size() >= 2);
            assertTrue(transport.threadNames.stream()
                    .allMatch(name -> name.equals("openkache-worker-7")));
        } finally {
            runtime.close();
            runtime.close();
        }

        assertFalse(runtime.onMessage(peer, new SwimMessage.Ping(100, peer, List.of())));
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met before timeout");
    }

    private static final class RecordingTransport implements Transport {
        private final List<String> threadNames = new CopyOnWriteArrayList<>();

        @Override
        public CompletionStage<Void> send(PeerEnvelope envelope) {
            threadNames.add(Thread.currentThread().getName());
            return CompletableFuture.completedFuture(null);
        }
    }
}
