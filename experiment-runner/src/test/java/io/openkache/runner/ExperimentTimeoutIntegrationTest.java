package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExperimentTimeoutIntegrationTest {

    @TempDir Path resultsDir;

    @Test
    void waitsForBlockedWorkersToStopBeforeReturningTimeout() throws Exception {
        try (StallingServer server = new StallingServer()) {
            RunAttempt attempt = Main.runOnce(config(server.port()), 1, 300);

            assertEquals(RunStatus.TIMEOUT, attempt.status());
            assertTrue(server.awaitRequest());
            assertTrue(server.awaitWorkerClosed());
            assertEquals(0, server.activeWorkers());

            int receivedBeforeWait = server.requestCount();
            Thread.sleep(100);
            assertEquals(receivedBeforeWait, server.requestCount());
        }
    }

    private RunnerConfig config(int port) {
        Properties properties = new Properties();
        properties.setProperty("target.host", "127.0.0.1");
        properties.setProperty("target.port", String.valueOf(port));
        properties.setProperty("study", "timeout-it");
        properties.setProperty("variant", "baseline");
        properties.setProperty("scenario", "none");
        properties.setProperty("seed", "1");
        properties.setProperty("key.count", "10");
        properties.setProperty("key.length", "16");
        properties.setProperty("value.size", "8");
        properties.setProperty("mix.get", "100");
        properties.setProperty("mix.set", "0");
        properties.setProperty("mix.delete", "0");
        properties.setProperty("distribution", "uniform");
        properties.setProperty("concurrency", "1");
        properties.setProperty("warmup.requests", "0");
        properties.setProperty("measured.requests", "100");
        properties.setProperty("request.timeout.ms", "10000");
        properties.setProperty("results.dir", resultsDir.toString());
        return RunnerConfig.from(properties);
    }

    /** PING에는 응답하지만 실제 요청에는 응답하지 않아 socket read가 block된 상태를 만든다. */
    private static final class StallingServer implements AutoCloseable {
        private final ServerSocket socket;
        private final ExecutorService handlers = Executors.newCachedThreadPool();
        private final AtomicInteger requests = new AtomicInteger();
        private final AtomicInteger activeWorkers = new AtomicInteger();
        private final CountDownLatch requestReceived = new CountDownLatch(1);
        private final CountDownLatch workerClosed = new CountDownLatch(1);

        StallingServer() throws IOException {
            socket = new ServerSocket(0);
            handlers.submit(this::acceptConnections);
        }

        int port() {
            return socket.getLocalPort();
        }

        boolean awaitRequest() throws InterruptedException {
            return requestReceived.await(2, TimeUnit.SECONDS);
        }

        boolean awaitWorkerClosed() throws InterruptedException {
            return workerClosed.await(2, TimeUnit.SECONDS);
        }

        int requestCount() {
            return requests.get();
        }

        int activeWorkers() {
            return activeWorkers.get();
        }

        private void acceptConnections() {
            try {
                while (!socket.isClosed()) {
                    Socket client = socket.accept();
                    handlers.submit(() -> handle(client));
                }
            } catch (IOException ignored) {
                // close()가 accept를 깨운다.
            }
        }

        private void handle(Socket client) {
            boolean workerConnection = false;
            try (client; DataInputStream in = new DataInputStream(client.getInputStream())) {
                int length = in.readInt();
                in.readUnsignedShort();
                in.readUnsignedByte();
                in.readUnsignedByte();
                int operation = in.readUnsignedByte();
                in.readUnsignedByte();
                in.readUnsignedShort();
                long requestId = in.readLong();
                in.readLong();
                in.skipNBytes(length - WireProtocol.HEADER_BYTES);

                if (operation == WireProtocol.OP_PING) {
                    respondToPing(client, requestId);
                    return;
                }

                requests.incrementAndGet();
                activeWorkers.incrementAndGet();
                workerConnection = true;
                requestReceived.countDown();
                while (in.read() != -1) {
                    // client가 취소로 socket을 닫을 때까지 응답하지 않는다.
                }
            } catch (IOException ignored) {
                // 취소 시 socket close로 끝난다.
            } finally {
                if (workerConnection) {
                    activeWorkers.decrementAndGet();
                    workerClosed.countDown();
                }
            }
        }

        private static void respondToPing(Socket client, long requestId) throws IOException {
            DataOutputStream out = new DataOutputStream(client.getOutputStream());
            out.writeInt(48);
            out.writeShort(WireProtocol.MAGIC);
            out.writeByte(WireProtocol.VERSION);
            out.writeByte(WireProtocol.CLASS_RESPONSE);
            out.writeByte(WireProtocol.OP_PING);
            out.writeByte(0);
            out.writeShort(WireProtocol.STATUS_OK);
            out.writeLong(requestId);
            out.writeLong(0);
            out.writeLong(0);
            out.writeLong(0);
            out.writeInt(0);
            out.writeInt(0);
            out.flush();
        }

        @Override
        public void close() throws IOException {
            socket.close();
            handlers.shutdownNow();
            try {
                handlers.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
