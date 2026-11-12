package io.openkache.server.ionia_fourth;

import io.openkache.server.ionia.*;
import io.openkache.core.replication.ionia_fourth.*;

import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.*;
import io.openkache.core.spi.TimeSource;
import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** V03 독립 노드 조립부. The existing single-node Netty application remains available for comparison.
 * Restarted nodes must recover from a survivor majority, even when local disk files exist.
 */
public final class IoniaFourthNode implements AutoCloseable {
    private final IoniaConfig config;
    private final IoniaTcpTransport transport;
    private final IoniaFileStorage storage;
    private final OwnerWorker owner;
    private final IoniaFourthEngine engine;
    private final ServerSocket listener;
    private final ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch terminated = new CountDownLatch(1);
    private final String configuration;

    public IoniaFourthNode(IoniaConfig config, Map<NodeId, InetSocketAddress> addresses, Path directory,
                     int storageWorkers, long failureTimeoutMillis, IoniaBatchSettings settings) throws IOException {
        this.config = Objects.requireNonNull(config);
        if (!addresses.keySet().equals(Set.copyOf(config.replicas())) || failureTimeoutMillis < 200)
            throw new IllegalArgumentException("static membership/address mismatch or failure timeout too small");
        configuration = Objects.requireNonNull(settings).configurationId(config);
        TimeSource time = TimeSource.system();
        IoniaFileViewStore stable = new IoniaFileViewStore(directory, configuration + "|node=" + config.nodeId().value());
        storage = new IoniaFileStorage(config.nodeId(), directory, storageWorkers, time, false);
        transport = new IoniaTcpTransport(config.nodeId(), configuration, addresses, 1000);
        owner = new OwnerWorker(new WorkerId(0), 65536);
        engine = new IoniaFourthEngine(config, storage, owner, transport, time, stable, stable.restarted(), stable.view(),
                TimeUnit.MILLISECONDS.toNanos(failureTimeoutMillis), settings);
        listener = new ServerSocket();
        try {
            listener.setReuseAddress(true); listener.bind(addresses.get(config.nodeId()));
            connections.execute(this::accept);
            if (stable.restarted()) engine.startRecovery(UUID.randomUUID());
            timer.scheduleAtFixedRate(() -> engine.onTimer(new TimerEvent(IoniaEngine.TICK, time.monotonicNanos())), 20, 20, TimeUnit.MILLISECONDS);
        } catch (IOException | RuntimeException error) { close(); throw error; }
    }
    public IoniaFourthEngine engine() { return engine; }
    public void awaitTermination() throws InterruptedException { terminated.await(); }
    private void accept() {
        while (!closed.get()) {
            try { Socket socket = listener.accept(); sockets.add(socket); connections.execute(() -> handle(socket)); }
            catch (IOException | RejectedExecutionException error) { if (!closed.get()) close(); }
        }
    }
    private void handle(Socket socket) {
        try (socket) {
            socket.setSoTimeout(5000); socket.setTcpNoDelay(true);
            IoniaWire.Packet packet = IoniaWire.read(socket.getInputStream());
            if (!packet.target().equals(config.nodeId()) || !packet.configuration().equals(configuration)) throw new IOException("wrong destination/configuration");
            DataInputStream in = packet.body(); IoniaWire.Writer reply;
            if (packet.kind() == IoniaWire.PEER) {
                if (!config.replicas().contains(packet.source())) throw new IOException("nonmember peer sender");
                IoniaMessage message = IoniaWire.peer(in); IoniaWire.exhausted(in); engine.onPeerMessage(packet.source(), message); return;
            } else if (packet.kind() == IoniaWire.PROBE) {
                IoniaWire.exhausted(in); IoniaEngine.ViewInfo info = engine.viewInfo();
                reply = out -> { out.writeLong(info.view()); out.writeLong(info.leaderId().value()); out.writeUTF(info.status().name()); };
            } else {
                IoniaClientTransport.Request request = IoniaWire.request(in); IoniaWire.exhausted(in);
                long wait = Math.max(1, Math.min(30_000, TimeUnit.NANOSECONDS.toMillis(request.timeoutNanos()) + 100));
                reply = switch (packet.kind()) {
                    case IoniaWire.RECORD -> {
                        IoniaClientTransport.WriteAck ack = engine.recordWrite(request).toCompletableFuture().get(wait, TimeUnit.MILLISECONDS);
                        yield out -> { IoniaWire.identity(out, ack.view(), ack.clientId(), ack.operationId()); out.writeUTF(ack.status().name()); };
                    }
                    case IoniaWire.EXECUTE -> {
                        IoniaClientTransport.Reply value = engine.execute(request).toCompletableFuture().get(wait, TimeUnit.MILLISECONDS);
                        yield out -> { IoniaWire.identity(out, value.view(), value.clientId(), value.operationId()); IoniaWire.result(out, value.result()); };
                    }
                    case IoniaWire.READ -> {
                        IoniaClientTransport.ReadReply value = engine.followerRead(request).toCompletableFuture().get(wait, TimeUnit.MILLISECONDS);
                        yield out -> { IoniaWire.identity(out, value.view(), value.clientId(), value.operationId()); out.writeLong(value.appliedIndex()); IoniaWire.result(out, value.result()); };
                    }
                    case IoniaWire.META -> {
                        IoniaClientTransport.MetaReply value = engine.metaQuery(request).toCompletableFuture().get(wait, TimeUnit.MILLISECONDS);
                        yield out -> { IoniaWire.identity(out, value.view(), value.clientId(), value.operationId()); out.writeLong(value.index());
                            out.writeBoolean(value.data() != null); if (value.data() != null) IoniaWire.result(out, value.data()); };
                    }
                    default -> throw new IOException("unknown RPC opcode");
                };
            }
            IoniaWire.write(socket.getOutputStream(), packet.correlation(), config.nodeId(), packet.source(), configuration, packet.kind(), reply);
        } catch (IOException | RuntimeException | ExecutionException | TimeoutException ignored) {
            // Malformed/expired RPCs close this connection. No successful ACK is fabricated.
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        finally { sockets.remove(socket); }
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        timer.shutdownNow();
        try { listener.close(); } catch (IOException ignored) { }
        sockets.forEach(socket -> { try { socket.close(); } catch (IOException ignored) { } });
        engine.close(); transport.close(); connections.shutdownNow(); owner.close(); storage.close(); terminated.countDown();
    }
}
