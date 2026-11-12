package io.openkache.server.ionia;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import io.openkache.server.observation.TcpTraffic;

/** Real TCP transport. Blocking sockets run on virtual threads, never the protocol owner or Netty loop.
 * A PEER stage completes on local flush; only explicit PrepareAck messages contribute to consensus.
 */
public final class IoniaTcpTransport implements Transport, IoniaClientTransport, AutoCloseable {
    @FunctionalInterface private interface Decoder<T> { T read(NodeId from, DataInputStream in) throws IOException; }
    private final NodeId self;
    private final String configuration;
    private final Map<NodeId, InetSocketAddress> addresses;
    private final ExecutorService io = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final int timeoutMillis;

    public IoniaTcpTransport(NodeId self, String configuration, Map<NodeId, InetSocketAddress> addresses, int timeoutMillis) {
        this.self = Objects.requireNonNull(self); this.configuration = Objects.requireNonNull(configuration);
        this.addresses = Map.copyOf(addresses);
        if (timeoutMillis < 1 || timeoutMillis > 60_000) throw new IllegalArgumentException("invalid TCP timeout");
        this.timeoutMillis = timeoutMillis;
    }
    @Override public CompletionStage<Void> send(PeerEnvelope envelope) {
        if (!self.equals(envelope.from()) || !(envelope.message() instanceof IoniaMessage message))
            return CompletableFuture.failedFuture(new IllegalArgumentException("invalid peer sender/message"));
        return call(envelope.to(), IoniaWire.PEER, timeoutMillis, out -> IoniaWire.peer(out, message), null, message.getClass().getSimpleName());
    }
    @Override public CompletionStage<WriteAck> record(NodeId to, Request request) {
        return call(to, IoniaWire.RECORD, timeout(request), out -> IoniaWire.request(out, request), (node, in) ->
                new WriteAck(node, in.readLong(), new ClientId(in.readLong()), new OperationId(in.readLong()), ResultStatus.valueOf(in.readUTF())));
    }
    @Override public CompletionStage<Reply> execute(NodeId to, Request request) {
        return call(to, IoniaWire.EXECUTE, timeout(request), out -> IoniaWire.request(out, request), (node, in) ->
                new Reply(node, in.readLong(), new ClientId(in.readLong()), new OperationId(in.readLong()), IoniaWire.result(in)));
    }
    @Override public CompletionStage<ReadReply> followerRead(NodeId to, Request request) {
        return call(to, IoniaWire.READ, timeout(request), out -> IoniaWire.request(out, request), (node, in) ->
                new ReadReply(node, in.readLong(), new ClientId(in.readLong()), new OperationId(in.readLong()), in.readLong(), IoniaWire.result(in)));
    }
    @Override public CompletionStage<MetaReply> metaQuery(NodeId to, Request request) {
        return call(to, IoniaWire.META, timeout(request), out -> IoniaWire.request(out, request), (node, in) -> {
            long view = in.readLong(); ClientId client = new ClientId(in.readLong()); OperationId operation = new OperationId(in.readLong());
            long index = in.readLong(); return new MetaReply(node, view, client, operation, index, in.readBoolean() ? IoniaWire.result(in) : null);
        });
    }
    @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) {
        return call(to, IoniaWire.PROBE, timeoutMillis, out -> { }, (node, in) ->
                new IoniaEngine.ViewInfo(in.readLong(), new NodeId(in.readLong()), IoniaEngine.Status.valueOf(in.readUTF())));
    }
    private int timeout(Request request) {
        return (int) Math.max(1, Math.min(timeoutMillis, TimeUnit.NANOSECONDS.toMillis(request.timeoutNanos()) + 1));
    }
    private <T> CompletionStage<T> call(NodeId to, int kind, int timeout, IoniaWire.Writer payload, Decoder<T> decoder) {
        return call(to, kind, timeout, payload, decoder, "");
    }
    private <T> CompletionStage<T> call(NodeId to, int kind, int timeout, IoniaWire.Writer payload, Decoder<T> decoder, String peerType) {
        CompletableFuture<T> future = new CompletableFuture<>();
        var traffic = TcpTraffic.exchange(configuration, self.value(), to.value(), kind, peerType);
        if (closed.get() || !addresses.containsKey(to)) { traffic.rejected(); future.completeExceptionally(new IOException("transport closed/unknown replica")); return future; }
        long correlation = sequence.incrementAndGet();
        try {
            io.execute(() -> {
                try (Socket socket = new Socket()) {
                    traffic.created(socket);
                    sockets.add(socket);
                    if (closed.get()) throw new IOException("transport closed");
                    traffic.connecting(); socket.connect(addresses.get(to), Math.min(timeout, 500)); traffic.connected(); socket.setSoTimeout(timeout); socket.setTcpNoDelay(true);
                    IoniaWire.write(traffic.output(socket.getOutputStream()), correlation, self, to, configuration, kind, payload); traffic.written();
                    if (decoder == null) { traffic.succeeded(); future.complete(null); return; }
                    IoniaWire.Packet reply = IoniaWire.read(traffic.input(socket.getInputStream())); traffic.replyRead();
                    if (reply.correlation() != correlation || !reply.source().equals(to) || !reply.target().equals(self)
                            || !reply.configuration().equals(configuration) || reply.kind() != kind) throw new IOException("response correlation/configuration mismatch");
                    T value = decoder.read(to, reply.body()); IoniaWire.exhausted(reply.body()); traffic.succeeded(); future.complete(value);
                } catch (IOException | RuntimeException error) { future.completeExceptionally(error); }
                finally { traffic.close(); sockets.removeIf(Socket::isClosed); }
            });
        } catch (RejectedExecutionException error) { traffic.rejected(); future.completeExceptionally(error); }
        return future;
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        sockets.forEach(socket -> { try { socket.close(); } catch (IOException ignored) { } });
        io.shutdownNow();
    }
}
