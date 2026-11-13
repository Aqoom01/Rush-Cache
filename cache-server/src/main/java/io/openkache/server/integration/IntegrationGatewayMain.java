package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.tablet.routing.TableMessageCodec;
import io.openkache.server.observation.TcpTraffic;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** 실제 client v1 요청을 B1/B2/B3와 native replica 그룹에 연결하는 run-local gateway. */
public final class IntegrationGatewayMain implements AutoCloseable {
    private final IntegrationConfig config;
    private final JsonLine journal;
    private final IntegratedCache cache;
    private final ServerSocket listener;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore permits = new Semaphore(2048);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final TcpTraffic.Scope traffic;
    private IntegrationGatewayMain(IntegrationConfig config, Path directory) throws IOException {
        this.config = config;
        Files.createDirectories(directory.getParent()); Files.createDirectory(directory);
        journal = new JsonLine(directory.resolve("events.jsonl"));
        traffic = config.composed() || config.ionia() ? TcpTraffic.open(IoniaProfile.configuration(config.composed() ? config.layer(false) : config, 0), Set.of(1L, 2L, 3L), 100000) : null;
        cache = new IntegratedCache(config, java.util.stream.IntStream.range(0, config.groups()).mapToObj(g -> ReplicatedBackend.connect(config, g)).toList(), journal);
        listener = new ServerSocket();
        try {
            listener.setReuseAddress(true); listener.bind(new InetSocketAddress(config.text("gateway.host", "127.0.0.1"), config.number("gateway.port", 7300)));
            connections.execute(this::accept);
            Files.writeString(directory.resolve("ready.json"), cache.status(), StandardOpenOption.CREATE_NEW);
        } catch (IOException | RuntimeException e) { close(); throw e; }
    }
    /** 새 run 디렉터리만 사용한다. bind 완료는 backend의 quorum readiness를 뜻하지 않는다. */
    public static IntegrationGatewayMain start(IntegrationConfig config, Path directory) throws IOException { return new IntegrationGatewayMain(config, directory.toAbsolutePath()); }
    private void accept() {
        while (!closed.get()) {
            try {
                Socket socket = listener.accept();
                if (!permits.tryAcquire()) { socket.close(); continue; }
                sockets.add(socket); connections.execute(() -> handle(socket));
            } catch (IOException | RejectedExecutionException e) { if (!closed.get()) close(); }
        }
    }
    private void handle(Socket socket) {
        try (socket) {
            socket.setTcpNoDelay(true); socket.setSoTimeout(60000);
            var in = new DataInputStream(socket.getInputStream()); var out = new DataOutputStream(socket.getOutputStream());
            while (!closed.get()) {
                IntegrationWire.Packet packet;
                try { packet = IntegrationWire.read(in); } catch (EOFException done) { break; }
                Result result;
                try {
                    if (packet.messageClass() == 1 && packet.opcode() == 4 && packet.body().length == 0) result = ReplicatedBackend.status(ResultStatus.OK);
                    else if (packet.messageClass() == 1) result = cache.execute(IntegrationWire.decode(packet), packet.version());
                    else if (packet.messageClass() == 3) result = control(packet);
                    else result = ReplicatedBackend.status(ResultStatus.BAD_REQUEST);
                } catch (IllegalArgumentException | CompletionException | IOException e) { result = ReplicatedBackend.status(ResultStatus.BAD_REQUEST); }
                catch (ExecutionException | TimeoutException e) { result = ReplicatedBackend.status(ResultStatus.UNAVAILABLE); }
                IntegrationWire.write(out, IntegrationWire.reply(packet, result, ""));
            }
        } catch (IOException ignored) {
            // EOF/timeout은 해당 연결만 종료한다. 성공 응답을 만들지 않는다.
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        finally { sockets.remove(socket); permits.release(); }
    }
    private Result control(IntegrationWire.Packet packet) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        byte[] value;
        if (!Set.of(IntegrationWire.MOVE, IntegrationWire.SCALE, IntegrationWire.CONFIRM_CRASH, IntegrationWire.RESUME).contains(packet.opcode()) && packet.body().length != 0) return ReplicatedBackend.status(ResultStatus.BAD_REQUEST);
        switch (packet.opcode()) {
            case IntegrationWire.STATUS -> value = status().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            case IntegrationWire.READY -> {
                if (!cache.ready()) return ReplicatedBackend.status(ResultStatus.UNAVAILABLE);
                value = ("{\"runId\":" + JsonLine.quote(config.runId()) + ",\"ready\":true}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case IntegrationWire.BALANCE -> {
                int planned = cache.balance().get(5, TimeUnit.SECONDS); value = ("{\"plannedMoves\":" + planned + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case IntegrationWire.MOVE -> {
                if (packet.body().length != 12) return ReplicatedBackend.status(ResultStatus.BAD_REQUEST);
                ByteBuffer b = ByteBuffer.wrap(packet.body()); long id = cache.move(b.getLong(), b.getInt()); value = ("{\"job\":" + id + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case IntegrationWire.RESUME -> {
                if (packet.body().length != 8) return ReplicatedBackend.status(ResultStatus.BAD_REQUEST);
                boolean resumed = cache.resume(ByteBuffer.wrap(packet.body()).getLong()).get(10, TimeUnit.SECONDS); value = ("{\"resumed\":" + resumed + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case IntegrationWire.SCALE -> {
                if (packet.body().length != 4) return ReplicatedBackend.status(ResultStatus.BAD_REQUEST);
                int planned = cache.scaleTo(ByteBuffer.wrap(packet.body()).getInt()).get(10, TimeUnit.SECONDS); value = ("{\"plannedMoves\":" + planned + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case IntegrationWire.STOP_CONTROL -> { cache.stopControl(); value = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8); }
            case IntegrationWire.CONFIRM_CRASH -> {
                if (packet.body().length != 8) return ReplicatedBackend.status(ResultStatus.BAD_REQUEST);
                ByteBuffer b = ByteBuffer.wrap(packet.body()); long id = cache.confirmedCrash(b.getInt(), b.getInt()); value = ("{\"repair\":" + id + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case IntegrationWire.RESET_METRICS -> { cache.resetMetrics(); if (traffic != null) traffic.stage(TcpTraffic.Stage.MEASURE, System.nanoTime()); value = new byte[0]; }
            case IntegrationWire.TABLE -> value = TableMessageCodec.encodeTable(cache.table());
            default -> { return ReplicatedBackend.status(ResultStatus.BAD_REQUEST); }
        }
        return Result.ok(Bytes.copyOf(value), 0, cache.table().version(), OwnershipEpoch.ZERO);
    }
    String status() {
        String core = cache.status();
        long sent = 0, received = 0, pending = 0; String issues = "[]";
        if (traffic != null) {
            var observation = traffic.snapshot(); pending = observation.pending();
            issues = observation.issues().stream().map(JsonLine::quote).collect(java.util.stream.Collectors.joining(",", "[", "]"));
            for (var row : observation.rows()) {
                sent += row.counts().getOrDefault(TcpTraffic.Metric.SENT_BYTES, 0L); received += row.counts().getOrDefault(TcpTraffic.Metric.RECEIVED_BYTES, 0L);
            }
        }
        return core.substring(0, core.length() - 1) + ",\"nativeTcpObservationEnabled\":" + (traffic != null)
                + ",\"nativeTcpSentStreamBytes\":" + (traffic == null ? "null" : sent) + ",\"nativeTcpReceivedStreamBytes\":" + (traffic == null ? "null" : received)
                + ",\"nativeTcpPending\":" + (traffic == null ? "null" : pending) + ",\"nativeTcpObservationIssues\":" + issues + "}";
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try { listener.close(); } catch (IOException ignored) { }
        sockets.forEach(s -> { try { s.close(); } catch (IOException ignored) { } });
        cache.close(); connections.shutdownNow(); if (traffic != null) traffic.close();
        journal.event("gateway-stopped", "\"status\":" + status());
        try { journal.close(); } catch (IOException ignored) { } stopped.countDown();
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("usage: IntegrationGatewayMain config new-directory");
        try (var gateway = start(IntegrationConfig.load(Path.of(args[0])), Path.of(args[1]))) {
            var hook = new Thread(gateway::close, "integration-gateway-shutdown"); Runtime.getRuntime().addShutdownHook(hook);
            try { gateway.stopped.await(); } finally { try { Runtime.getRuntime().removeShutdownHook(hook); } catch (IllegalStateException ignored) { } }
        }
    }
}
