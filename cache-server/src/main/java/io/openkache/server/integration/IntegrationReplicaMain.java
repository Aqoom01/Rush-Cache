package io.openkache.server.integration;

import com.sun.net.httpserver.HttpServer;
import io.openkache.core.failure.MemberStatus;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.server.observation.TcpTraffic;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** 실제 replica JVM 진입점: config group(0..groups.count-1) replica(0..2) NEW_DIRECTORY. */
public final class IntegrationReplicaMain {
    private IntegrationReplicaMain() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("usage: IntegrationReplicaMain config group index new-directory");
        IntegrationConfig config = IntegrationConfig.load(Path.of(args[0]));
        int group = Integer.parseInt(args[1]), index = Integer.parseInt(args[2]);
        if (group < 0 || group >= config.groups() || index < 0 || index > 2) throw new IllegalArgumentException("invalid replica role");
        Path directory = Path.of(args[3]).toAbsolutePath();
        var layers = new java.util.ArrayList<AutoCloseable>();
        try {
            if (config.composed()) {
                Files.createDirectories(directory.getParent()); Files.createDirectory(directory);
                layers.add(startNative(config.layer(true), group, index, directory.resolve("a1-authority")));
                layers.add(startNative(config.layer(false), group, index, directory.resolve("a2-data")));
            } else layers.add(startNative(config, group, index, directory));
            var stopped = new CountDownLatch(1);
            Runnable close = () -> { for (int i = layers.size() - 1; i >= 0; i--) try { layers.get(i).close(); } catch (Exception ignored) { } stopped.countDown(); };
            Thread hook = new Thread(close, "openkache-replica-shutdown"); Runtime.getRuntime().addShutdownHook(hook);
            try { stopped.await(); } finally { close.run(); try { Runtime.getRuntime().removeShutdownHook(hook); } catch (IllegalStateException ignored) { } }
        } finally { for (int i = layers.size() - 1; i >= 0; i--) layers.get(i).close(); }
    }
    /** Starts one native layer. Both layers own separate storage, consensus state and ports. */
    static AutoCloseable startNative(IntegrationConfig config, int group, int index, Path directory) throws Exception {
        return startNative(config, group, index, directory, true);
    }
    static AutoCloseable startNative(IntegrationConfig config, int group, int index, Path directory, boolean observeTcp) throws Exception {
        Files.createDirectories(directory.getParent()); Files.createDirectory(directory);
        var resources = new java.util.ArrayList<AutoCloseable>(); boolean success = false;
        try {
        var journal = new JsonLine(directory.resolve("events.jsonl"));
        resources.add(journal);
        {
            var stopped = new CountDownLatch(1); AtomicBoolean closed = new AtomicBoolean();
            Path storage = nativeStoragePath(config, group, index, directory);
            IoniaProfile.Replica ionia = config.ionia() ? IoniaProfile.replica(config, group, index, storage) : null;
            A1Replica a1 = config.ionia() ? null : new A1Replica(config, group, index, storage);
            if (ionia != null) resources.add(ionia); if (a1 != null) resources.add(a1);
            TcpTraffic.Scope traffic = ionia == null || !observeTcp ? null : TcpTraffic.open(IoniaProfile.configuration(config, group), Set.of(1L, 2L, 3L), 100000);
            if (traffic != null) resources.add(traffic);
            FailureRuntime bridge = config.externalFd() ? FailureRuntime.start(config, group, index, event -> {
                    if (ionia != null) {
                        IoniaEngine.ViewInfo view = ionia.view().get();
                        if (event.current() == MemberStatus.DEAD && event.member().equals(view.leaderId()) && view.status() == IoniaEngine.Status.NORMAL) {
                            journal.event("external-fd-view-change", "\"view\":" + view.view() + ",\"failedLeader\":" + view.leaderId().value());
                            ionia.startViewChange().accept(Math.incrementExact(view.view()));
                        }
                    } else a1.membership(event);
                }, journal) : null;
            if (bridge != null) resources.add(bridge);
            Supplier<String> status = () -> {
                String nativeStatus;
                if (ionia != null) {
                    var v = ionia.view().get(); var p = ionia.progress().get();
                    nativeStatus = "{\"node\":" + (index + 1) + ",\"leader\":" + v.leaderId().value() + ",\"view\":" + v.view()
                            + ",\"role\":" + JsonLine.quote(v.status().name()) + ",\"recorded\":" + p.recordedCount() + ",\"commit\":" + p.commitIndex() + ",\"applied\":" + p.appliedIndex() + "}";
                } else nativeStatus = a1.json();
                long sent = 0, received = 0, pending = 0; String issues = "[]";
                if (traffic != null) {
                    var observation = traffic.snapshot(); pending = observation.pending();
                    issues = observation.issues().stream().map(JsonLine::quote).collect(java.util.stream.Collectors.joining(",", "[", "]"));
                    for (var row : observation.rows()) { sent += row.counts().getOrDefault(TcpTraffic.Metric.SENT_BYTES, 0L); received += row.counts().getOrDefault(TcpTraffic.Metric.RECEIVED_BYTES, 0L); }
                }
                return "{\"runId\":" + JsonLine.quote(config.runId()) + ",\"group\":" + group + ",\"index\":" + index + ",\"pid\":" + ProcessHandle.current().pid()
                        + ",\"protocol\":" + JsonLine.quote(config.protocol()) + ",\"native\":" + nativeStatus + ",\"fd\":" + (bridge == null ? "null" : bridge.json())
                        + ",\"tcpObservationEnabled\":" + (traffic != null) + ",\"tcpSentStreamBytes\":" + (traffic == null ? "null" : sent)
                        + ",\"tcpReceivedStreamBytes\":" + (traffic == null ? "null" : received) + ",\"tcpPending\":" + (traffic == null ? "null" : pending) + ",\"tcpObservationIssues\":" + issues
                        + ",\"processCpuNanos\":" + ProcessHandle.current().info().totalCpuDuration().map(java.time.Duration::toNanos).orElse(-1L)
                        + ",\"processors\":" + Runtime.getRuntime().availableProcessors() + ",\"heapUsedBytes\":" + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() + "}";
            };
            var http = HttpServer.create(new InetSocketAddress(config.hosts(group).get(index), config.number("replica.port", 17101) + 4), 64);
            resources.add(() -> http.stop(0));
            http.createContext("/status", exchange -> {
                byte[] body = status.get().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json"); exchange.sendResponseHeaders(200, body.length);
                try (var out = exchange.getResponseBody()) { out.write(body); }
            });
            var httpWorkers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor(); resources.add(httpWorkers::shutdownNow); http.setExecutor(httpWorkers);
            http.createContext("/master", exchange -> {
                int code = 200; String answer;
                try {
                    if (!exchange.getRequestMethod().equals("POST") || !config.runId().equals(exchange.getRequestHeaders().getFirst("X-Openkache-Run")) || a1 == null) throw new IllegalArgumentException("wrong master method/run/engine");
                    var params = new java.util.HashMap<String, String>(); String query = exchange.getRequestURI().getRawQuery();
                    if (query == null || query.length() > 4096) throw new IllegalArgumentException("master query required");
                    for (String part : query.split("&")) { String[] pair = part.split("=", 2); if (pair.length != 2 || params.put(java.net.URLDecoder.decode(pair[0], StandardCharsets.UTF_8), java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8)) != null) throw new IllegalArgumentException("invalid master query"); }
                    String detail = a1.master(params).toCompletableFuture().get(config.number("request.timeout.ms", 3000), java.util.concurrent.TimeUnit.MILLISECONDS);
                    journal.event("master-control", "\"op\":" + JsonLine.quote(params.get("op")) + ",\"epoch\":" + params.get("epoch"));
                    answer = "{\"runId\":" + JsonLine.quote(config.runId()) + "," + detail + "}";
                } catch (Exception failure) { code = 409; answer = "{\"runId\":" + JsonLine.quote(config.runId()) + ",\"error\":" + JsonLine.quote(failure.toString()) + "}"; }
                byte[] body = answer.getBytes(StandardCharsets.UTF_8); exchange.sendResponseHeaders(code, body.length);
                try (var out = exchange.getResponseBody()) { out.write(body); }
            });
            http.start();
            Runnable close = () -> {
                if (!closed.compareAndSet(false, true)) return;
                http.stop(0); httpWorkers.shutdownNow(); if (bridge != null) bridge.close(); if (ionia != null) ionia.close(); if (a1 != null) a1.close();
                if (traffic != null) traffic.close(); try { journal.close(); } catch (java.io.IOException ignored) { } stopped.countDown();
            };
            Files.writeString(directory.resolve("ready.json"), status.get(), StandardOpenOption.CREATE_NEW);
            journal.event("replica-started", "\"protocol\":" + JsonLine.quote(config.protocol()) + ",\"group\":" + group + ",\"index\":" + index);
            success = true; return close::run;
        }
        } finally { if (!success) for (int i = resources.size() - 1; i >= 0; i--) try { resources.get(i).close(); } catch (Exception ignored) { } }
    }
    /** Keep experiment evidence on EBS while placing only disposable cache files on NVMe. */
    static Path nativeStoragePath(IntegrationConfig config, int group, int index, Path evidence) {
        String root = config.text("native.data.root", "");
        return root.isEmpty() ? evidence.resolve("data") : Path.of(root).resolve(config.runId())
                .resolve("g" + group + "-n" + index).resolve(config.protocol());
    }
}
