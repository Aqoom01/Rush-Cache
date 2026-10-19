package io.openkache.server.metrics;

import io.openkache.core.api.ResultStatus;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Prometheus 지표와 {@code /metrics} HTTP endpoint.
 *
 * <p>요청 수({@code openkache_requests_total})와 JVM 기본 지표를 노출한다. label은 listener, opcode, status로 값의 종류가
 * 고정되어 있다. Key나 request ID를 label로 쓰지 않는다. 여러 thread에서 동시에 호출해도 안전하다.
 */
public final class ServerMetrics implements AutoCloseable {

    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private final Counter requests;
    private final HTTPServer httpServer;
    private final ExecutorService httpExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ServerMetrics(Counter requests, HTTPServer httpServer, ExecutorService httpExecutor) {
        this.requests = requests;
        this.httpServer = httpServer;
        this.httpExecutor = httpExecutor;
    }

    /** 새 registry를 만들고 {@code host:port}에서 {@code /metrics}를 연다. */
    public static ServerMetrics start(String host, int port) {
        PrometheusRegistry registry = new PrometheusRegistry();
        JvmMetrics.builder().register(registry);
        Counter requests =
                Counter.builder()
                        .name("openkache_requests_total")
                        .help("Requests handled by the server")
                        .labelNames("listener", "opcode", "status")
                        .register(registry);

        AtomicInteger threadNumber = new AtomicInteger();
        ExecutorService executor =
                Executors.newFixedThreadPool(
                        1,
                        runnable -> new Thread(runnable, "openkache-metrics-http-" + threadNumber.incrementAndGet()));
        try {
            HTTPServer server =
                    HTTPServer.builder()
                            .hostname(host)
                            .port(port)
                            .registry(registry)
                            .executorService(executor)
                            .buildAndStart();
            return new ServerMetrics(requests, server, executor);
        } catch (IOException e) {
            executor.shutdownNow();
            throw new UncheckedIOException("failed to start metrics endpoint on " + host + ":" + port, e);
        }
    }

    /** 처리한 요청 한 건을 기록한다. */
    public void recordRequest(String listener, String opcode, ResultStatus status) {
        requests.labelValues(listener, opcode, status.name().toLowerCase(Locale.ROOT)).inc();
    }

    /** 실제로 열린 port. */
    public int port() {
        return httpServer.getPort();
    }

    /** endpoint를 닫는다. 여러 번 호출해도 안전하다. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        httpServer.close();
        httpExecutor.shutdownNow();
        try {
            httpExecutor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
