package io.openkache.server.metrics;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.spi.MetricsSink;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 *
 * <p>core가 {@link #sink()}로 보내는 지표는 이름 앞에 {@code openkache_}를 붙여 노출한다. 이름은 core 코드의 상수뿐이라 종류가
 * 늘지 않는다. {@code recordNanos}는 이름의 {@code _nanos}를 {@code _seconds}로 바꾼 histogram이다.
 */
public final class ServerMetrics implements AutoCloseable {

    private static final long CLOSE_TIMEOUT_SECONDS = 5;

    private final PrometheusRegistry registry;
    private final Counter requests;
    private final HTTPServer httpServer;
    private final ExecutorService httpExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<String, Counter> coreCounters = new ConcurrentHashMap<>();
    private final Map<String, Histogram> coreDurations = new ConcurrentHashMap<>();
    private final Map<String, Gauge> coreGauges = new ConcurrentHashMap<>();
    private final MetricsSink sink = new CoreSink();

    private ServerMetrics(
            PrometheusRegistry registry, Counter requests, HTTPServer httpServer, ExecutorService httpExecutor) {
        this.registry = registry;
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
            return new ServerMetrics(registry, requests, server, executor);
        } catch (IOException e) {
            executor.shutdownNow();
            throw new UncheckedIOException("failed to start metrics endpoint on " + host + ":" + port, e);
        }
    }

    /** 처리한 요청 한 건을 기록한다. */
    public void recordRequest(String listener, String opcode, ResultStatus status) {
        requests.labelValues(listener, opcode, status.name().toLowerCase(Locale.ROOT)).inc();
    }

    /** core 지표를 이 endpoint로 내보내는 {@link MetricsSink}. */
    public MetricsSink sink() {
        return sink;
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

    private final class CoreSink implements MetricsSink {
        @Override
        public void increment(String name, long delta) {
            coreCounters
                    .computeIfAbsent(name, n -> Counter.builder().name("openkache_" + n).help("core counter " + n).register(registry))
                    .inc(delta);
        }

        @Override
        public void recordNanos(String name, long nanos) {
            String seconds = "openkache_" + name.replaceFirst("_nanos$", "") + "_seconds";
            coreDurations
                    .computeIfAbsent(name, n -> Histogram.builder().name(seconds).help("core duration " + n).register(registry))
                    .observe(nanos / 1_000_000_000.0);
        }

        @Override
        public void gauge(String name, double value) {
            coreGauges
                    .computeIfAbsent(name, n -> Gauge.builder().name("openkache_" + n).help("core gauge " + n).register(registry))
                    .set(value);
        }
    }
}
