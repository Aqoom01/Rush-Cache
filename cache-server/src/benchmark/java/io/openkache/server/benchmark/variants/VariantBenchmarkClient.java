package io.openkache.server.benchmark.variants;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.LogicalRequest;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.measurement.BenchmarkRunWriter;
import io.openkache.server.benchmark.ionia.*;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** 기존 O00 runtime의 수명주기를 재사용하는 파생 adapter 공통부. public 메서드는 thread-safe다. */
public abstract class VariantBenchmarkClient implements BenchmarkClient {
    private final String variant, configuration;
    private final O00BenchmarkClient.Settings settings;
    private final O00BenchmarkClient runtime;
    private final Map<String, String> metadata;
    private final Supplier<Map<String, Long>> protocolPending;

    protected VariantBenchmarkClient(String variant, O00BenchmarkClient.Settings settings,
            String configuration, Map<String, String> policy, IoniaClientTransport raw, TimeSource time,
            IoniaBenchmarkSession.Factory factory, Supplier<Map<String, Long>> protocolPending) {
        this.variant = Objects.requireNonNull(variant); this.settings = Objects.requireNonNull(settings);
        this.configuration = Objects.requireNonNull(configuration);
        this.protocolPending = Objects.requireNonNull(protocolPending);
        TreeMap<String, String> m = new TreeMap<>(policy);
        m.put("variant", variant); m.put("wire_configuration", configuration);
        m.put("run_id", settings.runId()); m.put("load_slot", settings.loadSlot());
        m.put("stream_count", Integer.toString(settings.streamCount())); m.put("wire_client_base", Long.toString(settings.wireClientBase()));
        m.put("replicas", settings.replicas().stream().map(n -> Long.toString(n.value())).reduce((a,b) -> a + "," + b).orElseThrow());
        m.put("initial_leader", Long.toString(settings.leader().value())); m.put("runtime_read_replica", Long.toString(settings.readReplica().value()));
        m.put("total_timeout_ns", Long.toString(settings.totalTimeoutNanos())); m.put("max_rounds", Integer.toString(settings.maxRounds()));
        m.put("retry_backoff_ns", Long.toString(settings.retryBackoffNanos())); m.put("logical_limit", Integer.toString(settings.maxInFlight()));
        m.put("normal_rpc_limit", Integer.toString(settings.maxOutstandingRpc())); m.put("timer_period_ns", Long.toString(settings.timerPeriodNanos()));
        m.put("tcp_socket_policy", "new_socket_per_rpc"); metadata = Collections.unmodifiableMap(m);
        runtime = factory == null ? new O00BenchmarkClient(settings, raw, time) : new O00BenchmarkClient(settings, raw, time, factory);
    }
    public final String variant() { return variant; }
    public final String configuration() { return configuration; }
    public final O00BenchmarkClient.Settings settings() { return settings; }
    public final Map<String, String> adapterConfiguration() { return metadata; }
    public final O00BenchmarkClient.Diagnostics diagnostics() { return runtime.diagnostics(); }
    /** 실행기에서 보이지 않는 파생 내부 대기를 포함한다. logical timeout으로 raw 상태를 지우지 않는다. */
    public final Map<String, Long> protocolPending() { return Map.copyOf(protocolPending.get()); }
    public final BenchmarkRunWriter.Metadata metadata(String scenario, String clockId,
            String sourceCommit, Boolean sourceDirty, Set<String> requiredMetrics) {
        return new BenchmarkRunWriter.Metadata(variant, scenario, settings.replicas().size(), sourceCommit,
                sourceDirty, clockId, requiredMetrics, metadata);
    }
    @Override public final CompletionStage<ClientOutcome> submit(LogicalRequest request) { return runtime.submit(request); }
    @Override public final boolean cancel(LogicalRequest.RequestId id) { return runtime.cancel(id); }
    @Override public final ClientAccounting.Snapshot snapshot() { return runtime.snapshot(); }
    @Override public final void close() { runtime.close(); }
    protected final LocalVariantDrainProbe probe(Map<NodeId, Supplier<LocalVariantDrainProbe.NodeSnapshot>> nodes) {
        return new LocalVariantDrainProbe(this, nodes, settings.leader(), 0);
    }
    public static IoniaConfig membership(O00BenchmarkClient.Settings runtime) {
        return new IoniaConfig(runtime.leader(), runtime.leader(), runtime.replicas(), 0);
    }
    /** 실제 transport timeout은 open에서 기록할 추가 정책으로 조합한다. */
    public static Map<String, String> policy(String... pairs) {
        Map<String, String> result = new TreeMap<>();
        for (int i=0;i<pairs.length;i+=2) { result.put(pairs[i], pairs[i+1]); } return Map.copyOf(result);
    }
}
