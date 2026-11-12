package io.openkache.server.benchmark.variants;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaClientTransport;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import io.openkache.server.ionia.IoniaTcpTransport;
import io.openkache.server.ionia_eighth.IoniaMetaBatchTcpTransport;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Function;

/** 실제 wire profile을 명시해 기존 TCP를 연다. 각 RPC의 새 socket 정책은 유지한다. */
public final class VariantTcp {
    private VariantTcp() {}
    public static <T> T open(O00BenchmarkClient.Settings runtime, String configuration,
            Map<NodeId, InetSocketAddress> addresses, int timeoutMillis, TimeSource time,
            boolean metaBatch, Function<IoniaClientTransport, T> create) {
        Objects.requireNonNull(time); Objects.requireNonNull(create);
        if (!addresses.keySet().equals(Set.copyOf(runtime.replicas())) || runtime.replicas().contains(new NodeId(0))) {
            throw new IllegalArgumentException("address membership mismatch/client node0 conflict");
        }
        IoniaClientTransport raw = metaBatch
                ? new IoniaMetaBatchTcpTransport(new NodeId(0), configuration, addresses, timeoutMillis)
                : new IoniaTcpTransport(new NodeId(0), configuration, addresses, timeoutMillis);
        try { return create.apply(raw); }
        catch (RuntimeException failure) {
            try { ((AutoCloseable) raw).close(); } catch (Exception close) { failure.addSuppressed(close); }
            throw failure;
        }
    }
}
