package io.openkache.server.benchmark.ionia_second;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaConfig;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.ionia.IoniaTcpTransport;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/** V01이 재사용하는 기존 O00 서버/wire/TCP를 연결한다. RPC별 새 socket 정책을 유지한다. */
public final class V01TcpClients {
    private V01TcpClients() {}
    public static V01BenchmarkClient open(V01BenchmarkClient.Settings settings,
            Map<NodeId, InetSocketAddress> addresses, int socketTimeoutMillis, TimeSource time) {
        if (!addresses.keySet().equals(Set.copyOf(settings.replicas())) || settings.replicas().contains(new NodeId(0))) {
            throw new IllegalArgumentException("address membership mismatch/client node0 conflict");
        }
        var membership = new IoniaConfig(settings.leader(), settings.leader(), settings.replicas(), 0);
        var transport = new IoniaTcpTransport(new NodeId(0), membership.configurationId(), addresses, socketTimeoutMillis);
        try { return new V01BenchmarkClient(settings, transport, time); }
        catch (RuntimeException error) { transport.close(); throw error; }
    }
}
