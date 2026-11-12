package io.openkache.server.benchmark.ionia;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaConfig;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.ionia.IoniaTcpTransport;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/** 원본 O00 TCP 전송을 재사용한다. 각 RPC의 새 socket 정책은 변경하지 않는다. */
public final class O00TcpClients {
    private O00TcpClients() {}

    /** 주소와 구성 검증 후 세션을 연다. 실제 연결/발견은 submit 시 수행한다. */
    public static O00BenchmarkClient open(O00BenchmarkClient.Settings settings,
            Map<NodeId, InetSocketAddress> addresses, int socketTimeoutMillis, TimeSource time) {
        if (!addresses.keySet().equals(Set.copyOf(settings.replicas()))
                || settings.replicas().contains(new NodeId(0))) {
            throw new IllegalArgumentException("address membership mismatch/client node0 conflict");
        }
        IoniaConfig membership = new IoniaConfig(settings.leader(), settings.leader(), settings.replicas(), 0);
        IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0),
                membership.configurationId(), addresses, socketTimeoutMillis);
        try { return new O00BenchmarkClient(settings, transport, time); }
        catch (RuntimeException error) { transport.close(); throw error; }
    }
}

