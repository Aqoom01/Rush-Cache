package io.openkache.server.routing;

import io.openkache.core.model.NodeId;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * B1 실험 전용 고정 노드 설정. 노드 i의 NodeId는 i이고 node 0이 Controller를 겸한다.
 * 모든 노드가 같은 설정(순서 포함)을 읽어야 초기 표가 일치한다.
 */
public record B1NodeConfig(int node, List<String> hosts, int clientPort, int peerPort, int tabletsPerNode) {

    public B1NodeConfig {
        hosts = List.copyOf(hosts);
        if (hosts.isEmpty() || hosts.size() > 16 || hosts.stream().anyMatch(String::isBlank)
                || node < 0 || node >= hosts.size()
                || clientPort < 1 || clientPort > 65535 || peerPort < 1 || peerPort > 65535
                || clientPort == peerPort || tabletsPerNode < 1) {
            throw new IllegalArgumentException("invalid B1 node configuration");
        }
    }

    public NodeId self() {
        return new NodeId(node);
    }

    public NodeId controller() {
        return new NodeId(0);
    }

    /** 표의 노드 목록. 모든 노드에서 같은 순서다. */
    public List<NodeId> nodeIds() {
        List<NodeId> ids = new ArrayList<>(hosts.size());
        for (int i = 0; i < hosts.size(); i++) {
            ids.add(new NodeId(i));
        }
        return List.copyOf(ids);
    }

    /** peer listener 주소. PbTransport에 넘긴다. */
    public Map<NodeId, InetSocketAddress> peerAddresses() {
        Map<NodeId, InetSocketAddress> m = new LinkedHashMap<>();
        for (int i = 0; i < hosts.size(); i++) {
            m.put(new NodeId(i), new InetSocketAddress(hosts.get(i), peerPort));
        }
        return Map.copyOf(m);
    }

    /** UTF-8 properties를 읽고 알 수 없는 key를 거절한다. hosts는 콤마로 구분한다. */
    public static B1NodeConfig load(Path path) throws IOException {
        Properties p = new Properties();
        try (var reader = Files.newBufferedReader(path)) {
            p.load(reader);
        }
        Set<String> keys = Set.of("node.id", "hosts", "client.port", "peer.port", "tablets.per.node");
        for (String key : p.stringPropertyNames()) {
            if (!keys.contains(key)) {
                throw new IllegalArgumentException("unknown B1 key: " + key);
            }
        }
        List<String> hosts = List.of(p.getProperty("hosts", "").split(","));
        return new B1NodeConfig(
                Integer.parseInt(p.getProperty("node.id", "-1").trim()),
                hosts.stream().map(String::trim).toList(),
                Integer.parseInt(p.getProperty("client.port", "0").trim()),
                Integer.parseInt(p.getProperty("peer.port", "0").trim()),
                Integer.parseInt(p.getProperty("tablets.per.node", "2").trim()));
    }
}
