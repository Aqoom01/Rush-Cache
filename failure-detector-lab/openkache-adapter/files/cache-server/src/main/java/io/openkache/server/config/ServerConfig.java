package io.openkache.server.config;

import io.openkache.core.model.NodeId;
import io.openkache.core.failure.SwimConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * 검증을 마친 서버 설정. 불변이다.
 *
 * <p>{@link #from(Properties)}는 모든 오류를 모아 한 번에 {@link ConfigException}으로 알린다. 알 수 없는 key도 오타로 보고
 * 거절한다.
 *
 * @param nodeId 이 노드의 ID ({@code node.id})
 * @param clusterId 클러스터 이름 ({@code cluster.id})
 * @param bindHost client·peer listener bind 주소 ({@code bind.host})
 * @param adminHost {@code /metrics} bind 주소 ({@code admin.host})
 * @param clientPort client listener port ({@code client.port})
 * @param peerPort peer TCP와 SWIM UDP listener가 각각 사용하는 port ({@code peer.port})
 * @param adminPort {@code /metrics} port ({@code admin.port})
 * @param workers core worker 수 ({@code workers}, 1-256)
 * @param requestTimeout 요청 기본 timeout ({@code request.timeout.ms}). GET·SET·DELETE 구현 시 사용한다
 * @param peerAddresses 정적 클러스터의 {@link NodeId}별 SWIM UDP 및 peer TCP 주소
 * @param swimConfig SWIM probe와 gossip 설정
 */
public record ServerConfig(
        NodeId nodeId,
        String clusterId,
        String bindHost,
        String adminHost,
        int clientPort,
        int peerPort,
        int adminPort,
        int workers,
        Duration requestTimeout,
        Map<NodeId, PeerAddress> peerAddresses,
        SwimConfig swimConfig) {

    public static final int MAX_WORKERS = 256;

    private static final String DEFAULT_RESOURCE = "/application.properties";
    private static final Set<String> KEYS =
            Set.of(
                    "node.id",
                    "cluster.id",
                    "bind.host",
                    "admin.host",
                    "client.port",
                    "peer.port",
                    "admin.port",
                    "workers",
                    "request.timeout.ms",
                    "cluster.members",
                    "swim.probe-interval.ms",
                    "swim.direct-timeout.ms",
                    "swim.indirect-timeout.ms",
                    "swim.suspicion-timeout.ms",
                    "swim.indirect-checks",
                    "swim.gossip-retransmit-multiplier",
                    "swim.max-piggyback-updates");

    public ServerConfig {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        peerAddresses = Map.copyOf(Objects.requireNonNull(peerAddresses, "peerAddresses"));
        Objects.requireNonNull(swimConfig, "swimConfig");
        if (new HashSet<>(peerAddresses.values()).size() != peerAddresses.size()) {
            throw new IllegalArgumentException("cluster members must not share a peer address");
        }
        PeerAddress localAddress = peerAddresses.get(nodeId);
        if (localAddress == null) {
            throw new IllegalArgumentException("cluster.members must contain local node " + nodeId.value());
        }
        if (localAddress.port() != peerPort) {
            throw new IllegalArgumentException(
                    "local node peer port " + localAddress.port() + " differs from peer.port " + peerPort);
        }
    }

    /** 기존 단일 노드 구성용 생성자. 로컬 노드만 포함한 기본 SWIM 설정을 사용한다. */
    public ServerConfig(
            NodeId nodeId,
            String clusterId,
            String bindHost,
            String adminHost,
            int clientPort,
            int peerPort,
            int adminPort,
            int workers,
            Duration requestTimeout) {
        this(
                nodeId,
                clusterId,
                bindHost,
                adminHost,
                clientPort,
                peerPort,
                adminPort,
                workers,
                requestTimeout,
                Map.of(nodeId, new PeerAddress(bindHost, peerPort)),
                defaultSwimConfig());
    }

    /** {@code path}가 있으면 그 파일을, 없으면 classpath의 {@code application.properties}를 읽는다. */
    public static ServerConfig load(Path path) {
        Properties properties = new Properties();
        try {
            if (path != null) {
                try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                    properties.load(reader);
                }
            } else {
                try (InputStream in = ServerConfig.class.getResourceAsStream(DEFAULT_RESOURCE)) {
                    if (in == null) {
                        throw new ConfigException("classpath resource not found: " + DEFAULT_RESOURCE);
                    }
                    properties.load(in);
                }
            }
        } catch (IOException e) {
            throw new ConfigException("failed to read config: " + (path != null ? path : DEFAULT_RESOURCE), e);
        }
        return from(properties);
    }

    /** properties를 검증해 설정을 만든다. */
    public static ServerConfig from(Properties properties) {
        Parser p = new Parser(properties);

        for (String key : properties.stringPropertyNames()) {
            if (!KEYS.contains(key)) {
                p.errors.add("unknown key '" + key + "'");
            }
        }

        long nodeId = p.longValue("node.id", 0, Long.MAX_VALUE);
        String clusterId = p.text("cluster.id");
        String bindHost = p.text("bind.host");
        String adminHost = p.text("admin.host");
        int clientPort = (int) p.longValue("client.port", 1, 65535);
        int peerPort = (int) p.longValue("peer.port", 1, 65535);
        int adminPort = (int) p.longValue("admin.port", 1, 65535);
        int workers = (int) p.longValue("workers", 1, MAX_WORKERS);
        long timeoutMs = p.longValue("request.timeout.ms", 1, Duration.ofHours(1).toMillis());
        Map<NodeId, PeerAddress> peerAddresses = p.peerAddresses("cluster.members");
        long probeIntervalMs = p.longValue("swim.probe-interval.ms", 1, Duration.ofHours(1).toMillis());
        long directTimeoutMs = p.longValue("swim.direct-timeout.ms", 1, Duration.ofHours(1).toMillis());
        long indirectTimeoutMs = p.longValue("swim.indirect-timeout.ms", 1, Duration.ofHours(1).toMillis());
        long suspicionTimeoutMs = p.longValue("swim.suspicion-timeout.ms", 1, Duration.ofHours(1).toMillis());
        int indirectChecks = (int) p.longValue("swim.indirect-checks", 1, Integer.MAX_VALUE);
        int retransmitMultiplier =
                (int) p.longValue("swim.gossip-retransmit-multiplier", 1, Integer.MAX_VALUE);
        int maxPiggybackUpdates =
                (int) p.longValue("swim.max-piggyback-updates", 1, 1_024);

        Set<Integer> ports = new HashSet<>();
        for (int port : new int[] {clientPort, peerPort, adminPort}) {
            if (port > 0 && !ports.add(port)) {
                p.errors.add("client.port, peer.port, admin.port must be different (duplicate " + port + ")");
            }
        }

        if (nodeId >= 0) {
            PeerAddress localAddress = peerAddresses.get(new NodeId(nodeId));
            if (localAddress == null) {
                p.errors.add("'cluster.members' must contain local node " + nodeId);
            } else if (peerPort > 0 && localAddress.port() != peerPort) {
                p.errors.add("local node port in 'cluster.members' must equal peer.port " + peerPort);
            }
        }

        SwimConfig swimConfig = null;
        if (probeIntervalMs > 0
                && directTimeoutMs > 0
                && indirectTimeoutMs > 0
                && suspicionTimeoutMs > 0
                && indirectChecks > 0
                && retransmitMultiplier > 0
                && maxPiggybackUpdates > 0) {
            try {
                swimConfig = new SwimConfig(
                        Duration.ofMillis(probeIntervalMs),
                        Duration.ofMillis(directTimeoutMs),
                        Duration.ofMillis(indirectTimeoutMs),
                        Duration.ofMillis(suspicionTimeoutMs),
                        indirectChecks,
                        retransmitMultiplier,
                        maxPiggybackUpdates);
            } catch (IllegalArgumentException e) {
                p.errors.add("invalid SWIM config: " + e.getMessage());
            }
        }

        if (!p.errors.isEmpty()) {
            throw new ConfigException("invalid config: " + String.join("; ", p.errors));
        }
        return new ServerConfig(
                new NodeId(nodeId),
                clusterId,
                bindHost,
                adminHost,
                clientPort,
                peerPort,
                adminPort,
                workers,
                Duration.ofMillis(timeoutMs),
                peerAddresses,
                swimConfig);
    }

    private static SwimConfig defaultSwimConfig() {
        return new SwimConfig(
                Duration.ofMillis(500),
                Duration.ofMillis(100),
                Duration.ofMillis(100),
                Duration.ofMillis(1_500),
                3,
                3,
                8);
    }

    private static final class Parser {
        final Properties properties;
        final List<String> errors = new ArrayList<>();

        Parser(Properties properties) {
            this.properties = properties;
        }

        String text(String key) {
            String value = properties.getProperty(key);
            if (value == null || value.isBlank()) {
                errors.add("'" + key + "' is required");
                return "";
            }
            return value.trim();
        }

        long longValue(String key, long min, long max) {
            String value = text(key);
            if (value.isEmpty()) {
                return -1;
            }
            try {
                long parsed = Long.parseLong(value);
                if (parsed < min || parsed > max) {
                    errors.add("'" + key + "' must be between " + min + " and " + max + ": " + parsed);
                    return -1;
                }
                return parsed;
            } catch (NumberFormatException e) {
                errors.add("'" + key + "' must be an integer: " + value);
                return -1;
            }
        }

        Map<NodeId, PeerAddress> peerAddresses(String key) {
            String value = text(key);
            Map<NodeId, PeerAddress> result = new LinkedHashMap<>();
            Set<PeerAddress> addresses = new HashSet<>();
            if (value.isEmpty()) {
                return result;
            }
            for (String rawMember : value.split(",", -1)) {
                String member = rawMember.trim();
                try {
                    int at = member.indexOf('@');
                    int colon = member.lastIndexOf(':');
                    if (at <= 0 || colon <= at + 1 || colon == member.length() - 1) {
                        throw new IllegalArgumentException("expected id@host:port");
                    }
                    NodeId id = new NodeId(Long.parseLong(member.substring(0, at)));
                    String host = member.substring(at + 1, colon);
                    int port = Integer.parseInt(member.substring(colon + 1));
                    PeerAddress address = new PeerAddress(host, port);
                    if (result.putIfAbsent(id, address) != null) {
                        errors.add("duplicate NodeId in '" + key + "': " + id.value());
                    }
                    if (!addresses.add(address)) {
                        errors.add("duplicate peer address in '" + key + "': " + host + ":" + port);
                    }
                } catch (IllegalArgumentException e) {
                    errors.add("invalid member in '" + key + "': '" + member + "' (" + e.getMessage() + ")");
                }
            }
            return result;
        }
    }
}
