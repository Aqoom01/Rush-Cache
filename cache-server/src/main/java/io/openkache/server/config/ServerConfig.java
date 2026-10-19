package io.openkache.server.config;

import io.openkache.core.model.NodeId;
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
 * @param peerPort peer listener port ({@code peer.port})
 * @param adminPort {@code /metrics} port ({@code admin.port})
 * @param workers core worker 수 ({@code workers}, 1-256)
 * @param requestTimeout 요청 기본 timeout ({@code request.timeout.ms}). GET·SET·DELETE 구현 시 사용한다
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
        Duration requestTimeout) {

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
                    "request.timeout.ms");

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

        Set<Integer> ports = new HashSet<>();
        for (int port : new int[] {clientPort, peerPort, adminPort}) {
            if (port > 0 && !ports.add(port)) {
                p.errors.add("client.port, peer.port, admin.port must be different (duplicate " + port + ")");
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
                Duration.ofMillis(timeoutMs));
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
    }
}
