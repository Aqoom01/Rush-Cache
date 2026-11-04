package io.openkache.server.config;

import io.openkache.core.kv.KvStorage;
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
 * @param storage KV 저장소 ({@code storage.*}). 기본은 메모리. {@code storage.engine=nvme}이면 {@code storage.path}와
 *     {@code storage.capacity.bytes}가 필요하다({@code docs/storage-nvme.md})
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
        KvStorage storage) {

    public static final int MAX_WORKERS = 256;

    private static final long DEFAULT_SEGMENT_BYTES = 64L * 1024 * 1024;
    private static final long DEFAULT_WRITE_BUFFER_BYTES = 64L * 1024;

    /** 메모리 저장소 설정. */
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
        this(nodeId, clusterId, bindHost, adminHost, clientPort, peerPort, adminPort, workers, requestTimeout, KvStorage.memory());
    }

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
                    "storage.engine",
                    "storage.path",
                    "storage.capacity.bytes",
                    "storage.segment.bytes",
                    "storage.write.buffer.bytes",
                    "storage.direct.io");

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

        KvStorage storage = storage(p, workers);

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
                storage);
    }

    /** {@code storage.*}를 읽는다. 오류는 {@code p.errors}에 모으고 그때는 메모리 저장소를 돌려준다. */
    private static KvStorage storage(Parser p, int workers) {
        String engine = p.optionalText("storage.engine", "memory");
        if (engine.equals("memory")) {
            for (String key : List.of("storage.path", "storage.capacity.bytes", "storage.segment.bytes", "storage.write.buffer.bytes", "storage.direct.io")) {
                if (p.properties.getProperty(key) != null) {
                    p.errors.add("'" + key + "' is only used with storage.engine=nvme");
                }
            }
            return KvStorage.memory();
        }
        if (!engine.equals("nvme")) {
            p.errors.add("'storage.engine' must be memory or nvme: " + engine);
            return KvStorage.memory();
        }
        String path = p.text("storage.path");
        long capacity = p.longValue("storage.capacity.bytes", 1, Long.MAX_VALUE);
        long segment = p.optionalLongValue("storage.segment.bytes", DEFAULT_SEGMENT_BYTES, 4096, 1L << 40);
        long buffer = p.optionalLongValue("storage.write.buffer.bytes", DEFAULT_WRITE_BUFFER_BYTES, 4096, 1L << 30);
        String directIo = p.optionalText("storage.direct.io", "true");
        if (!directIo.equals("true") && !directIo.equals("false")) {
            p.errors.add("'storage.direct.io' must be true or false: " + directIo);
        }
        if (buffer > segment) {
            p.errors.add("'storage.write.buffer.bytes' must be <= storage.segment.bytes");
        }
        if (!p.errors.isEmpty()) {
            return KvStorage.memory();
        }
        KvStorage.Nvme nvme = new KvStorage.Nvme(Path.of(path), capacity, segment, (int) buffer, directIo.equals("true"));
        if (nvme.segmentsPerWorker(workers) < 2) {
            p.errors.add("'storage.capacity.bytes' / workers must hold at least 2 segments of storage.segment.bytes");
        }
        return nvme;
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

        String optionalText(String key, String defaultValue) {
            String value = properties.getProperty(key);
            return value == null || value.isBlank() ? defaultValue : value.trim();
        }

        long optionalLongValue(String key, long defaultValue, long min, long max) {
            return properties.getProperty(key) == null ? defaultValue : longValue(key, min, max);
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
