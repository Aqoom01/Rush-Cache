package io.openkache.runner;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * 검증을 마친 Runner 설정. 불변이다.
 *
 * <p>{@link #from(Properties)}는 모든 오류를 모아 한 번에 {@link ConfigException}으로 알린다. 알 수 없는 key도 오타로
 * 보고 거절한다. 같은 설정 파일과 {@code seed}를 다시 쓰면 같은 요청열이 재현된다({@link WorkloadGenerator}).
 *
 * @param targetHost 실험 대상 서버 host
 * @param targetPort 실험 대상 서버의 client listener port
 * @param study 연구 ID (결과 metadata, 예: {@code "baseline"})
 * @param variant 실험 변형 이름 (결과 metadata)
 * @param scenario 시나리오 이름 (결과 metadata). fault scenario가 아니면 {@code "none"}
 * @param seed 요청열 생성에 쓰는 seed
 * @param keyCount 서로 다른 key의 수
 * @param keyLength 생성하는 key의 고정 byte 길이
 * @param valueSize SET에 쓰는 value의 byte 크기
 * @param mixGetPercent GET 비율 (0-100)
 * @param mixSetPercent SET 비율 (0-100)
 * @param mixDeletePercent DELETE 비율 (0-100). 세 비율의 합은 100이어야 한다
 * @param distribution key 선택 분포
 * @param hotKeyFraction {@link Distribution#FIXED_HOT_KEY}일 때 고정 key를 고를 확률 (0.0-1.0)
 * @param concurrency 동시 클라이언트(thread) 수
 * @param warmupRequests thread당이 아닌 전체 warm-up 요청 수. 결과 집계에 포함하지 않는다
 * @param measuredRequests thread당이 아닌 전체 측정 요청 수. 결과 집계에 포함한다
 * @param requestTimeoutMs 요청마다 wire로 보내는 상대 timeout(ms)이자 소켓 read timeout
 * @param repeatCount 같은 설정으로 실행할 횟수. 모든 반복은 같은 seed로 같은 요청열을 사용한다
 * @param runTimeoutMs 반복 한 건의 실행 제한 시간(ms)
 * @param executionTimeoutMs 반복을 모두 포함한 전체 실험 실행 제한 시간(ms)
 * @param configSnapshot 결과 파일에 남길 전체 설정값. 재현을 위해 key 순서가 고정돼 있다
 * @param resultsDir 결과 JSON/CSV를 쓸 디렉터리
 * @param serverWorkerCountHint 서버를 띄울 때 쓴 worker 수. Runner가 직접 측정하지 않으므로 운영자가 적어 넣는 참고값이다.
 *     기록하지 않으면 -1(미측정)
 * @param serverJvmOptsNote 서버 JVM 실행 옵션에 대한 참고용 자유 기술. Runner는 서버 JVM을 직접 관측하지 않는다. 기록하지
 *     않으면 빈 문자열(미측정)
 * @param serverStorageNote 서버 KV 저장소 종류에 대한 참고값({@code server.storage.note}). Runner는 서버 저장소를 관측하지
 *     않는다. 기록하지 않으면 {@link #DEFAULT_SERVER_STORAGE}
 * @param targets 요청을 보낼 서버들({@code target.hosts}, {@code host:port} 쉼표 구분). 없으면 {@code target.host}·
 *     {@code target.port} 하나
 * @param routingBoundaries key 해시 구간 경계({@code routing.boundaries}, 부호 없는 64비트 10진수 오름차순). 해시값이 i번째
 *     경계 미만인 가장 앞 구간의 서버로 보낸다. 경계 k개면 앞 k+1개 서버를 쓴다. 비어 있으면 모든 요청이 첫 서버로 간다
 * @param hotRangeKeyFraction {@link Distribution#HOT_RANGE}에서 뜨거운 key 비율(해시값이 작은 쪽, 0 초과 1 미만)
 * @param hotRangeRequestFraction {@link Distribution#HOT_RANGE}에서 뜨거운 key로 가는 요청 비율(0-1)
 */
public record RunnerConfig(
        String targetHost,
        int targetPort,
        String study,
        String variant,
        String scenario,
        long seed,
        int keyCount,
        int keyLength,
        int valueSize,
        int mixGetPercent,
        int mixSetPercent,
        int mixDeletePercent,
        Distribution distribution,
        double hotKeyFraction,
        int concurrency,
        long warmupRequests,
        long measuredRequests,
        long requestTimeoutMs,
        int repeatCount,
        long runTimeoutMs,
        long executionTimeoutMs,
        Map<String, String> configSnapshot,
        Path resultsDir,
        int serverWorkerCountHint,
        String serverJvmOptsNote,
        String serverStorageNote,
        List<Target> targets,
        List<Long> routingBoundaries,
        double hotRangeKeyFraction,
        double hotRangeRequestFraction) {

    /** 요청 대상 서버. */
    public record Target(String host, int port) {
        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    /** {@link #routingBoundaries}로 실제로 요청을 받는 서버 수. */
    public int routedTargetCount() {
        return routingBoundaries.size() + 1;
    }

    /** 해시값 {@code keyHash}의 key를 보낼 서버 번호(0부터). */
    public int targetIndex(long keyHash) {
        int index = 0;
        while (index < routingBoundaries.size() && Long.compareUnsigned(keyHash, routingBoundaries.get(index)) >= 0) {
            index++;
        }
        return index;
    }

    /** {@code server.storage.note}가 없을 때 기록하는 저장소. 서버 기본값(메모리 저장소)이다. */
    public static final String DEFAULT_SERVER_STORAGE = "in-memory (io.openkache.core.kv.KvEngine)";

    private static final Set<String> KEYS =
            Set.of(
                    "execution.mode",
                    "target.host",
                    "target.port",
                    "study",
                    "variant",
                    "scenario",
                    "seed",
                    "key.count",
                    "key.length",
                    "value.size",
                    "mix.get",
                    "mix.set",
                    "mix.delete",
                    "distribution",
                    "hot.key.fraction",
                    "concurrency",
                    "warmup.requests",
                    "measured.requests",
                    "request.timeout.ms",
                    "repeat.count",
                    "run.timeout.ms",
                    "execution.timeout.ms",
                    "results.dir",
                    "server.workers.hint",
                    "server.jvm.opts.note",
                    "server.storage.note",
                    "target.hosts",
                    "routing.boundaries",
                    "hot.range.key.fraction",
                    "hot.range.request.fraction");

    /** {@code path}의 properties 파일을 읽어 검증한다. */
    public static RunnerConfig load(Path path) {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException e) {
            throw new ConfigException("failed to read config: " + path, e);
        }
        return from(properties);
    }

    /** properties를 검증해 설정을 만든다. */
    public static RunnerConfig from(Properties properties) {
        Parser p = new Parser(properties);

        for (String key : properties.stringPropertyNames()) {
            if (!KEYS.contains(key)) {
                p.errors.add("unknown key '" + key + "'");
            }
        }

        String targetHost = p.text("target.host");
        if (!p.optionalText("execution.mode", "tcp").equals("tcp")) p.errors.add("TCP Runner의 execution.mode는 tcp여야 합니다");
        int targetPort = (int) p.longValue("target.port", 1, 65535);
        String study = p.text("study");
        String variant = p.text("variant");
        String scenario = p.text("scenario");
        long seed = p.longValue("seed", 0, Long.MAX_VALUE);
        int keyCount = (int) p.longValue("key.count", 1, Integer.MAX_VALUE);
        int keyLength = (int) p.longValue("key.length", 1, 4096);
        int minKeyLength = ("key-" + (keyCount - 1)).length();
        if (keyLength < minKeyLength) {
            p.errors.add(
                    "'key.length' must be >= "
                            + minKeyLength
                            + " to uniquely represent key.count="
                            + keyCount
                            + " (otherwise different keys truncate to the same bytes): "
                            + keyLength);
        }
        int valueSize = (int) p.longValue("value.size", 0, Integer.MAX_VALUE);
        int mixGet = (int) p.longValue("mix.get", 0, 100);
        int mixSet = (int) p.longValue("mix.set", 0, 100);
        int mixDelete = (int) p.longValue("mix.delete", 0, 100);
        if (mixGet + mixSet + mixDelete != 100) {
            p.errors.add(
                    "mix.get + mix.set + mix.delete must equal 100: " + (mixGet + mixSet + mixDelete));
        }
        Distribution distribution = p.enumValue("distribution", Distribution.class);
        double hotKeyFraction = p.doubleValue("hot.key.fraction", "0.8", 0.0, 1.0);
        int concurrency = (int) p.longValue("concurrency", 1, 10_000);
        long warmupRequests = p.longValue("warmup.requests", 0, Long.MAX_VALUE);
        long measuredRequests = p.longValue("measured.requests", 1, Long.MAX_VALUE);
        long requestTimeoutMs = p.longValue("request.timeout.ms", 1, ONE_HOUR_MS);
        int repeatCount = (int) p.optionalLongValue("repeat.count", "1", 1, 10_000);
        long runTimeoutMs = p.optionalLongValue("run.timeout.ms", "600000", 1, ONE_HOUR_MS);
        long executionTimeoutMs = p.optionalLongValue("execution.timeout.ms", "3600000", 1, ONE_HOUR_MS);
        String resultsDirText = p.text("results.dir");
        int serverWorkerCountHint = (int) p.optionalLongValue("server.workers.hint", "-1", -1, 10_000);
        String serverJvmOptsNote = p.optionalText("server.jvm.opts.note", "");
        String serverStorageNote = p.optionalText("server.storage.note", DEFAULT_SERVER_STORAGE);
        List<Target> targets = p.targets(targetHost, targetPort);
        List<Long> routingBoundaries = p.boundaries(targets.size());
        double hotRangeKeyFraction = p.doubleValue("hot.range.key.fraction", "0.2", 0.0, 1.0);
        double hotRangeRequestFraction = p.doubleValue("hot.range.request.fraction", "0.8", 0.0, 1.0);
        if (distribution == Distribution.HOT_RANGE) {
            int hotKeys = (int) (keyCount * hotRangeKeyFraction);
            if (hotKeys < 1 || hotKeys >= keyCount) {
                p.errors.add("'hot.range.key.fraction' must leave at least one hot and one cold key: " + hotRangeKeyFraction);
            }
        }

        if (!p.errors.isEmpty()) {
            throw new ConfigException("invalid config: " + String.join("; ", p.errors));
        }

        Map<String, String> configSnapshot = new TreeMap<>();
        for (String key : properties.stringPropertyNames()) {
            configSnapshot.put(key, properties.getProperty(key));
        }

        return new RunnerConfig(
                targetHost,
                targetPort,
                study,
                variant,
                scenario,
                seed,
                keyCount,
                keyLength,
                valueSize,
                mixGet,
                mixSet,
                mixDelete,
                distribution,
                hotKeyFraction,
                concurrency,
                warmupRequests,
                measuredRequests,
                requestTimeoutMs,
                repeatCount,
                runTimeoutMs,
                executionTimeoutMs,
                Map.copyOf(configSnapshot),
                Path.of(resultsDirText),
                serverWorkerCountHint,
                serverJvmOptsNote,
                serverStorageNote,
                targets,
                routingBoundaries,
                hotRangeKeyFraction,
                hotRangeRequestFraction);
    }

    private static final long ONE_HOUR_MS = 3_600_000;

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

        long longValue(String key, long min, long max) {
            String value = text(key);
            if (value.isEmpty()) {
                return min;
            }
            return parseLong(key, value, min, max);
        }

        long optionalLongValue(String key, String defaultValue, long min, long max) {
            String value = optionalText(key, defaultValue);
            return parseLong(key, value, min, max);
        }

        long parseLong(String key, String value, long min, long max) {
            try {
                long parsed = Long.parseLong(value);
                if (parsed < min || parsed > max) {
                    errors.add("'" + key + "' must be between " + min + " and " + max + ": " + parsed);
                    return min;
                }
                return parsed;
            } catch (NumberFormatException e) {
                errors.add("'" + key + "' must be an integer: " + value);
                return min;
            }
        }

        double doubleValue(String key, String defaultValue, double min, double max) {
            String value = optionalText(key, defaultValue);
            try {
                double parsed = Double.parseDouble(value);
                if (parsed < min || parsed > max) {
                    errors.add("'" + key + "' must be between " + min + " and " + max + ": " + parsed);
                    return min;
                }
                return parsed;
            } catch (NumberFormatException e) {
                errors.add("'" + key + "' must be a number: " + value);
                return min;
            }
        }

        List<Target> targets(String defaultHost, int defaultPort) {
            String value = optionalText("target.hosts", "");
            if (value.isEmpty()) {
                return List.of(new Target(defaultHost, defaultPort));
            }
            List<Target> targets = new ArrayList<>();
            for (String part : value.split(",")) {
                String address = part.trim();
                int colon = address.lastIndexOf(':');
                try {
                    int port = Integer.parseInt(address.substring(colon + 1));
                    if (colon <= 0 || port < 1 || port > 65535) {
                        throw new NumberFormatException();
                    }
                    targets.add(new Target(address.substring(0, colon), port));
                } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
                    errors.add("'target.hosts' entries must be host:port: " + address);
                }
            }
            return targets.isEmpty() ? List.of(new Target(defaultHost, defaultPort)) : List.copyOf(targets);
        }

        List<Long> boundaries(int targetCount) {
            String value = optionalText("routing.boundaries", "");
            if (value.isEmpty()) {
                return List.of();
            }
            List<Long> boundaries = new ArrayList<>();
            for (String part : value.split(",")) {
                try {
                    long boundary = Long.parseUnsignedLong(part.trim());
                    if (boundary == 0 || (!boundaries.isEmpty() && Long.compareUnsigned(boundaries.getLast(), boundary) >= 0)) {
                        errors.add("'routing.boundaries' must be increasing and greater than 0: " + value);
                        return List.of();
                    }
                    boundaries.add(boundary);
                } catch (NumberFormatException e) {
                    errors.add("'routing.boundaries' must be unsigned 64-bit integers: " + part.trim());
                    return List.of();
                }
            }
            if (boundaries.size() + 1 > targetCount) {
                errors.add("'routing.boundaries' needs " + (boundaries.size() + 1) + " targets but target.hosts has " + targetCount);
            }
            return List.copyOf(boundaries);
        }

        <E extends Enum<E>> E enumValue(String key, Class<E> type) {
            String value = text(key);
            if (value.isEmpty()) {
                return type.getEnumConstants()[0];
            }
            try {
                return Enum.valueOf(type, value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                errors.add(
                        "'"
                                + key
                                + "' must be one of "
                                + List.of(type.getEnumConstants())
                                + " (zipfian is not supported yet): "
                                + value);
                return type.getEnumConstants()[0];
            }
        }
    }
}
