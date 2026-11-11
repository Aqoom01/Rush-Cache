package io.openkache.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * B1 라우팅 서버 클러스터 실행 설정({@code cluster.mode=b1}). {@link RunnerConfig}의 나머지 설정(부하 구성, key, 결과 경로 등)과
 * 함께 쓴다. 요청 대상 노드는 {@code target.hosts} 순서가 곧 NodeId(0부터)이고 node 0이 Controller다.
 *
 * <p>부하는 open-loop다: {@code load.rate}(요청/초)로 정한 예정 시각에 요청을 보내고 지연은 예정 시각부터 잰다. 측정 구간 길이는
 * {@code measured.requests ÷ load.rate}초, warm-up도 같은 비율이다.
 *
 * @param peerPort 노드 peer listener 포트(표 조회·이동 지시)
 * @param loadRate 초당 요청 수(open-loop 예정 간격 = 1/loadRate)
 * @param prefill true면 측정 전에 모든 key를 한 번씩 SET한다
 * @param redirectMax MOVED를 따라갈 최대 횟수. 넘으면 실패로 센다
 * @param busyRetryMax BUSY 응답 뒤 다시 보낼 최대 횟수. 넘으면 BUSY로 끝낸다
 * @param busyRetryIntervalMs 첫 BUSY 재시도 간격(ms). 재시도마다 두 배로 늘린다
 * @param busyRetryMaxIntervalMs BUSY 재시도 간격 상한(ms). 기본은 첫 간격과 같아 고정 간격이다
 * @param bucketMs timeline 구간 폭(ms, 예정 시각 기준)
 * @param consistencyCheck true면 끝난 뒤 모든 key를 GET해 oracle과 대조한다
 * @param migrations 측정 구간 중 보낼 이동 지시(시각 순)
 * @param migrationSettings 이동 방식·대역·batch·pre-copy 설정(시각·Tablet·목적지는 -1). 균형 조정의 이동도 이 설정을 쓴다
 * @param balance B2 정책으로 Tablet을 나누고 옮기는 균형 조정. 없으면 하지 않는다
 */
record ClusterConfig(
        int peerPort,
        double loadRate,
        boolean prefill,
        int redirectMax,
        int busyRetryMax,
        long busyRetryIntervalMs,
        long busyRetryMaxIntervalMs,
        long bucketMs,
        boolean consistencyCheck,
        List<Migration> migrations,
        Migration migrationSettings,
        Optional<Balance> balance) {

    /** 측정 시작 {@code atMs} 뒤에 Tablet {@code tablet}을 node {@code to}로 옮기라고 그때의 주인에게 지시한다. */
    record Migration(long atMs, long tablet, long to, Method method, long rateBytesPerSec, int batchBytes,
                     long stopThresholdBytes, int maxRounds) {}

    /**
     * 측정 구간 중 window마다 B2 정책으로 균형을 맞춘다(관측 → Split → Placement → B3 이동).
     *
     * <p>분할 시작 조건은 B2 design.md §4.1·Benchmark C 규칙을 따른다: Tablet의 window 요청이 노드 용량의 1/4을
     * {@code sustainWindows} window 연속 넘고, 분할로 생긴 지 {@code cooldownWindows} window가 지났고, Tablet 수가
     * {@code tabletLimit} 미만이면 Split 정책을 부른다. 노드 용량은 window 전체 요청 ÷ (노드 수 × {@code targetUtilization})이다.
     *
     * @param split Split 정책: {@code heat}(split-for-heat), {@code midpoint}, {@code estore}, {@code none}
     * @param theta split-for-heat θ
     * @param placement Placement 정책: {@code hysteresis}, {@code greedy}, {@code cost-aware}, {@code none}
     * @param windowMs 관찰 window 길이(ms). 이동이 길면 그 window는 이동이 끝날 때까지 이어진다
     * @param startMs 측정 시작 뒤 첫 window를 시작할 시각(ms)
     */
    record Balance(String split, double theta, String placement, long windowMs, long startMs, int sustainWindows,
                   int cooldownWindows, int tabletLimit, double targetUtilization) {}

    /** 서버 MIGRATE method 코드와 같은 순서(protocol-v1.md §6-5). */
    enum Method {
        STOP_AND_COPY, PRE_COPY, CUTOVER;

        String label() {
            return name().toLowerCase(Locale.ROOT).replace('_', '-');
        }
    }

    static final Set<String> KEYS = Set.of(
            "cluster.mode", "cluster.peer.port", "cluster.prefill", "load.rate", "redirect.max", "retry.busy.max",
            "retry.busy.interval.ms", "retry.busy.max.interval.ms", "timeline.bucket.ms", "consistency.check", "migration.at.ms", "migration.tablet",
            "migration.to", "migration.method", "migration.rate.bytes.per.sec", "migration.batch.bytes",
            "migration.stop.threshold.bytes", "migration.max.rounds", "balance.split", "balance.split.theta",
            "balance.placement", "balance.window.ms", "balance.start.ms", "balance.sustain.windows",
            "balance.cooldown.windows", "balance.tablet.limit", "balance.target.utilization");

    /** 결과에 저장된 설정 스냅샷에서 다시 읽는다. cluster.mode가 없으면 빈 값. */
    static Optional<ClusterConfig> from(Map<String, String> snapshot) {
        List<String> errors = new ArrayList<>();
        Optional<ClusterConfig> config = parse(snapshot::get, errors);
        if (!errors.isEmpty()) throw new ConfigException("invalid cluster config: " + String.join("; ", errors));
        return config;
    }

    /** 오류는 {@code errors}에 모은다. cluster.mode가 없으면 cluster 전용 key가 하나라도 있을 때 오류다. */
    static Optional<ClusterConfig> parse(Function<String, String> get, List<String> errors) {
        String mode = text(get, "cluster.mode", "");
        if (mode.isEmpty()) {
            for (String key : KEYS) {
                if (get.apply(key) != null) errors.add("'" + key + "' is only used with cluster.mode=b1");
            }
            return Optional.empty();
        }
        if (!mode.equals("b1")) errors.add("'cluster.mode' must be b1: " + mode);
        if (get.apply("schedule.file") != null) errors.add("'schedule.file' cannot be used with cluster.mode");
        if (get.apply("routing.boundaries") != null) {
            errors.add("'routing.boundaries' cannot be used with cluster.mode (routing follows the B1 table)");
        }
        int peerPort = (int) number(get, "cluster.peer.port", "7001", 1, 65535, errors);
        double rate = decimal(get, "load.rate", errors);
        boolean prefill = flag(get, "cluster.prefill", "true", errors);
        int redirectMax = (int) number(get, "redirect.max", "3", 0, 16, errors);
        int busyMax = (int) number(get, "retry.busy.max", "1000", 0, 1_000_000, errors);
        long busyInterval = number(get, "retry.busy.interval.ms", "1", 0, 10_000, errors);
        long busyMaxInterval = number(get, "retry.busy.max.interval.ms", String.valueOf(busyInterval), 0, 60_000, errors);
        if (busyMaxInterval < busyInterval) errors.add("'retry.busy.max.interval.ms' must be >= retry.busy.interval.ms");
        long bucketMs = number(get, "timeline.bucket.ms", "100", 1, 60_000, errors);
        boolean check = flag(get, "consistency.check", "true", errors);
        Optional<Balance> balance = balance(get, errors);
        Migration settings = migrationSettings(get, errors);
        return Optional.of(new ClusterConfig(peerPort, rate, prefill, redirectMax, busyMax, busyInterval, busyMaxInterval, bucketMs,
                check, migrations(get, errors, settings, balance.isPresent()), settings, balance));
    }

    private static Optional<Balance> balance(Function<String, String> get, List<String> errors) {
        String split = text(get, "balance.split", "");
        if (split.isEmpty()) {
            for (String key : KEYS) {
                if (key.startsWith("balance.") && get.apply(key) != null) errors.add("'" + key + "' needs balance.split");
            }
            return Optional.empty();
        }
        if (!List.of("heat", "midpoint", "estore", "none").contains(split)) {
            errors.add("'balance.split' must be heat, midpoint, estore or none: " + split);
        }
        String placement = text(get, "balance.placement", "hysteresis");
        if (!List.of("hysteresis", "greedy", "cost-aware", "none").contains(placement)) {
            errors.add("'balance.placement' must be hysteresis, greedy, cost-aware or none: " + placement);
        }
        double theta = Double.parseDouble(text(get, "balance.split.theta", "0.8"));
        if (!(theta > 0 && theta <= 1)) errors.add("'balance.split.theta' must be in (0, 1]: " + theta);
        double utilization = Double.parseDouble(text(get, "balance.target.utilization", "0.7"));
        if (!(utilization > 0 && utilization <= 1)) errors.add("'balance.target.utilization' must be in (0, 1]");
        return Optional.of(new Balance(split, theta, placement,
                number(get, "balance.window.ms", "2000", 100, 600_000, errors),
                number(get, "balance.start.ms", "0", 0, 3_600_000, errors),
                (int) number(get, "balance.sustain.windows", "3", 1, 100, errors),
                (int) number(get, "balance.cooldown.windows", "3", 0, 100, errors),
                (int) number(get, "balance.tablet.limit", "256", 1, 100_000, errors), utilization));
    }

    private static Migration migrationSettings(Function<String, String> get, List<String> errors) {
        Method method;
        String methodText = text(get, "migration.method", "pre-copy");
        try {
            method = Method.valueOf(methodText.toUpperCase(Locale.ROOT).replace('-', '_'));
        } catch (IllegalArgumentException e) {
            errors.add("'migration.method' must be stop-and-copy, pre-copy or cutover: " + methodText);
            method = Method.PRE_COPY;
        }
        return new Migration(-1, -1, -1, method,
                number(get, "migration.rate.bytes.per.sec", "0", 0, Long.MAX_VALUE, errors),
                (int) number(get, "migration.batch.bytes", "65536", 64, 1024 * 1024, errors),
                number(get, "migration.stop.threshold.bytes", "262144", 0, Long.MAX_VALUE, errors),
                (int) number(get, "migration.max.rounds", "10", 0, 1000, errors));
    }

    private static List<Migration> migrations(Function<String, String> get, List<String> errors, Migration settings,
                                              boolean balance) {
        List<String> at = list(get, "migration.at.ms");
        List<String> tablets = list(get, "migration.tablet");
        List<String> targets = list(get, "migration.to");
        if (at.isEmpty() && tablets.isEmpty() && targets.isEmpty()) {
            for (String key : List.of("migration.method", "migration.rate.bytes.per.sec", "migration.batch.bytes",
                    "migration.stop.threshold.bytes", "migration.max.rounds")) {
                if (get.apply(key) != null && !balance) errors.add("'" + key + "' needs migration.at.ms or balance.split");
            }
            return List.of();
        }
        if (at.size() != tablets.size() || at.size() != targets.size()) {
            errors.add("migration.at.ms, migration.tablet, migration.to must have the same number of entries");
            return List.of();
        }
        List<Migration> result = new ArrayList<>();
        long previous = -1;
        for (int i = 0; i < at.size(); i++) {
            try {
                long atMs = Long.parseLong(at.get(i));
                long tablet = Long.parseLong(tablets.get(i));
                long to = Long.parseLong(targets.get(i));
                if (atMs < 0 || atMs < previous || tablet < 0 || to < 0) {
                    errors.add("migration entries must be non-negative and migration.at.ms non-decreasing");
                    return List.of();
                }
                previous = atMs;
                result.add(new Migration(atMs, tablet, to, settings.method(), settings.rateBytesPerSec(), settings.batchBytes(),
                        settings.stopThresholdBytes(), settings.maxRounds()));
            } catch (NumberFormatException e) {
                errors.add("migration lists must be integers: " + e.getMessage());
                return List.of();
            }
        }
        return List.copyOf(result);
    }

    private static List<String> list(Function<String, String> get, String key) {
        String value = text(get, key, "");
        if (value.isEmpty()) return List.of();
        List<String> parts = new ArrayList<>();
        for (String part : value.split(",")) parts.add(part.trim());
        return parts;
    }

    private static String text(Function<String, String> get, String key, String defaultValue) {
        String value = get.apply(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static long number(Function<String, String> get, String key, String defaultValue, long min, long max,
                               List<String> errors) {
        String value = text(get, key, defaultValue);
        try {
            long parsed = Long.parseLong(value);
            if (parsed < min || parsed > max) errors.add("'" + key + "' must be between " + min + " and " + max + ": " + parsed);
            return parsed;
        } catch (NumberFormatException e) {
            errors.add("'" + key + "' must be an integer: " + value);
            return min;
        }
    }

    private static double decimal(Function<String, String> get, String key, List<String> errors) {
        String value = text(get, key, "");
        if (value.isEmpty()) {
            errors.add("'" + key + "' is required with cluster.mode");
            return 1;
        }
        try {
            double parsed = Double.parseDouble(value);
            if (!(parsed > 0 && parsed <= 1_000_000)) errors.add("'" + key + "' must be in (0, 1000000]: " + value);
            return parsed;
        } catch (NumberFormatException e) {
            errors.add("'" + key + "' must be a number: " + value);
            return 1;
        }
    }

    private static boolean flag(Function<String, String> get, String key, String defaultValue, List<String> errors) {
        String value = text(get, key, defaultValue);
        if (!value.equals("true") && !value.equals("false")) errors.add("'" + key + "' must be true or false: " + value);
        return value.equals("true");
    }
}
