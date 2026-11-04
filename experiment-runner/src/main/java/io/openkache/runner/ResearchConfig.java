package io.openkache.runner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** 연구와 무관한 JVM backend 설정. 경로는 config 또는 저장소 기준으로 해석한다. */
record ResearchConfig(Path repository, Path results, String study, String experiment, Backend backend,
        int repeats, long runTimeoutMs, long executionTimeoutMs, Map<String, String> snapshot, Path configFile) {
    private static final Set<String> KEYS = Set.of("execution.mode", "study", "experiment", "repository.root",
            "repeat.count", "run.timeout.ms", "execution.timeout.ms", "results.dir",
            "backend.main", "backend.classpath", "backend.source.paths", "backend.expected.cases", "backend.result.schema", "backend.build.receipt");
    private static final Pattern TOKEN = Pattern.compile("\\{([^{}]+)}");

    record Backend(String mainClass, List<Path> classpath, List<Path> sources, Path buildReceipt, List<String> jvmArgs,
            List<String> arguments, int expectedCases, String resultSchema) {}

    static Properties properties(Path file) {
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) { properties.load(reader); }
        catch (IOException e) { throw new ConfigException("설정 파일 읽기 실패: " + file, e); }
        return properties;
    }

    static String mode(Path file) { return properties(file).getProperty("execution.mode", "tcp").strip(); }

    static ResearchConfig load(Path file) {
        Path absolute = file.toAbsolutePath().normalize();
        Properties p = properties(absolute);
        for (String key : p.stringPropertyNames()) {
            if (!KEYS.contains(key) && !key.matches("backend\\.(arg|jvm\\.arg)\\.[1-9][0-9]*")
                    && !key.matches("backend\\.option\\.[A-Za-z0-9_.-]+")) throw new ConfigException("알 수 없는 연구 설정: " + key);
        }
        if (!required(p, "execution.mode").equals("research")) throw new ConfigException("execution.mode=research가 필요합니다");
        String study = identifier(p, "study");
        String experiment = identifier(p, "experiment");
        Path repository = absolute.getParent().resolve(required(p, "repository.root")).normalize();
        if (!Files.isRegularFile(repository.resolve("settings.gradle.kts"))) throw new ConfigException("저장소 루트가 아닙니다: " + repository);
        String main = required(p, "backend.main");
        if (!main.matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*")) throw new ConfigException("backend.main은 Java 클래스 이름이어야 합니다");
        List<Path> classpath = paths(p, "backend.classpath", repository);
        List<Path> sources = paths(p, "backend.source.paths", repository);
        Map<String, String> snapshot = new TreeMap<>();
        p.stringPropertyNames().forEach(key -> snapshot.put(key, p.getProperty(key)));
        snapshot.putIfAbsent("repeat.count", "1");
        snapshot.putIfAbsent("run.timeout.ms", "600000");
        snapshot.putIfAbsent("execution.timeout.ms", "3600000");
        List<String> arguments = numbered(p, "backend.arg.");
        for (String argument : arguments) expand(argument, snapshot, absolute, repository, 1, 1);
        List<String> jvmArgs = numbered(p, "backend.jvm.arg.");
        for (String argument : jvmArgs) {
            if (!argument.startsWith("-X") && !argument.startsWith("-D")) {
                throw new ConfigException("backend.jvm.arg는 -X/-XX/-D JVM 옵션만 지원합니다");
            }
        }
        int expected = p.containsKey("backend.expected.cases") ? (int) positive(snapshot, "backend.expected.cases", Integer.MAX_VALUE) : -1;
        Backend backend = new Backend(main, classpath, sources, repository.resolve(required(p, "backend.build.receipt")).normalize(),
                jvmArgs, arguments, expected, required(p, "backend.result.schema"));
        return new ResearchConfig(repository, repository.resolve(required(p, "results.dir")).normalize(), study, experiment, backend,
                (int) positive(snapshot, "repeat.count", 100), positive(snapshot, "run.timeout.ms", 86_400_000),
                positive(snapshot, "execution.timeout.ms", 86_400_000), Map.copyOf(snapshot), absolute);
    }

    List<String> arguments(Path output, int repeat) {
        return backend.arguments().stream().map(arg -> expand(arg, snapshot, configFile, output, repeat, repeats)).toList();
    }

    private static String expand(String argument, Map<String, String> config, Path file, Path output, int repeat, int repeats) {
        var matcher = TOKEN.matcher(argument);
        return matcher.replaceAll(match -> java.util.regex.Matcher.quoteReplacement(switch (match.group(1)) {
            case "output.dir" -> output.toString();
            case "repeat" -> Integer.toString(repeat);
            case "repeat.count" -> Integer.toString(repeats);
            case "config.file" -> file.toString();
            default -> {
                String token = match.group(1);
                String value = token.startsWith("config.") ? config.get(token.substring(7)) : null;
                if (value == null) throw new ConfigException("알 수 없는 backend 인자 치환자: " + token);
                yield value;
            }
        }));
    }

    private static List<Path> paths(Properties p, String key, Path repository) {
        List<Path> paths = new ArrayList<>();
        for (String value : required(p, key).split(",", -1)) {
            if (value.isBlank()) throw new ConfigException(key + "에 빈 경로가 있습니다");
            paths.add(repository.resolve(value.strip()).normalize());
        }
        return List.copyOf(paths);
    }

    private static List<String> numbered(Properties p, String prefix) {
        long count = p.stringPropertyNames().stream().filter(key -> key.startsWith(prefix)).count();
        if (count > 100) throw new ConfigException(prefix + "는 최대 100개입니다");
        List<String> values = new ArrayList<>();
        for (int index = 1; index <= count; index++) {
            String value = p.getProperty(prefix + index);
            if (value == null) throw new ConfigException(prefix + "는 1부터 연속 번호를 사용해야 합니다");
            values.add(value);
        }
        return List.copyOf(values);
    }

    private static String required(Properties p, String key) {
        String value = p.getProperty(key, "").strip();
        if (value.isEmpty()) throw new ConfigException(key + "가 필요합니다");
        return value;
    }

    private static String identifier(Properties p, String key) {
        String value = required(p, key);
        if (!value.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) throw new ConfigException(key + "는 영문·숫자·점·밑줄·하이픈 식별자여야 합니다");
        return value;
    }

    private static long positive(Map<String, String> p, String key, long maximum) {
        try {
            long value = Long.parseLong(p.get(key));
            if (value > 0 && value <= maximum) return value;
        } catch (NumberFormatException ignored) { /* 아래에서 설정 오류로 보고한다. */ }
        throw new ConfigException(key + "는 1~" + maximum + " 범위의 정수여야 합니다");
    }
}
