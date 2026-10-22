package io.openkache.runner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Gradle의 testClasses 완료 뒤 연구 config의 소스·실행 클래스 hash를 함께 기록한다.
 * 실행 시 기록과 현재 입력을 대조한다. owner thread 상태를 사용하지 않는 개발 도구다.
 */
public final class ResearchBuild {
    private ResearchBuild() {}
    private static final String VERSION = "research-build-1";

    /** 인자는 config 파일 경로 목록이다. TCP config는 대상에서 제외한다. */
    public static void main(String[] args) throws IOException {
        for (String arg : args) {
            Path file = Path.of(arg);
            if (ResearchConfig.mode(file).equals("research")) record(ResearchConfig.load(file));
        }
    }

    static Map<String, String> fingerprints(ResearchConfig config) throws IOException {
        return Map.of("version", VERSION, "sourceHash", ResearchFiles.sourceHash(config.repository(), config.backend().sources()),
                "backendHash", classpathHash(config.backend().classpath()), "runnerHash", runnerHash());
    }

    static void record(ResearchConfig config) throws IOException {
        Map<String, String> hashes = fingerprints(config);
        Path receipt = config.backend().buildReceipt();
        Files.createDirectories(receipt.getParent());
        StringBuilder text = new StringBuilder();
        hashes.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> text.append(e.getKey()).append('=').append(e.getValue()).append('\n'));
        Files.writeString(receipt, text, StandardCharsets.UTF_8);
    }

    static Map<String, String> verify(ResearchConfig config) throws IOException {
        Path receipt = config.backend().buildReceipt();
        if (!Files.isRegularFile(receipt)) throw stale("빌드 기록이 없습니다");
        var recorded = ResearchConfig.properties(receipt);
        Map<String, String> actual = fingerprints(config);
        for (var entry : actual.entrySet()) {
            if (!entry.getValue().equals(recorded.getProperty(entry.getKey()))) throw stale("소스 또는 실행 클래스가 빌드 기록과 다릅니다: " + entry.getKey());
        }
        return actual;
    }

    private static ConfigException stale(String message) {
        return new ConfigException(message + ". gradlew build :experiment-runner:installDist로 다시 빌드하세요");
    }

    private static String classpathHash(List<Path> classpath) throws IOException {
        MessageDigest digest = digest();
        for (int index = 0; index < classpath.size(); index++) {
            Path root = classpath.get(index);
            if (Files.isDirectory(root)) {
                try (var paths = Files.walk(root)) {
                    for (Path file : paths.filter(Files::isRegularFile).sorted(java.util.Comparator.comparing(p -> root.relativize(p).toString().replace('\\', '/'))).toList()) {
                        update(digest, index + "/" + root.relativize(file).toString().replace('\\', '/'), Files.readAllBytes(file));
                    }
                }
            } else update(digest, index + "/jar", Files.readAllBytes(root));
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    // Gradle classes와 설치 JAR를 같은 class entry 기준으로 비교한다. JAR의 생성 시각은 제외한다.
    private static String runnerHash() throws IOException {
        Path location;
        try { location = Path.of(ResearchBuild.class.getProtectionDomain().getCodeSource().getLocation().toURI()); }
        catch (java.net.URISyntaxException impossible) { throw new IllegalStateException(impossible); }
        MessageDigest digest = digest();
        if (Files.isDirectory(location)) {
            try (var paths = Files.walk(location)) {
                for (Path file : paths.filter(p -> p.toString().endsWith(".class")).sorted(java.util.Comparator.comparing(p -> location.relativize(p).toString().replace('\\', '/'))).toList()) {
                    update(digest, location.relativize(file).toString().replace('\\', '/'), Files.readAllBytes(file));
                }
            }
        } else {
            try (var jar = new java.util.jar.JarFile(location.toFile())) {
                for (var entry : jar.stream().filter(e -> e.getName().endsWith(".class")).sorted(java.util.Comparator.comparing(java.util.jar.JarEntry::getName)).toList()) {
                    try (var stream = jar.getInputStream(entry)) { update(digest, entry.getName(), stream.readAllBytes()); }
                }
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static void update(MessageDigest digest, String name, byte[] bytes) {
        digest.update(name.getBytes(StandardCharsets.UTF_8)); digest.update((byte) 0);
        digest.update(bytes); digest.update((byte) 0);
    }
}
