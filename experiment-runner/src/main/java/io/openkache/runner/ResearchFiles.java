package io.openkache.runner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;

/** 실행 제어 metadata의 JSON 및 재현 hash. 연구 알고리즘은 포함하지 않는다. */
final class ResearchFiles {
    private ResearchFiles() {}

    static void write(Path file, Object value) throws IOException {
        Files.writeString(file, json(value) + "\n", StandardCharsets.UTF_8);
    }

    static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof String text) {
            StringBuilder out = new StringBuilder("\"");
            for (char c : text.toCharArray()) {
                switch (c) {
                    case '"' -> out.append("\\\"");
                    case '\\' -> out.append("\\\\");
                    case '\n' -> out.append("\\n");
                    case '\r' -> out.append("\\r");
                    case '\t' -> out.append("\\t");
                    default -> { if (c < 32) out.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c)); else out.append(c); }
                }
            }
            return out.append('"').toString();
        }
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> map) return map.entrySet().stream()
                .map(e -> json(e.getKey().toString()) + ":" + json(e.getValue())).collect(java.util.stream.Collectors.joining(",", "{", "}"));
        if (value instanceof Iterable<?> list) {
            var out = new java.util.ArrayList<String>();
            for (Object item : list) out.add(json(item));
            return String.join(",", out).transform(text -> "[" + text + "]");
        }
        throw new IllegalArgumentException("JSON으로 기록할 수 없는 값: " + value.getClass());
    }

    static String sourceHash(Path repository, java.util.List<Path> sources) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            var files = new java.util.HashSet<Path>();
            var roots = new java.util.ArrayList<>(sources);
            roots.add(repository.resolve("experiment-runner/src/main"));
            for (Path root : roots) {
                if (!Files.exists(root)) throw new ConfigException("source hash 입력 경로가 없습니다: " + root);
                try (var paths = Files.walk(root)) { files.addAll(paths.filter(Files::isRegularFile).toList()); }
            }
            try (var paths = Files.walk(repository, 2)) {
                files.addAll(paths.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().endsWith(".gradle.kts")).toList());
            }
            Path versions = repository.resolve("gradle/libs.versions.toml");
            if (Files.isRegularFile(versions)) files.add(versions);
            for (Path file : files.stream().sorted(java.util.Comparator.comparing(path -> repository.relativize(path).toString().replace('\\', '/'))).toList()) {
                digest.update(repository.relativize(file).toString().replace('\\', '/').getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
                byte[] contents = Files.readAllBytes(file);
                if (file.toString().matches(".*\\.(java|kt|kts|toml|properties|xml|json|txt|md|yaml|yml|gradle)$")) {
                    contents = new String(contents, StandardCharsets.UTF_8).replace("\r\n", "\n").replace('\r', '\n').getBytes(StandardCharsets.UTF_8);
                }
                digest.update(contents);
                digest.update((byte) 0);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

}
