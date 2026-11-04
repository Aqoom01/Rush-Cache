package io.openkache.runner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** config로 선택한 연구 main을 별도 JVM에서 실행하고 반복·제한 시간·산출물을 수집한다. */
final class ResearchLauncher {
    private ResearchLauncher() {}
    record Attempt(int number, String status, String folder, String error) {}
    record ProcessResult(String status, int exitCode) {}

    static int run(ResearchConfig config) throws IOException {
        validateBackend(config.backend());
        Map<String, String> fingerprints = ResearchBuild.verify(config);
        String sourceHash = fingerprints.get("sourceHash");
        String gitCommit = GitInfo.capture(config.repository()).commit().orElse(null);
        Files.createDirectories(config.results());
        String id = "run-" + Instant.now().toString().replace(':', '-') + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path suite = Files.createDirectory(config.results().resolve(id));
        Properties snapshot = new Properties();
        snapshot.putAll(config.snapshot());
        snapshot.setProperty("repository.root", config.repository().toString());
        snapshot.setProperty("results.dir", config.results().toString());
        try (var writer = Files.newBufferedWriter(suite.resolve("config.properties"), StandardCharsets.UTF_8)) { snapshot.store(writer, "실제 실행 설정"); }
        List<Attempt> attempts = new ArrayList<>();
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("runnerSchemaVersion", "research-run-1");
        manifest.put("resultSchemaVersion", config.backend().resultSchema());
        manifest.put("suiteExecutionId", id);
        manifest.put("study", config.study());
        manifest.put("experiment", config.experiment());
        manifest.put("configSnapshot", config.snapshot());
        manifest.put("configPath", config.configFile().toString());
        manifest.put("repositoryRoot", config.repository().toString());
        manifest.put("sourceHash", sourceHash);
        manifest.put("sourceHashAlgorithm", "sha256-lf-text-v1");
        manifest.put("backendHash", fingerprints.get("backendHash"));
        manifest.put("runnerHash", fingerprints.get("runnerHash"));
        manifest.put("gitCommit", gitCommit);
        manifest.put("expectedRepeats", config.repeats());
        manifest.put("expectedCasesPerRepeat", config.backend().expectedCases() < 0 ? null : config.backend().expectedCases());
        manifest.put("status", "partial");
        ResearchFiles.write(suite.resolve("manifest.json"), manifest);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.executionTimeoutMs());
        for (int repeat = 1; repeat <= config.repeats(); repeat++) {
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remaining <= 0 || Thread.currentThread().isInterrupted()) {
                attempts.add(new Attempt(repeat, "SKIPPED", "", "전체 실행 제한 또는 중단"));
                continue;
            }
            Path folder = Files.createDirectory(suite.resolve(String.format(java.util.Locale.ROOT, "repeat-%03d", repeat)));
            Properties context = new Properties();
            context.putAll(config.snapshot());
            context.setProperty("suiteExecutionId", id);
            context.setProperty("sourceHash", sourceHash);
            context.setProperty("sourceHashAlgorithm", "sha256-lf-text-v1");
            context.setProperty("backendHash", fingerprints.get("backendHash"));
            context.setProperty("runnerHash", fingerprints.get("runnerHash"));
            context.setProperty("gitCommit", gitCommit == null ? "" : gitCommit);
            context.setProperty("configPath", config.configFile().toString());
            context.setProperty("runnerJvmArgs", ResearchFiles.json(java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments()));
            List<String> command = new ArrayList<>();
            command.add(javaExecutable());
            command.addAll(config.backend().jvmArgs());
            command.addAll(List.of("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "-cp",
                    config.backend().classpath().stream().map(Path::toString).collect(java.util.stream.Collectors.joining(java.io.File.pathSeparator)),
                    config.backend().mainClass()));
            command.addAll(config.arguments(folder, repeat));
            context.setProperty("backendCommand", ResearchFiles.json(command));
            try (var writer = Files.newBufferedWriter(folder.resolve("launch.properties"), StandardCharsets.UTF_8)) { context.store(writer, "Runner가 전달한 실행 문맥"); }
            String status;
            String error = "";
            try {
                ProcessResult result = execute(command, config.repository(), folder.resolve("backend.log"), Math.min(remaining, config.runTimeoutMs()));
                status = result.status();
                if (status.equals("SUCCESS")) {
                    if (!complete(folder, config.backend())) { status = "FAILED"; error = "backend 산출물 누락·개수·완료 계약 불일치"; }
                } else error = "backend exit=" + result.exitCode();
            } catch (IOException | ConfigException e) { status = "FAILED"; error = e.toString(); }
            attempts.add(new Attempt(repeat, status, folder.getFileName().toString(), error));
            writeSummary(suite, attempts, config);
            System.out.println("연구 실험 " + repeat + "/" + config.repeats() + ": " + statusLabel(status) + " (" + folder + ")");
        }
        boolean success = attempts.stream().allMatch(a -> a.status().equals("SUCCESS"));
        manifest.put("status", success ? "complete" : "partial");
        manifest.put("artifacts", artifacts(suite));
        ResearchFiles.write(suite.resolve("manifest.json"), manifest);
        writeSummary(suite, attempts, config);
        System.out.println("연구 결과: " + suite.resolve("results.md"));
        return success ? 0 : 1;
    }

    static String javaExecutable() {
        return Path.of(System.getProperty("java.home"), "bin", System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java").toString();
    }

    static ProcessResult execute(List<String> command, Path repository, Path log, long timeoutMs) throws IOException {
        Process process = new ProcessBuilder(command).directory(repository.toFile()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        process.getOutputStream().close();
        try {
            if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) return new ProcessResult(process.exitValue() == 0 ? "SUCCESS" : "FAILED", process.exitValue());
            return new ProcessResult("TIMEOUT", -1);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ProcessResult("FAILED", -1);
        } finally {
            if (process.isAlive()) {
                var children = process.descendants().toList();
                children.forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
                boolean interrupted = false;
                while (process.isAlive()) {
                    try { process.waitFor(); }
                    catch (InterruptedException e) { interrupted = true; }
                }
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }

    private static void validateBackend(ResearchConfig.Backend backend) throws IOException {
        boolean found = false;
        String entry = backend.mainClass().replace('.', '/') + ".class";
        for (Path path : backend.classpath()) {
            if (Files.isDirectory(path)) found |= Files.isRegularFile(path.resolve(entry));
            else if (Files.isRegularFile(path)) {
                try (var jar = new java.util.jar.JarFile(path.toFile())) { found |= jar.getJarEntry(entry) != null; }
            } else throw new ConfigException("backend classpath가 없습니다. 먼저 빌드하세요: " + path);
        }
        if (!found) throw new ConfigException("backend.main 클래스를 찾을 수 없습니다. classpath와 빌드를 확인하세요: " + backend.mainClass());
    }

    private static boolean complete(Path folder, ResearchConfig.Backend backend) throws IOException {
        if (!Files.isRegularFile(folder.resolve("summary.csv")) || !Files.isRegularFile(folder.resolve("results.md"))
                || !Files.isRegularFile(folder.resolve("completion.properties")) || !Files.isDirectory(folder.resolve("results"))) return false;
        Properties completion = ResearchConfig.properties(folder.resolve("completion.properties"));
        if (!completion.getProperty("status", "").equals("complete")
                || !completion.getProperty("resultSchemaVersion", "").equals(backend.resultSchema())) return false;
        int cases;
        try { cases = Integer.parseInt(completion.getProperty("cases", "")); }
        catch (NumberFormatException invalid) { return false; }
        if (cases < 1 || (backend.expectedCases() > 0 && cases != backend.expectedCases())) return false;
        try (var files = Files.list(folder.resolve("results"))) {
            return files.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".json")).count() == cases;
        }
    }

    private static List<Map<String, String>> artifacts(Path suite) throws IOException {
        var artifacts = new ArrayList<Map<String, String>>();
        try (var paths = Files.walk(suite)) {
            for (Path file : paths.filter(Files::isRegularFile).sorted().toList()) {
                if (!suite.relativize(file).toString().startsWith("repeat-")) continue;
                try {
                    String hash = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
                    artifacts.add(Map.of("path", suite.relativize(file).toString().replace('\\', '/'), "sha256", hash));
                } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
            }
        }
        return artifacts;
    }

    private static void writeSummary(Path suite, List<Attempt> attempts, ResearchConfig config) throws IOException {
        List<Map<String, Object>> rows = attempts.stream().map(a -> Map.<String, Object>of("runNumber", a.number(), "status", a.status(), "folder", a.folder(), "error", a.error())).toList();
        ResearchFiles.write(suite.resolve("run-summary.json"), Map.of("requestedRuns", config.repeats(), "attempts", rows));
        StringBuilder csv = new StringBuilder("\uFEFF반복 번호,상태,결과 폴더\n");
        StringBuilder report = new StringBuilder("# " + config.experiment() + " 실행 결과\n\n| 반복 | 실행 상태 | 상세 결과 |\n|---:|---|---|\n");
        for (Attempt a : attempts) {
            csv.append(a.number()).append(',').append(statusLabel(a.status())).append(',').append(a.folder()).append('\n');
            report.append("| ").append(a.number()).append(" | ").append(statusLabel(a.status())).append(" | ");
            report.append(a.status().equals("SUCCESS") ? "[표·검증](" + a.folder() + "/results.md)" : "미완료: " + a.error().replace("|", "\\|").replace('\n', ' ')).append(" |\n");
        }
        Files.writeString(suite.resolve("summary.csv"), csv, StandardCharsets.UTF_8);
        Files.writeString(suite.resolve("results.md"), report, StandardCharsets.UTF_8);
    }

    private static String statusLabel(String status) {
        return switch (status) {
            case "SUCCESS" -> "성공";
            case "FAILED" -> "실패";
            case "TIMEOUT" -> "실행 시간 초과";
            case "SKIPPED" -> "미실행";
            default -> throw new IllegalArgumentException("알 수 없는 실행 상태: " + status);
        };
    }
}
