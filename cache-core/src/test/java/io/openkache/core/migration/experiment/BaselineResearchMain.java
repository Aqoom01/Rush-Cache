package io.openkache.core.migration.experiment;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** 공통 Runner가 호출하는 B3 backend. JSON을 먼저 저장하고 다시 읽어 CSV·보고서를 생성한다. */
public final class BaselineResearchMain {
    private BaselineResearchMain() {}

    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) {
            Path folder = Path.of(args[1]).toAbsolutePath().normalize();
            Properties completion = new Properties();
            try (var reader = Files.newBufferedReader(folder.resolve("completion.properties"), StandardCharsets.UTF_8)) { completion.load(reader); }
            regenerate(folder, Integer.parseInt(completion.getProperty("cases")));
            return;
        }
        if (args.length != 4) throw new IllegalArgumentException("backend: <새 결과 폴더> <full|quick> <반복 번호> <전체 반복 수>");
        Path folder = Path.of(args[0]).toAbsolutePath().normalize();
        if (!args[1].equals("full") && !args[1].equals("quick")) throw new IllegalArgumentException("profile 오류");
        boolean quick = args[1].equals("quick");
        int repeat = Integer.parseInt(args[2]);
        int repeats = Integer.parseInt(args[3]);
        if (repeat < 1 || repeat > repeats) throw new IllegalArgumentException("반복 번호 오류");
        Properties launch = new Properties();
        try (var reader = Files.newBufferedReader(folder.resolve("launch.properties"), StandardCharsets.UTF_8)) { launch.load(reader); }
        Path originals = Files.createDirectory(folder.resolve("results"));
        Map<String, Object> context = context(launch, repeat, repeats);
        Map<BaselineResults.Condition, String> cases = new LinkedHashMap<>();
        try (PrintWriter discardedCsv = new PrintWriter(Writer.nullWriter())) {
            StopAndCopyBaseline backend = new StopAndCopyBaseline(discardedCsv, run -> {
                String caseId = cases.computeIfAbsent(run.condition(), ignored -> String.format(java.util.Locale.ROOT, "case-%03d", cases.size() + 1));
                String runId = "repeat-" + repeat + "-" + caseId + "-seed-" + run.seed();
                try { BaselineJson.write(originals.resolve(runId + ".json"), run, context, caseId); }
                catch (IOException e) { throw new UncheckedIOException(e); }
            });
            backend.execute(quick);
            if (backend.violations() != 0) throw new IllegalStateException("정합성 위반: " + backend.violations());
        }
        int expected = quick ? 125 : 130;
        regenerate(folder, expected);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion=b3-baseline-json-1\ncases=" + expected + "\nviolations=0\n", StandardCharsets.UTF_8);
        System.out.println("JSON 원본·CSV 요약·보고서 생성 완료: " + folder);
    }

    static void regenerate(Path folder, int expected) throws IOException {
        BaselineResults report = new BaselineResults();
        var files = new java.util.ArrayList<Path>();
        try (var paths = Files.list(folder.resolve("results"))) { files.addAll(paths.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()); }
        if (files.size() != expected) throw new IllegalStateException("JSON 원본 누락: " + files.size() + "/" + expected);
        var runs = new java.util.ArrayList<BaselineResults.Run>();
        Map<BaselineResults.Condition, Integer> caseOrder = new LinkedHashMap<>();
        var unique = new java.util.HashSet<String>();
        for (Path file : files) {
            var run = BaselineJson.read(file);
            if (!unique.add(run.condition() + ":" + run.seed())) throw new IllegalStateException("중복 실행 원본: " + file);
            if (run.metrics().get(BaselineResults.VIOLATIONS).longValue() != 0) throw new IllegalStateException("정합성 위반 원본");
            runs.add(run);
            caseOrder.computeIfAbsent(run.condition(), ignored -> caseOrder.size());
        }
        runs.sort(java.util.Comparator.comparingLong(BaselineResults.Run::seed).thenComparingInt(run -> caseOrder.get(run.condition())));
        // 실행별 long CSV는 분석용 로컬 파일(metrics.csv)이다. Git에는 조건별 요약(summary.csv)과 보고서만 올린다.
        try (PrintWriter csv = new PrintWriter(Files.newBufferedWriter(folder.resolve("metrics.csv"), StandardCharsets.UTF_8))) {
            StopAndCopyBaseline.writeHeader(csv);
            for (var run : runs) { report.add(run); StopAndCopyBaseline.writeRun(csv, run); }
            if (csv.checkError()) throw new IOException("실행별 CSV 저장 실패");
        }
        try (PrintWriter csv = new PrintWriter(Files.newBufferedWriter(folder.resolve("summary.csv"), StandardCharsets.UTF_8))) {
            report.writeConditionSummary(csv);
            if (csv.checkError()) throw new IOException("조건별 CSV 요약 저장 실패");
        }
        String markdown = report.markdown("summary.csv", BaselineResults.guideLink(folder.resolve("results.md")))
                .replace("[분석용 원본]", "[조건별 요약]")
                .replaceFirst("\n\n`StopAndCopyBaseline`", java.util.regex.Matcher.quoteReplacement("\n\n" + provenance(folder) + "\n\n`StopAndCopyBaseline`"))
                .replace("## 핵심 결과", "원본은 `results/*.json`이다. 조건별 `summary.csv`, 실행별 `metrics.csv`(로컬 전용), 이 보고서는 JSON을 다시 읽어 생성했다.\n\n## 핵심 결과");
        Files.writeString(folder.resolve("results.md"), markdown, StandardCharsets.UTF_8);
    }

    /** 커밋되는 보고서에 남기는 출처. manifest·launch 파일은 Git에 올리지 않으므로 핵심 값만 옮긴다. */
    static String provenance(Path folder) throws IOException {
        Path file = folder.resolve("launch.properties");
        if (!Files.isRegularFile(file)) return "실행 정보: `launch.properties` 없음(단독 재생성)";
        Properties launch = new Properties();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) { launch.load(reader); }
        String commit = launch.getProperty("gitCommit", "");
        String repeat = folder.getFileName().toString().replaceFirst("^repeat-0*", "");
        String source = launch.getProperty("sourceHash", "");
        return "실행 정보: `" + launch.getProperty("suiteExecutionId", "?") + "` · 반복 " + repeat + "/" + launch.getProperty("repeat.count", "?")
                + " · 설정 `" + launch.getProperty("backend.option.profile", "?") + "` · commit `" + (commit.isEmpty() ? "미확인" : commit) + "`"
                + " · source hash `" + (source.length() > 12 ? source.substring(0, 12) : source.isEmpty() ? "미확인" : source) + "`";
    }

    static Map<String, Object> context(Properties launch, int repeat, int repeats) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("suiteExecutionId", launch.getProperty("suiteExecutionId"));
        context.put("runNumber", repeat); context.put("repeatCount", repeats);
        context.put("backendStartedAtEpochMillis", Instant.now().toEpochMilli());
        Map<String, String> config = new java.util.TreeMap<>();
        for (String key : launch.stringPropertyNames()) if (!java.util.Set.of("suiteExecutionId", "sourceHash", "sourceHashAlgorithm",
                "backendHash", "runnerHash", "gitCommit", "workingTreeDirty", "configPath", "runnerJvmArgs").contains(key)) config.put(key, launch.getProperty(key));
        context.put("configSnapshot", config);
        context.put("invocation", Map.of("command", "openkache-experiment-runner", "configPath", launch.getProperty("configPath", ""),
                "arguments", java.util.List.of(launch.getProperty("configPath", ""))));
        context.put("sourceHash", launch.getProperty("sourceHash"));
        context.put("sourceHashAlgorithm", launch.getProperty("sourceHashAlgorithm"));
        context.put("backendHash", launch.getProperty("backendHash"));
        context.put("runnerHash", launch.getProperty("runnerHash"));
        String commit = launch.getProperty("gitCommit", "");
        context.put("gitCommit", commit.isEmpty() ? null : commit);
        context.put("jdkVendor", System.getProperty("java.vendor")); context.put("jdkVersion", System.getProperty("java.version"));
        context.put("osName", System.getProperty("os.name")); context.put("osArch", System.getProperty("os.arch"));
        context.put("runnerJvmArgs", BaselineJson.decode(launch.getProperty("runnerJvmArgs", "[]")));
        context.put("backendJvmArgs", java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments());
        context.put("nodeCount", 2); context.put("storageLayer", "in-memory-test-fixture");
        context.put("modelConfig", Map.of("keyBytes", 16, "valueBytes", 128, "recordBytes", 152,
                "batchTargetBytes", 65_536, "generatorVersion", "b3-seed-index-1", "timeUnit", "tick"));
        return context;
    }

}
