package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BaselineJsonTest {
    @TempDir Path directory;

    @Test
    void roundTripPreservesFullPrecisionAndRegeneratesFromPersistedJson() throws Exception {
        Path originals = Files.createDirectory(directory.resolve("results"));
        var captured = new java.util.ArrayList<BaselineResults.Run>();
        try (PrintWriter unused = new PrintWriter(Writer.nullWriter())) {
            var baseline = new StopAndCopyBaseline(unused, captured::add);
            baseline.run("SC1", 10, 65_536, 0, 0, MigrationFixture.Fault.NONE, "none", 0, 1);
        }
        var run = captured.getFirst();
        Path json = originals.resolve("one.json");
        Map<String, Object> context = new HashMap<>();
        context.put("command", "경로\\이름\"\n\t\u0001"); context.put("gitCommit", null);
        BaselineJson.write(json, run, context, "case-1");
        var restored = BaselineJson.read(json);
        assertEquals(run.condition(), restored.condition());
        assertTrue(restored.startedAtEpochMillis() > 0);
        assertEquals(run.startedAtEpochMillis(), restored.startedAtEpochMillis());
        assertEquals(run.outcome(), restored.outcome());
        for (String metric : run.metrics().keySet()) assertEquals(run.metrics().get(metric).doubleValue(), restored.metrics().get(metric).doubleValue());
        assertTrue(Files.readString(json).contains("1.019078947368421"));
        context.put("suiteExecutionId", "another-suite");
        Path other = directory.resolve("one.json");
        BaselineJson.write(other, run, context, "case-1");
        assertTrue(Files.readString(other).contains("another-suite-one"));
        assertFalse(Files.readString(json).contains("another-suite-one"));
        BaselineResearchMain.regenerate(directory, 1);
        assertTrue(Files.readString(directory.resolve("metrics.csv")).contains("전송량 비율,1.019079"));
        assertTrue(Files.readString(directory.resolve("results.md")).contains("JSON을 다시 읽어 생성"));
        assertTrue(Files.readString(directory.resolve("results.md")).contains("`launch.properties` 없음"));
        var summary = Files.readString(directory.resolve("summary.csv")).lines().toList();
        assertEquals(2, summary.size(), "조건 1개는 헤더 + 1행");
        assertEquals(summary.get(0).split(",", -1).length, summary.get(1).split(",", -1).length);
        assertTrue(summary.get(1).contains(",1,1,tick,완료,"), "seed 수 1, seed별 값 동일 1");
        assertTrue(summary.get(1).contains(",1.019079,"));
        Map<String, Number> changed = new HashMap<>(restored.metrics());
        changed.put(BaselineResults.BLOCKED, 9L); changed.put(BaselineResults.TOTAL, 10L);
        BaselineJson.write(json, new BaselineResults.Run(restored.condition(), restored.seed(), restored.outcome(), changed), context, "case-1");
        BaselineResearchMain.regenerate(directory, 1);
        assertTrue(Files.readString(directory.resolve("metrics.csv")).contains(BaselineResults.BLOCKED + ",9"));
        assertTrue(Files.readString(directory.resolve("results.md")).contains("| 10 | 64 | 완료 | 9 | 10 |"));
        assertThrows(IllegalStateException.class, () -> BaselineResearchMain.regenerate(directory, 2));
        Files.copy(json, originals.resolve("duplicate.json"));
        assertThrows(IllegalStateException.class, () -> BaselineResearchMain.regenerate(directory, 2));
    }

    @Test
    void summaryHasOneRowPerConditionAndReportCarriesProvenance() throws Exception {
        Path folder = Files.createDirectory(directory.resolve("repeat-002"));
        Path originals = Files.createDirectory(folder.resolve("results"));
        Files.writeString(folder.resolve("launch.properties"), "suiteExecutionId=run-test\ngitCommit=abc123\nworkingTreeDirty=false\n"
                + "sourceHash=0123456789abcdef\nrepeat.count=3\nbackend.option.profile=full\n");
        var captured = new java.util.ArrayList<BaselineResults.Run>();
        try (PrintWriter unused = new PrintWriter(Writer.nullWriter())) {
            var baseline = new StopAndCopyBaseline(unused, captured::add);
            for (int seed = 1; seed <= 2; seed++) {
                baseline.run("SC1", 10, 65_536, 0, 0, MigrationFixture.Fault.NONE, "none", 0, seed);
                baseline.run("SC5", 10, 65_536, 0, 0, MigrationFixture.Fault.CHECKSUM, "none", 0, seed);
            }
        }
        for (int i = 0; i < captured.size(); i++) {
            var run = captured.get(i);
            Map<String, Number> metrics = new HashMap<>(run.metrics());
            // 첫 조건의 seed 2만 값을 바꿔 범위 표시를 확인한다.
            if (i == 2) metrics.put(BaselineResults.TOTAL, metrics.get(BaselineResults.TOTAL).longValue() + 5);
            BaselineJson.write(originals.resolve("run-" + i + ".json"), new BaselineResults.Run(run.condition(), run.seed(), run.outcome(), metrics),
                    new HashMap<>(), "case-" + (i % 2));
        }
        BaselineResearchMain.regenerate(folder, 4);
        var summary = Files.readString(folder.resolve("summary.csv")).lines().toList();
        assertEquals(3, summary.size(), "조건 2개는 헤더 + 2행");
        int columns = summary.get(0).split(",", -1).length;
        for (String line : summary) assertEquals(columns, line.split(",", -1).length, line);
        assertTrue(summary.get(1).contains(",2,0,tick,완료,"), "SC1은 seed 2개, 값 다름");
        assertTrue(summary.get(1).matches(".*,[0-9]+~[0-9]+,.*"), "다른 값은 최솟값~최댓값");
        assertTrue(summary.get(2).contains(",2,1,tick,취소(timeout),"));
        assertTrue(summary.get(1).contains("–"), "SC1의 전송 검증 실패 수 등은 해당 없음");
        assertEquals(4, Files.readString(folder.resolve("metrics.csv")).lines().filter(l -> l.contains("," + BaselineResults.BLOCKED + ",")).count(),
                "실행별 CSV는 seed별 행을 그대로 보존");
        String report = Files.readString(folder.resolve("results.md"));
        assertTrue(report.contains("실행 정보: `run-test` · 반복 2/3 · 설정 `full` · commit `abc123` · source hash `0123456789ab`"), report);
        assertFalse(report.contains("작업 트리"));
        assertTrue(report.contains("[조건별 요약](summary.csv)"));
    }

    @Test
    void keepsCommitAndIgnoresLegacyWorkingTreeStatus() {
        var launch = new java.util.Properties();
        launch.setProperty("suiteExecutionId", "captured-run");
        launch.setProperty("gitCommit", "captured-before-output");
        launch.setProperty("workingTreeDirty", "false");
        var context = BaselineResearchMain.context(launch, 1, 1);
        assertEquals("captured-before-output", context.get("gitCommit"));
        assertFalse(context.containsKey("workingTreeDirty"));
        assertFalse(((Map<?, ?>) context.get("configSnapshot")).containsKey("workingTreeDirty"));
    }

    @Test
    void malformedJsonAndUnsupportedSchemaDoNotProduceAReport() throws Exception {
        Path file = directory.resolve("bad.json");
        for (String malformed : new String[] {"{", "{} trailing", "{\"x\":1,\"x\":2}", "{\"x\":\"\\q\"}", "{\"x\":1e999}", "{\"resultSchemaVersion\":\"unknown\"}"}) {
            Files.writeString(file, malformed);
            assertThrows(IllegalArgumentException.class, () -> BaselineJson.read(file), malformed);
        }
        assertFalse(Files.exists(directory.resolve("results.md")));
    }
}
