package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Command;
import io.openkache.core.migration.JobState;
import io.openkache.core.model.Bytes;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 기대한 종료 이유와 실제 관측값을 혼동하지 않도록 실행 경로와 CSV를 함께 검증한다. */
class BaselineMeasurementsTest {
    @TempDir Path directory;

    @Test
    void unsupportedCommandsAreRejectedBeforeRoutingAndFreezeChecks() {
        MigrationFixture fixture = new MigrationFixture(false, MigrationFixture.Fault.NONE);
        Bytes key = Bytes.utf8("key");
        fixture.source.data.put(key, Bytes.utf8("value"));
        for (boolean frozen : new boolean[] {false, true}) {
            if (frozen) fixture.protocol.begin(fixture.initialJob);
            for (var node : List.of(MigrationFixture.SOURCE, MigrationFixture.DESTINATION)) {
                assertThrows(IllegalArgumentException.class, () -> fixture.request(node, new Command.Exists(key)));
                assertThrows(IllegalArgumentException.class, () -> fixture.request(node, new Command.Count()));
            }
        }
        assertEquals(Map.of(key, Bytes.utf8("value")), fixture.source.data);
        assertTrue(fixture.destination.isEmpty());
        assertEquals(0, fixture.coordinator.successfulCommits);
    }

    @Test
    void timeoutIsRecordedOnlyWhenExecutionBudgetActuallyExpires() {
        MigrationFixture timeout = new MigrationFixture(false, MigrationFixture.Fault.LOST_ACK);
        timeout.source.data.put(Bytes.utf8("key"), Bytes.utf8("value"));
        timeout.protocol.begin(timeout.initialJob);
        TickRunner runner = new TickRunner(timeout, 2);
        assertFalse(runner.timedOut());
        assertEquals(JobState.COPYING, runner.step(1_000));
        assertEquals(JobState.COPYING, runner.step(1_000));
        assertFalse(runner.timedOut());
        assertEquals(JobState.ABORTED, runner.step(1_000));
        assertTrue(runner.timedOut());
        assertEquals(JobState.ABORTED, runner.step(1_000));
        assertTrue(runner.timedOut());
    }

    @Test
    void otherAbortPathsDoNotMasqueradeAsTimeout() {
        MigrationFixture explicit = new MigrationFixture(false, MigrationFixture.Fault.NONE);
        explicit.protocol.begin(explicit.initialJob);
        TickRunner cancelled = new TickRunner(explicit, 1);
        explicit.protocol.abort();
        assertEquals(JobState.ABORTED, cancelled.step(1_000));
        assertFalse(cancelled.timedOut());

        MigrationFixture lostFreeze = new MigrationFixture(false, MigrationFixture.Fault.NONE);
        lostFreeze.protocol.begin(lostFreeze.initialJob);
        TickRunner unready = new TickRunner(lostFreeze, 100);
        lostFreeze.source.unfreeze(lostFreeze.protocol.expectedTable().get(MigrationFixture.MOVING));
        assertEquals(JobState.ABORTED, unready.step(1_000));
        assertFalse(unready.timedOut());

        MigrationFixture conflict = new MigrationFixture(false, MigrationFixture.Fault.NONE);
        conflict.protocol.begin(conflict.initialJob);
        conflict.protocol.step(1_000);
        assertEquals(JobState.SWITCHING, conflict.protocol.state());
        assertTrue(conflict.coordinator.commit(conflict.protocol.job(), conflict.protocol.expectedTable()));
        TickRunner stale = new TickRunner(conflict, 100);
        assertEquals(JobState.ABORTED, stale.step(1_000));
        assertFalse(stale.timedOut());
    }

    @Test
    void csvAndReportShareObservedOutcomesAndWriteRecovery() throws Exception {
        Path csv = directory.resolve("summary.csv");
        Path report = StopAndCopyBaseline.reportPath(csv);
        // 전체 SC1~SC6 main 대신 완료·취소·취소 거부 경로를 검증한다.
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(csv, StandardCharsets.UTF_8))) {
            StopAndCopyBaseline.writeHeader(writer);
            StopAndCopyBaseline baseline = new StopAndCopyBaseline(writer);
            for (int seed = 1; seed <= 2; seed++) {
                baseline.run("SC1", 10, 65_536, 0, 0, MigrationFixture.Fault.NONE, "none", 0, seed);
                baseline.run("SC4", 10_000, 65_536, 0, 0, MigrationFixture.Fault.NONE, "copying", 0, seed);
                baseline.run("SC4", 10, 65_536, 0, 0, MigrationFixture.Fault.ACK_DELAY, "before-final-ack", 0, seed);
                baseline.run("SC4", 10, 65_536, 0, 0, MigrationFixture.Fault.NONE, "cleanup", 0, seed);
                baseline.run("SC5", 10, 65_536, 0, 0, MigrationFixture.Fault.CHECKSUM, "none", 0, seed);
                baseline.run("SC5", 10, 65_536, 0, 0, MigrationFixture.Fault.LOST_ACK, "none", 0, seed);
            }
            baseline.writeReport(report, csv.getFileName().toString());
            assertFalse(writer.checkError());
        }
        byte[] bytes = Files.readAllBytes(csv);
        assertEquals(0xef, bytes[0] & 255);
        assertEquals(0xbb, bytes[1] & 255);
        assertEquals(0xbf, bytes[2] & 255);
        String content = new String(bytes, StandardCharsets.UTF_8);
        assertFalse(content.contains("\r"));
        assertTrue(content.endsWith("\n"));
        assertFalse(content.contains("소유권 전환 전 시간 초과 여부"));
        assertFalse(content.contains("쓰기 중단 시간"));
        Map<String, Map<String, Double>> runs = new HashMap<>();
        Map<String, String> outcomes = new HashMap<>();
        for (String line : content.substring(1).lines().skip(1).toList()) {
            String[] cells = line.split(",", -1);
            assertEquals(14, cells.length);
            assertEquals("stop-and-copy", cells[0]);
            assertEquals("tick", cells[10]);
            if (cells[12].equals("전송량 비율")) assertTrue(cells[13].matches("[0-9]+\\.[0-9]{6}"), cells[13]);
            String key = String.join(",", java.util.Arrays.copyOf(cells, 11));
            outcomes.put(key, cells[11]);
            assertNull(runs.computeIfAbsent(key, ignored -> new HashMap<>()).put(cells[12], Double.valueOf(cells[13])));
        }
        assertEquals(12, runs.size());
        assertEquals(java.util.Set.of("완료", "취소(timeout)", "취소(요청)", "취소 거부"), new java.util.HashSet<>(outcomes.values()));
        for (var entry : runs.entrySet()) {
            Map<String, Double> metrics = entry.getValue();
            assertEquals(0.0, metrics.get(BaselineResults.VIOLATIONS).doubleValue());
            assertFalse(metrics.containsKey(BaselineResults.OTHER_REJECTED));
            assertFalse(metrics.containsKey(BaselineResults.DUPLICATES));
            double blocked = metrics.get(BaselineResults.BLOCKED);
            double total = metrics.get(BaselineResults.TOTAL);
            assertTrue(blocked >= 0);
            if (outcomes.get(entry.getKey()).startsWith("취소(")) assertEquals(total, blocked);
            else assertTrue(total > blocked);
        }
        String markdown = Files.readString(report);
        assertTrue(markdown.contains("총 12회 실행"));
        assertTrue(markdown.contains("24/24건 일치"));
        assertTrue(markdown.contains("모든 조건에서 seed별 지표와 결과가 같다"));
        assertTrue(markdown.contains("| checksum 손상 | 취소(timeout) | 32 | 1,549 | 1 | – |"));
        assertEquals(1, markdown.lines().filter(line -> line.startsWith("| checksum 손상 |")).count());
        assertEquals(6, markdown.lines().filter(line -> line.startsWith("## SC")).count());
        assertFalse(markdown.contains("\r"));
        assertEquals(directory.resolve("quick-results.md"), StopAndCopyBaseline.reportPath(directory.resolve("quick.csv")));
    }

    @Test
    void reportLinksGuideFromRunFolder() {
        Path folder = BaselineResults.GUIDE.toAbsolutePath().getParent();
        assertEquals("../../README.md", BaselineResults.guideLink(folder.resolve("runs/sc-baseline/results.md")));
        assertEquals("README.md", BaselineResults.guideLink(folder.resolve("results.md")));
    }

    @Test
    void reportShowsRangesWhenSeedsDifferInsteadOfSilentlySelectingOne() {
        BaselineResults results = new BaselineResults();
        var condition = new BaselineResults.Condition("SC1", 10, 65_536, 0, 0, MigrationFixture.Fault.NONE, "none", 0);
        for (int seed = 1; seed <= 2; seed++) {
            Map<String, Number> metrics = new HashMap<>();
            metrics.put(BaselineResults.BLOCKED, seed + 1);
            metrics.put(BaselineResults.TOTAL, seed + 2);
            metrics.put(BaselineResults.SENT, 1_549);
            metrics.put("전송량 비율", 1_549.0 / 1_520);
            metrics.put(BaselineResults.VIOLATIONS, 0);
            metrics.put("이전 노드 데이터 일치 여부", 1);
            metrics.put("새 노드 데이터 일치 여부", 1);
            results.add(new BaselineResults.Run(condition, seed, "완료", metrics));
        }
        String report = results.markdown("summary.csv");
        assertFalse(report.contains("모든 조건에서 seed별 지표와 결과가 같다"));
        assertTrue(report.contains("| 10 | 64 | 완료 | 2~3 | 3~4 | 1,549 | 1.019079 |"));
    }

    @Test
    void interpretationDoesNotClaimPredictionAgreementOrConstantDowntimeWhenMeasurementsDiffer() {
        BaselineResults results = new BaselineResults();
        var conditions = java.util.List.of(
                new BaselineResults.Condition("SC1", 10, 1_024, 0, 0, MigrationFixture.Fault.NONE, "none", 0),
                new BaselineResults.Condition("SC1-rate", 10, 2_048, 0, 0, MigrationFixture.Fault.NONE, "none", 0),
                new BaselineResults.Condition("SC2", 10, 1_024, 0, 0, MigrationFixture.Fault.NONE, "none", 0),
                new BaselineResults.Condition("SC2", 10, 1_024, 0.5, 0, MigrationFixture.Fault.NONE, "none", 0));
        int[] blocked = {3, 8, 3, 4};
        for (int i = 0; i < conditions.size(); i++) {
            Map<String, Number> metrics = Map.of(
                    BaselineResults.BLOCKED, blocked[i], BaselineResults.TOTAL, blocked[i] + 1,
                    BaselineResults.SENT, 1_549, "전송량 비율", 1_549.0 / 1_520,
                    BaselineResults.VIOLATIONS, 0, "이전 노드 데이터 일치 여부", 1,
                    "새 노드 데이터 일치 여부", 1, BaselineResults.REJECTED, i == 3 ? 7 : 0);
            results.add(new BaselineResults.Run(conditions.get(i), 1, "완료", metrics));
        }
        String report = results.markdown("summary.csv");
        assertTrue(report.contains("예산 1/2 KiB/tick의 쓰기 차단은 3/8 tick이다. 1/2회가 예측식"));
        assertTrue(report.contains("rho=0.00/0.50에서 쓰기 차단은 3/4 tick, 거절 수는 0/7건이다."));
        assertFalse(report.contains("rho가 거절 수를 바꾸고 쓰기 차단 시간은 바꾸지 않았다"));
    }
}
