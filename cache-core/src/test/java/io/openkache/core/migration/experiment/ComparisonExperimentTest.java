package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Command;
import io.openkache.core.migration.JobState;
import io.openkache.core.model.Bytes;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ComparisonExperimentTest {
    @Test
    void standaloneInvocationIsRecordedWithoutClaimingTheCommonRunnerWasUsed() {
        var launch = new java.util.Properties();
        launch.setProperty("execution.command", "java");
        launch.setProperty("execution.arguments", BaselineJson.encode(List.of("-cp", "classes", "B3Main")));
        launch.setProperty("execution.launcher", "b3-standalone-python");
        var context = BaselineResearchMain.context(launch, 1, 1);
        var invocation = BaselineJson.object(context.get("invocation"));
        assertEquals("java", invocation.get("command"));
        assertEquals(List.of("-cp", "classes", "B3Main"), invocation.get("arguments"));
        assertEquals("b3-standalone-python", context.get("executionLauncher"));
    }

    @Test
    void tickTracesRepeatExactlyAndZipfConcentratesRequests() {
        var uniform = new ComparisonExperiment.Case("test", "test", 1_000, 512, .5, 30, 0, 64, 10);
        var skew = new ComparisonExperiment.Case("test", "test", 1_000, 512, .5, 30, 1.2, 64, 10);
        var a = new ComparisonExperiment.Trace(uniform, 3);
        var b = new ComparisonExperiment.Trace(uniform, 3);
        var hot = new ComparisonExperiment.Trace(skew, 3);
        Map<Bytes, Integer> uniformCounts = new HashMap<>(), hotCounts = new HashMap<>();
        for (int tick = 0; tick < 500; tick++) {
            assertEquals(a.at(tick), b.at(tick));
            for (Command command : a.at(tick)) if (command instanceof Command.Get get) uniformCounts.merge(get.key(), 1, Integer::sum);
            for (Command command : hot.at(tick)) if (command instanceof Command.Get get) hotCounts.merge(get.key(), 1, Integer::sum);
        }
        assertTrue(hotCounts.values().stream().mapToInt(Integer::intValue).max().orElseThrow()
                > uniformCounts.values().stream().mapToInt(Integer::intValue).max().orElseThrow() * 5);
    }

    @Test
    void uniformWritesCoverTheKeyspaceInsteadOfOverlappingTickStreams() {
        var c = new ComparisonExperiment.Case("uniform", "test", 100_000, 65_536, .5, 30, 0, 262_144, 10);
        var trace = new ComparisonExperiment.Trace(c, 1);
        var unique = new java.util.HashSet<Bytes>();
        int attempts = 0;
        for (int tick = 0; tick < 100; tick++) {
            for (Command command : trace.at(tick)) {
                switch (command) {
                    case Command.Set set -> { unique.add(set.key()); attempts++; }
                    case Command.Delete delete -> { unique.add(delete.key()); attempts++; }
                    default -> { }
                }
            }
        }
        assertEquals(21_557, attempts);
        // 100,000 key에 약 21,557번 독립 선택하면 약 19,400개가 서로 달라야 한다.
        assertTrue(unique.size() > 18_000 && unique.size() < 21_000, "uniform diversity: " + unique.size());
    }

    @Test
    void variantsShareInputAndValidateLiveWritesDeletesAndReadRetries() {
        var c = new ComparisonExperiment.Case("test", "test", 1_000, 512, .5, 30, .99, 1_024, 10);
        String hash = null;
        for (String variant : ComparisonExperiment.VARIANTS) {
            var run = ComparisonExperiment.run(c, 1, variant);
            if (hash == null) hash = run.inputHash();
            assertEquals(hash, run.inputHash());
            assertEquals(0, metric(run, "invariant_violations"));
            assertEquals(1, metric(run, "exact_comparisons"));
            if (variant.equals("post-copy")) {
                assertEquals(1, metric(run, "writes_blocked_ticks"));
                assertTrue(metric(run, "reads_busy") > 0);
            } else assertEquals(0, metric(run, "reads_busy"));
        }
    }

    @Test
    void stopAndCopyRegressionMatchesOriginalTickRunnerAndTransferBytes() {
        var c = new ComparisonExperiment.Case("test", "test", 100, 512, 0, 30, 0, 1_024, 10);
        var result = ComparisonExperiment.run(c, 1, "stop-and-copy");
        var fixture = new MigrationFixture(false, MigrationFixture.Fault.NONE);
        fixture.source.data.putAll(new ComparisonExperiment.Trace(c, 1).initial());
        fixture.protocol.begin(fixture.initialJob);
        var runner = new TickRunner(fixture, 10_000);
        int ticks = 0;
        while (fixture.protocol.state() != JobState.DONE && ticks < 10_000) { runner.step(c.budget()); ticks++; }
        assertEquals(JobState.DONE, fixture.protocol.state());
        assertEquals(ticks, metric(result, "total_ticks"));
        assertEquals(fixture.protocol.job().progress().sentBytes(), metric(result, "sent_bytes"));
    }

    @Test
    void destinationSnapshotsAreImmutableAndIndependentOfLaterMutations() {
        var receiver = new io.openkache.core.migration.precopy.PreCopyReceiver();
        var f = new LiveMigrationFixture(false);
        Bytes key = Bytes.utf8("key"), old = Bytes.utf8("old"), updated = Bytes.utf8("updated");
        var session = f.job("pre-copy");
        var tablet = f.current.get(LiveMigrationFixture.MOVING);
        receiver.prepare(session, tablet);
        receiver.receive(session, tablet.epoch(), io.openkache.core.migration.TransferBatch.create(0, true,
                List.of(new io.openkache.core.migration.TransferBatch.Record(key, old))));
        var snapshot = receiver.data();
        assertThrows(UnsupportedOperationException.class, () -> snapshot.put(key, updated));
        receiver.discard(session);
        assertEquals(Map.of(key, old), snapshot);
        f.source.put(key, old);
        var post = new io.openkache.core.migration.postcopy.PostCopyProtocol(f.source, f.current);
        var job = f.job(post.name());
        post.begin(job);
        assertTrue(f.commit(post, job));
        post.execute(new Command.Set(key, old));
        var postSnapshot = post.data();
        post.execute(new Command.Set(key, updated));
        assertEquals(Map.of(key, old), postSnapshot);
        assertThrows(UnsupportedOperationException.class, () -> postSnapshot.clear());
    }

    @Test
    void reportRegeneratesFromOriginalsAndRejectsMissingOrTamperedRuns(@TempDir Path folder) throws Exception {
        Files.writeString(folder.resolve("launch.properties"), "suiteExecutionId=test-suite\nrepeat.count=1\nbackend.option.profile=quick\n", StandardCharsets.UTF_8);
        ComparisonResearchMain.main(new String[] {folder.toString(), "quick", "1", "1"});
        List<Path> originals;
        try (var files = Files.list(folder.resolve("results"))) { originals = files.sorted().toList(); }
        assertEquals(240, originals.size());
        String report = Files.readString(folder.resolve("results.md"));
        String csv = Files.readString(folder.resolve("summary.csv"));
        String dirtyCsv = Files.readString(folder.resolve("dirty-rounds.csv"));
        String dirtyReport = Files.readString(folder.resolve("dirty-rounds.md"));
        assertTrue(dirtyCsv.contains("NOT_SHRINKING") || dirtyCsv.contains("THRESHOLD"));
        assertTrue(dirtyReport.contains("dirty keys"));
        ComparisonResearchMain.regenerate(folder, "quick");
        assertEquals(report, Files.readString(folder.resolve("results.md")));
        assertEquals(csv, Files.readString(folder.resolve("summary.csv")));
        assertEquals(dirtyCsv, Files.readString(folder.resolve("dirty-rounds.csv")));
        assertEquals(dirtyReport, Files.readString(folder.resolve("dirty-rounds.md")));
        Path file = originals.getFirst();
        String original = Files.readString(file);
        var changed = BaselineJson.object(BaselineJson.decode(original));
        changed.put("inputHash", "tampered");
        Files.writeString(file, BaselineJson.encode(changed));
        assertThrows(IllegalStateException.class, () -> ComparisonResearchMain.regenerate(folder, "quick"));
        Files.writeString(file, original);
        Files.move(file, file.resolveSibling("missing.txt"));
        assertThrows(IllegalStateException.class, () -> ComparisonResearchMain.regenerate(folder, "quick"));
        Files.move(file.resolveSibling("missing.txt"), file);
        Path legacy = Files.createDirectory(folder.resolve("legacy"));
        Path legacyResults = Files.createDirectory(legacy.resolve("results"));
        for (Path source : originals) {
            var row = BaselineJson.object(BaselineJson.decode(Files.readString(source)));
            row.put("resultSchemaVersion", ComparisonResearchMain.LEGACY_SCHEMA);
            row.remove("dirtyRounds");
            Files.writeString(legacyResults.resolve(source.getFileName()), BaselineJson.encode(row));
        }
        ComparisonResearchMain.regenerate(legacy, "quick");
        assertEquals(csv, Files.readString(legacy.resolve("summary.csv")));
        assertFalse(Files.exists(legacy.resolve("dirty-rounds.csv")));
    }

    private static double metric(ComparisonExperiment.Run run, String key) {
        return ((Number) BaselineJson.object(run.metrics().get(key)).get("value")).doubleValue();
    }
}
