package io.openkache.runner.benchmark.measurement;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.measurement.MeasurementFixture.*;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.runner.benchmark.execution.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Timeout(15)
class BenchmarkRunWriterTest {
    @TempDir Path directory;
    private static final Set<String> REQUIRED = Set.of("M01", "M02", "M03", "M04", "M10", "M11");

    @Test void healthyOpenLoopExportsRealMetadataFullPopulationsHashesAndStandardEnvelope() throws Exception {
        var collector = collector(); var p = openPlan(); var client = new Client();
        var result = new OpenLoopRunner(generator(), p, client, probe(), collector).run();
        assertTrue(result.passed(), result.toString());
        Path path = example("open-healthy");
        var written = BenchmarkRunWriter.write(path, metadata(REQUIRED), generator().config(), p, result, collector.freeze());
        assertTrue(written.valid(), written.toString()); assertTrue(Files.exists(path.resolve("COMPLETE")));
        String json = Files.readString(path.resolve("result.json"));
        assertTrue(json.contains("\"schema_version\":\"replication-result-v1\""));
        assertTrue(json.contains("\"cohort\":{\"admitted\":10,\"error_response\":0,\"no_response\":0,\"pending\":0,\"success\":10}"));
        assertTrue(json.contains(written.manifestSha256()));
        String actualHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path.resolve("manifest.json"))));
        assertEquals(actualHash, written.manifestSha256());
        assertEquals(10, Files.readAllLines(path.resolve("counters.csv")).size());
        String histogram = Files.readString(path.resolve("histograms.jsonl"));
        assertTrue(histogram.contains("\"count\":10")); assertTrue(histogram.contains("insufficient_samples"));
        assertTrue(Files.readString(path.resolve("manifest.json")).contains("\"seed\":101"));
        assertTrue(Files.readString(path.resolve("environment.json")).contains("\"storage_kind\":null"));
    }
    @Test void closedLoopExportsUseTheSameSchemaWithoutMixingWarmupIntoMeasurement() throws Exception {
        var c = collector(); var p = closedPlan();
        var result = new ClosedLoopRunner(generator(), p, new Client(), probe(), c).run();
        var written = BenchmarkRunWriter.write(example("closed-healthy"), metadata(REQUIRED), generator().config(), p, result, c.freeze());
        assertTrue(written.valid(), written.toString());
        String json = Files.readString(written.directory().resolve("result.json"));
        assertTrue(json.contains("\"admitted\":10")); assertTrue(json.contains("closed_loop"));
    }
    @ParameterizedTest
    @EnumSource(value = ClientOutcome.Category.class, names = {"ERROR_RESPONSE", "NO_RESPONSE"})
    void observedReadFailuresRemainValidMeasurementsWithSeparateCensoredDistributions(ClientOutcome.Category category) throws Exception {
        var c = collector(); var client = new Client(); client.measureCategory = category;
        var result = new OpenLoopRunner(generator(), openPlan(), client, probe(), c).run();
        assertTrue(result.collectionValid()); assertFalse(result.serviceHealthy());
        var written = BenchmarkRunWriter.write(example(category.name()), metadata(REQUIRED), generator().config(), openPlan(), result, c.freeze());
        assertTrue(written.valid(), written.toString());
        String lines = Files.readString(written.directory().resolve("histograms.jsonl"));
        assertTrue(lines.contains(category.name()));
        assertEquals(category == ClientOutcome.Category.NO_RESPONSE, lines.contains("\"censored\":true"));
    }
    @Test void admissionRejectionsAreRetainedOutsideTheAdmittedCohort() throws Exception {
        var c = collector(); var client = new Client(); client.rejectOdd = true;
        var result = new OpenLoopRunner(generator(), openPlan(), client, probe(), c).run();
        var written = BenchmarkRunWriter.write(example("rejections"), metadata(REQUIRED), generator().config(), openPlan(), result, c.freeze());
        assertTrue(written.valid(), written.toString());
        String counters = Files.readString(written.directory().resolve("counters.csv"));
        assertTrue(counters.contains("MEASURE,GET,10,5,5,0,0,5,0,5,0"));
        assertTrue(Files.readString(written.directory().resolve("result.json")).contains("\"admitted\":5"));
    }
    @Test void missingObserverInvalidatesExportInsteadOfFabricatingHistograms() throws Exception {
        var result = new OpenLoopRunner(generator(), openPlan(), new Client(), probe()).run();
        var written = BenchmarkRunWriter.write(example("missing-observer"), metadata(REQUIRED), generator().config(), openPlan(), result, collector().freeze());
        assertFalse(written.valid()); assertTrue(written.invalidReasons().stream().anyMatch(s -> s.startsWith("COLLECTOR_COUNTER_MISMATCH")));
        assertTrue(Files.exists(written.directory().resolve("COMPLETE"))); // INVALID이지만 완전 저장된 증거
    }
    @Test void requiredResourceMetricCannotSilentlyBecomeZero() throws Exception {
        var c = collector(); var result = new OpenLoopRunner(generator(), openPlan(), new Client(), probe(), c).run();
        var written = BenchmarkRunWriter.write(example("required-unsupported"), metadata(Set.of("M18")), generator().config(), openPlan(), result, c.freeze());
        assertFalse(written.valid()); assertTrue(written.invalidReasons().contains("REQUIRED_METRIC_UNAVAILABLE:M18"));
        String json = Files.readString(written.directory().resolve("result.json"));
        assertTrue(json.contains("\"metric_id\":\"M18\"")); assertTrue(json.contains("collector_not_implemented_in_local_export"));
    }
    @Test void preflightFailureExportsFailedEvidenceWithoutInventingMeasureSamples() throws Exception {
        var c = collector(); var result = new OpenLoopRunner(generator(), openPlan(), new Client(),
                writes -> new DrainProbe.Observation(DrainProbe.Status.UNSUPPORTED, "not_available", Map.of()), c).run();
        var written = BenchmarkRunWriter.write(example("preflight-failed"), metadata(REQUIRED), generator().config(), openPlan(), result, c.freeze());
        assertFalse(written.valid());
        assertTrue(Files.readString(written.directory().resolve("result.json")).contains("\"execution_status\":\"failed\""));
        assertEquals("", Files.readString(written.directory().resolve("histograms.jsonl")));
    }
    @Test void outputCollisionAndUnwritableShapePropagateAndPreserveExistingBytes() throws Exception {
        var c = collector(); var result = new OpenLoopRunner(generator(), openPlan(), new Client(), probe(), c).run();
        Path occupied = directory.resolve("occupied"); Files.createDirectory(occupied); Files.writeString(occupied.resolve("keep"), "existing");
        assertThrows(FileAlreadyExistsException.class, () -> BenchmarkRunWriter.write(occupied, metadata(REQUIRED), generator().config(), openPlan(), result, c.freeze()));
        assertEquals("existing", Files.readString(occupied.resolve("keep"))); assertFalse(Files.exists(occupied.resolve("COMPLETE")));
        Path file = directory.resolve("file"); Files.writeString(file, "keep");
        assertThrows(java.io.IOException.class, () -> BenchmarkRunWriter.write(file.resolve("run"), metadata(REQUIRED), generator().config(), openPlan(), result, c.freeze()));
        assertEquals("keep", Files.readString(file));
    }
    @Test void jsonEscapingIsValidAndUnsupportedValuesAreRejected() {
        assertEquals("\"a\\n\\\"\\\\\\u0001\\ud83d\\ude00\"", BenchmarkJson.encode("a\n\"\\\u0001😀"));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkJson.encode(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkJson.encode(new Object()));
        assertThrows(IllegalArgumentException.class, () -> metadata(Set.of("M99")));
        assertEquals("{\"수치\":1}", BenchmarkJson.encode(Map.of("수치", 1)));
        assertArrayEquals("{\"수치\":1}".getBytes(StandardCharsets.UTF_8), BenchmarkJson.encode(Map.of("수치", 1)).getBytes(StandardCharsets.UTF_8));
    }
    @Test void incompatiblePlanAndClockMetadataAreMarkedInvalid() throws Exception {
        var c = collector(); var p = openPlan();
        var result = new OpenLoopRunner(generator(), p, new Client(), probe(), c).run();
        var different = new OpenLoopPlan(p.runId(), p.loadSlot(), p.warmupRequests(), 11,
                p.loadOpsPerSecond(), p.warmupOpsPerSecond(), p.measureOpsPerSecond(), p.maxInFlight(),
                p.maxDispatchLagNanos(), 0, 0, p.limits());
        var meta = new BenchmarkRunWriter.Metadata("O00", "local", 3, null, null, "wrong-clock", REQUIRED);
        var written = BenchmarkRunWriter.write(example("metadata-mismatch"), meta, generator().config(), different, result, c.freeze());
        assertFalse(written.valid()); assertTrue(written.invalidReasons().contains("PLAN_RESULT_MISMATCH:MEASURE"));
        assertTrue(written.invalidReasons().contains("COLLECTOR_IDENTITY_MISMATCH"));
    }
    private Path example(String name) throws Exception {
        // 후속 독립 schema/hash 검사에 쓸 작은 기능 산출물. 대규모 데이터셋/성능 결과가 아니다.
        Path parent = Path.of("build", "benchmark-b5-fixtures"); Files.createDirectories(parent);
        Path path = parent.resolve(name + "-" + UUID.randomUUID());
        System.out.println("B5_EXPORT_FIXTURE=" + path.toAbsolutePath()); return path;
    }
}
