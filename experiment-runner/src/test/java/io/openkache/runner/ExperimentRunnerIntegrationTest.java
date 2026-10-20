package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.server.config.ServerConfig;
import io.openkache.server.lifecycle.ServerApplication;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 실제 {@code cache-server}(KvEngine까지 연결된)를 띄워 Runner 전체 파이프라인(연결 → warm-up → 측정 → 결과 기록)이
 * 실제로 동작하는지 확인한다. Mock engine이나 fake transport를 쓰지 않는다.
 */
class ExperimentRunnerIntegrationTest {

    private ServerApplication server;
    private ServerConfig serverConfig;

    @TempDir private Path resultsDir;

    @BeforeEach
    void startServer() throws Exception {
        serverConfig =
                new ServerConfig(
                        new NodeId(1),
                        "runner-it",
                        "127.0.0.1",
                        "127.0.0.1",
                        freePort(),
                        freePort(),
                        freePort(),
                        2,
                        Duration.ofSeconds(2));
        server = ServerApplication.start(serverConfig);
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    @Test
    void runsBaselineExperimentAgainstRealServerAndWritesResults() throws Exception {
        RunnerConfig config = runnerConfig(20, 100, 4);

        RunnerInvocation invocation =
                new RunnerInvocation(
                        "openkache-experiment-runner",
                        "experiments/runner-it.properties",
                        List.of("experiments/runner-it.properties"));
        ExperimentResult result = new ExperimentRunner(config, 1, invocation).run();

        assertEquals(100, result.measuredRequests());
        long totalOutcomes = result.outcomeCounts().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(100, totalOutcomes);
        assertEquals(0L, result.outcomeCounts().get(Outcome.CLIENT_FAILURE));
        assertEquals(0L, result.outcomeCounts().get(Outcome.BUSY));
        assertEquals(0L, result.outcomeCounts().get(Outcome.TIMEOUT));
        assertEquals(0L, result.outcomeCounts().get(Outcome.UNAVAILABLE));
        assertEquals(0L, result.outcomeCounts().get(Outcome.SERVER_ERROR));
        assertTrue(result.throughputPerSecond() > 0);
        assertTrue(result.latencySampleCount() > 0 && result.latencySampleCount() <= 100);

        Path jsonFile = ResultWriter.writeJson(result, config.resultsDir());
        Path csvFile = ResultWriter.appendCsv(result, config.resultsDir());

        String json = Files.readString(jsonFile);
        assertTrue(json.contains("\"measuredRequests\": 100"));
        assertTrue(json.contains("\"resultSchemaVersion\": \"experiment-result-v1\""));
        assertTrue(json.contains("\"faultScenario\": \"none\""));
        assertTrue(json.contains("\"runNumber\": 1"));
        assertTrue(json.contains("\"configSnapshot\""));
        assertTrue(json.contains("\"command\": \"openkache-experiment-runner\""));
        assertTrue(json.contains("\"configPath\": \"experiments/runner-it.properties\""));

        List<String> csvLines = Files.readAllLines(csvFile);
        assertEquals(2, csvLines.size()); // header + 1행
        assertTrue(csvLines.get(0).startsWith("executionId,"));
    }

    @Test
    void connectivityFailureIsReportedNotSilentlySucceeded() {
        server.close(); // 서버를 내려 연결이 실제로 실패하게 만든다.

        RunnerConfig config = runnerConfig(0, 10, 1);
        Assertions.assertThrows(IOException.class, () -> new ExperimentRunner(config).run());
    }

    private RunnerConfig runnerConfig(long warmup, long measured, int concurrency) {
        Properties p = new Properties();
        p.setProperty("target.host", serverConfig.bindHost());
        p.setProperty("target.port", String.valueOf(serverConfig.clientPort()));
        p.setProperty("study", "runner-it");
        p.setProperty("variant", "baseline");
        p.setProperty("scenario", "none");
        p.setProperty("seed", "123");
        p.setProperty("key.count", "20");
        p.setProperty("key.length", "16");
        p.setProperty("value.size", "32");
        p.setProperty("mix.get", "60");
        p.setProperty("mix.set", "30");
        p.setProperty("mix.delete", "10");
        p.setProperty("distribution", "uniform");
        p.setProperty("concurrency", String.valueOf(concurrency));
        p.setProperty("warmup.requests", String.valueOf(warmup));
        p.setProperty("measured.requests", String.valueOf(measured));
        p.setProperty("request.timeout.ms", "2000");
        p.setProperty("results.dir", resultsDir.toString());
        return RunnerConfig.from(p);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
