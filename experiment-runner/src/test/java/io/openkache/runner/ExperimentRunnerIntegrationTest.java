package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.KeyHash;
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
import java.util.Random;
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

    @Test
    void routesKeysByHashRangeToTwoServersAndReportsEachServer() throws Exception {
        ServerConfig secondConfig =
                new ServerConfig(
                        new NodeId(2), "runner-it", "127.0.0.1", "127.0.0.1",
                        freePort(), freePort(), freePort(), 2, Duration.ofSeconds(2));
        ServerApplication second = ServerApplication.start(secondConfig);
        try {
            Properties p = runnerProperties(0, 2_000, 4);
            p.setProperty("key.count", "200");
            p.setProperty(
                    "target.hosts",
                    "127.0.0.1:" + serverConfig.clientPort() + ",127.0.0.1:" + secondConfig.clientPort());
            p.setProperty("routing.boundaries", Long.toUnsignedString(1L << 63));
            RunnerConfig config = RunnerConfig.from(p);

            ExperimentResult result = new ExperimentRunner(config).run();

            assertEquals(2, result.nodeCount());
            assertEquals(2, result.targets().size());
            TargetResult left = result.targets().get(0);
            TargetResult right = result.targets().get(1);
            assertEquals(2_000, left.requests() + right.requests());
            assertTrue(left.requests() > 0 && right.requests() > 0);
            assertEquals("[0, 9223372036854775808)", left.hashRange());
            assertEquals(0L, result.outcomeCounts().get(Outcome.CLIENT_FAILURE));

            // 같은 seed의 요청열을 해시로 나눈 수와 서버별 요청 수가 정확히 같아야 한다.
            long expectedLeft = 0;
            for (int thread = 0; thread < config.concurrency(); thread++) {
                WorkloadGenerator generator = new WorkloadGenerator(config, new Random(config.seed() + thread));
                for (int i = 0; i < 2_000 / config.concurrency(); i++) {
                    Command command = generator.next();
                    Bytes key = switch (command) {
                        case Command.Get get -> get.key();
                        case Command.Set set -> set.key();
                        case Command.Delete delete -> delete.key();
                        default -> throw new AssertionError(command);
                    };
                    if (config.targetIndex(KeyHash.of(key)) == 0) {
                        expectedLeft++;
                    }
                }
            }
            assertEquals(expectedLeft, left.requests());

            String json = Files.readString(ResultWriter.writeJson(result, config.resultsDir()));
            assertTrue(json.contains("\"targets\": [{\"address\": \"127.0.0.1:" + serverConfig.clientPort() + "\""));
        } finally {
            second.close();
        }
    }

    private RunnerConfig runnerConfig(long warmup, long measured, int concurrency) {
        return RunnerConfig.from(runnerProperties(warmup, measured, concurrency));
    }

    private Properties runnerProperties(long warmup, long measured, int concurrency) {
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
        return p;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
