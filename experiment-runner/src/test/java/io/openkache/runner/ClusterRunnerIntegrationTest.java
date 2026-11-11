package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.server.routing.B1NodeConfig;
import io.openkache.server.routing.B1Server;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * B1 서버 2대(127.0.0.1/127.0.0.2)를 실제로 띄워 {@code cluster.mode=b1} 실행 전체를 확인한다: 표 조회, open-loop 부하,
 * 측정 중 Tablet 이동 지시, MOVED 추적·BUSY 재시도, 결과 파일, 끝난 뒤 정합성 검사.
 */
class ClusterRunnerIntegrationTest {

    private static final List<String> HOSTS = List.of("127.0.0.1", "127.0.0.2");

    @TempDir private Path resultsDir;
    private final List<B1Server> servers = new ArrayList<>();
    private int clientPort;
    private int peerPort;

    @BeforeEach
    void start() throws Exception {
        clientPort = freePort();
        peerPort = freePort();
        while (peerPort == clientPort) peerPort = freePort();
    }

    @AfterEach
    void stop() {
        servers.forEach(B1Server::close);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private Properties base() throws Exception {
        return base(HOSTS);
    }

    private Properties base(List<String> hosts) throws Exception {
        return base(hosts, 2);
    }

    private Properties base(List<String> hosts, int tabletsPerNode) throws Exception {
        for (int i = 0; i < hosts.size(); i++) {
            servers.add(B1Server.start(new B1NodeConfig(i, hosts, clientPort, peerPort, tabletsPerNode)));
        }
        Properties p = new Properties();
        p.setProperty("target.host", HOSTS.get(0));
        p.setProperty("target.port", String.valueOf(clientPort));
        p.setProperty("target.hosts", String.join(",", hosts.stream().map(h -> h + ":" + clientPort).toList()));
        p.setProperty("study", "cluster-it");
        p.setProperty("variant", "it");
        p.setProperty("scenario", "none");
        p.setProperty("seed", "7");
        p.setProperty("key.count", "2000");
        p.setProperty("key.length", "16");
        p.setProperty("value.size", "128");
        p.setProperty("mix.get", "40");
        p.setProperty("mix.set", "30");
        p.setProperty("mix.delete", "30");
        p.setProperty("distribution", "uniform");
        p.setProperty("concurrency", "8");
        p.setProperty("warmup.requests", "500");
        p.setProperty("measured.requests", "4000");
        p.setProperty("request.timeout.ms", "2000");
        p.setProperty("results.dir", resultsDir.toString());
        p.setProperty("cluster.mode", "b1");
        p.setProperty("cluster.peer.port", String.valueOf(peerPort));
        p.setProperty("load.rate", "2000");
        return p;
    }

    private String clusterJson(ExperimentResult result) throws IOException {
        Path dir = resultsDir.resolve("cluster-it");
        assertTrue(Files.exists(dir.resolve("timeline-" + result.executionId() + ".csv")));
        return Files.readString(dir.resolve("cluster-" + result.executionId() + ".json"));
    }

    private static long field(String json, String name) {
        Matcher m = Pattern.compile("\"" + name + "\": (-?\\d+)").matcher(json);
        assertTrue(m.find(), name + " missing in " + json);
        return Long.parseLong(m.group(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"stop-and-copy", "pre-copy", "cutover"})
    void movesTabletDuringOpenLoopLoadAndChecksConsistency(String method) throws Exception {
        Properties p = base();
        p.setProperty("migration.at.ms", "800");
        p.setProperty("migration.tablet", "1");
        p.setProperty("migration.to", "1");
        p.setProperty("migration.method", method);
        p.setProperty("migration.rate.bytes.per.sec", "500000");
        p.setProperty("migration.stop.threshold.bytes", "4096");
        RunnerConfig config = RunnerConfig.from(p);

        ExperimentResult result = new ExperimentRunner(config).run();
        String json = clusterJson(result);
        System.out.println(json);

        assertEquals(4000, field(json, "completedRequests"));
        assertEquals(0L, result.outcomeCounts().get(Outcome.CLIENT_FAILURE));
        assertEquals(2, field(json, "tableVersionEnd"));
        assertTrue(json.contains("\"phase\": 4"), json);
        assertEquals(0, field(json, "unverified"));
        assertEquals(0, field(json, "staleHit"));
        assertEquals(0, field(json, "wrongValue"));
        if (method.equals("cutover")) {
            // 복사가 없으므로 옮긴 Tablet에서 이동 뒤 다시 쓰지 않은 값은 사라진다(참고선)
            assertTrue(field(json, "lostSet") > 0, json);
        } else {
            assertEquals(0, field(json, "lostSet"));
        }
        // MOVED 수는 표 갱신과 요청의 타이밍에 따라 0일 수 있다. 끝까지 실패 없이 새 주인에 닿았는지만 본다
        assertEquals(0L, result.outcomeCounts().get(Outcome.SERVER_ERROR));
        assertEquals(0, field(json, "redirectExhausted"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void drainsThreeNodeClusterAndReportsSchedulerSummary(boolean fifo) throws Exception {
        Properties p = base(List.of("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        // A controller source publishes locally. Remote concurrent commits can race on the expected table version.
        p.setProperty("drain.node", fifo ? "0" : "2");
        p.setProperty("drain.at.ms", "200");
        if (fifo) {
            p.setProperty("migration.scheduler", "fifo");
            p.setProperty("migration.scheduler.max.concurrent", "2");
        }
        p.setProperty("migration.rate.bytes.per.sec", "500000");
        p.setProperty("migration.stop.threshold.bytes", "4096");

        String json = clusterJson(new ExperimentRunner(RunnerConfig.from(p)).run());
        assertEquals(2, field(json, "plannedMoves"));
        assertEquals(0, field(json, "remainingTablets"), json);
        assertEquals(fifo ? 2 : 1, field(json, "maxObservedConcurrent"));
        assertEquals(fifo ? 2 : 1, field(json, "maxConcurrent"));
        assertEquals(fifo ? 4 : 1, field(json, "maxPerNode"));
        assertTrue(json.contains("\"policy\": \"" + (fifo ? "fifo" : "sequential") + "\""), json);
        String moves = json.substring(json.indexOf("\"migrations\":"));
        var tablets = Pattern.compile("\"tablet\": (\\d+), \"from\":").matcher(moves).results()
                .map(m -> Long.parseLong(m.group(1))).toList();
        assertEquals(2, tablets.size(), json);
        assertTrue(json.contains("\"admissionOrder\": " + tablets), json);
        var times = Pattern.compile("\"queuedMs\": (\\d+\\.\\d+)").matcher(moves).results()
                .mapToDouble(m -> Double.parseDouble(m.group(1))).toArray();
        var sent = Pattern.compile("\"sentMs\": (\\d+\\.\\d+)").matcher(moves).results()
                .mapToDouble(m -> Double.parseDouble(m.group(1))).toArray();
        var finished = Pattern.compile("\"finishedMs\": (\\d+\\.\\d+)").matcher(moves).results()
                .mapToDouble(m -> Double.parseDouble(m.group(1))).toArray();
        var summary = Pattern.compile("\"makespanMs\": (\\d+\\.\\d+), \"meanJobCompletionMs\": (\\d+\\.\\d+)")
                .matcher(json);
        assertTrue(summary.find(), json);
        assertEquals(Math.max(finished[0], finished[1]) - sent[0], Double.parseDouble(summary.group(1)), 0.002);
        assertEquals((finished[0] - times[0] + finished[1] - times[1]) / 2,
                Double.parseDouble(summary.group(2)), 0.002);
        assertEquals(2, Pattern.compile("\"phase\": 4").matcher(json).results().count());
        assertTrue(json.contains("\"nodeRemoved\": false"), json);
        assertEquals(0, field(json, "lostSet"));
        assertEquals(0, field(json, "wrongValue"));
        assertEquals(0, field(json, "staleHit"));
        assertEquals(0, field(json, "unverified"));
    }

    @Test
    void drainsNonControllerWithFourConcurrentFifoMoves() throws Exception {
        Properties p = base(List.of("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"), 4);
        p.setProperty("drain.node", "3");
        p.setProperty("drain.at.ms", "200");
        p.setProperty("migration.scheduler", "fifo");
        p.setProperty("migration.scheduler.max.concurrent", "4");
        p.setProperty("migration.rate.bytes.per.sec", "500000");
        p.setProperty("migration.stop.threshold.bytes", "4096");

        String json = clusterJson(new ExperimentRunner(RunnerConfig.from(p)).run());
        assertEquals(4, field(json, "plannedMoves"));
        assertEquals(4, field(json, "maxObservedConcurrent"), json);
        assertEquals(0, field(json, "remainingTablets"), json);
        assertEquals(4, Pattern.compile("\"phase\": 4").matcher(json).results().count(), json);
        for (String metric : List.of("lostSet", "wrongValue", "staleHit", "unverified")) {
            assertEquals(0, field(json, metric), json);
        }
        List<Integer> retries = Pattern.compile("\"commitRetries\":(\\d+)").matcher(json).results()
                .map(m -> Integer.parseInt(m.group(1))).toList();
        assertEquals(4, retries.size(), json);
        System.out.println("Four-node FIFO drain commit retries by move: " + retries);
    }

    @Test
    void legacyMovesKeepSequentialOrderAndJsonFields() throws Exception {
        Properties p = base();
        p.setProperty("migration.at.ms", "100,100");
        p.setProperty("migration.tablet", "1,0");
        p.setProperty("migration.to", "1,1");

        String json = clusterJson(new ExperimentRunner(RunnerConfig.from(p)).run());
        String moves = json.substring(json.indexOf("\"migrations\":"));
        assertFalse(json.contains("\"scheduler\":"));
        assertFalse(json.contains("\"drain\":"));
        assertFalse(moves.contains("\"queuedMs\":"));
        assertFalse(moves.contains("\"admissionRetries\":"));
        var sequence = Pattern.compile("\"tablet\": (\\d+).*?\"sentMs\": (\\d+\\.\\d+).*?"
                + "\"finishedMs\": (\\d+\\.\\d+), \"phase\": 4, \"server\":").matcher(moves);
        assertTrue(sequence.find(), moves);
        assertEquals("1", sequence.group(1));
        double firstFinished = Double.parseDouble(sequence.group(3));
        assertTrue(sequence.find(), moves);
        assertEquals("0", sequence.group(1));
        assertTrue(Double.parseDouble(sequence.group(2)) >= firstFinished, moves);
        assertFalse(sequence.find());
    }

    @Test
    void runsWithoutMigrationAndRoutesDirectly() throws Exception {
        RunnerConfig config = RunnerConfig.from(base());
        ExperimentResult result = new ExperimentRunner(config).run();
        String json = clusterJson(result);
        assertEquals(0, field(json, "redirectedRequests"));
        assertEquals(0, field(json, "lostSet"));
        assertEquals(2, result.targets().size());
        assertTrue(result.targets().stream().allMatch(t -> t.requests() > 0));
        try (Stream<Path> files = Files.list(resultsDir.resolve("cluster-it"))) {
            assertEquals(2, files.count());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"heat", "midpoint", "estore"})
    void splitsAndMovesHotTabletWithB2PoliciesAndB3PreCopy(String split) throws Exception {
        Properties p = base();
        p.setProperty("distribution", "hot_range");
        p.setProperty("hot.range.key.fraction", "0.1");
        p.setProperty("hot.range.request.fraction", "0.9");
        p.setProperty("measured.requests", "16000");
        p.setProperty("balance.split", split);
        p.setProperty("balance.placement", "hysteresis");
        p.setProperty("balance.window.ms", "500");
        p.setProperty("balance.sustain.windows", "2");
        p.setProperty("balance.cooldown.windows", "1");
        p.setProperty("migration.method", "pre-copy");
        RunnerConfig config = RunnerConfig.from(p);

        ExperimentResult result = new ExperimentRunner(config).run();
        String json = clusterJson(result);
        System.out.println(json);

        assertEquals(0L, result.outcomeCounts().get(Outcome.CLIENT_FAILURE));
        assertEquals(0L, result.outcomeCounts().get(Outcome.SERVER_ERROR));
        assertEquals(0, field(json, "lostSet"));
        assertEquals(0, field(json, "staleHit"));
        assertEquals(0, field(json, "wrongValue"));
        assertFalse(json.contains("\"error\""), json);
        // 쏠린 Tablet 0(node 0)을 B2 정책으로 나누고 B3 pre-copy로 다른 노드에 옮긴다
        assertTrue(field(json, "splits") >= 1, json);
        assertTrue(field(json, "moves") >= 1, json);
    }

    @Test
    void rejectsClusterKeysWithoutClusterMode() throws Exception {
        Properties p = base();
        p.remove("cluster.mode");
        assertThrows(ConfigException.class, () -> RunnerConfig.from(p));
    }
}
