package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.server.config.ServerConfig;
import io.openkache.server.lifecycle.ServerApplication;
import io.openkache.server.routing.B1NodeConfig;
import io.openkache.server.routing.B1Server;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ClusterExperimentConfigTest {
    private static final Path ROOT = Path.of("..", "experiments", "cluster-integration");
    private static final Path E1 = ROOT.resolve("e1-routing-overhead");
    private static final Path E2 = ROOT.resolve("e2-moved/01-precopy-20.properties");

    @TempDir Path results;

    static List<Path> e1Configs() throws IOException {
        try (Stream<Path> paths = Files.list(E1)) {
            return paths.filter(p -> p.toString().endsWith(".properties")).sorted().toList();
        }
    }

    @ParameterizedTest
    @MethodSource("e1Configs")
    void e1BoundariesMatchInitialB1Owners(Path path) throws IOException {
        Properties properties = load(path);
        int members = members(properties);
        int tabletsPerNode = Integer.parseInt(properties.getProperty("deploy.b1.tablets.per.node", "1"));
        boolean b1 = properties.getProperty("deploy.server").equals("b1");
        RunnerConfig config = RunnerConfig.from(forRunner(properties, 7000));
        assertTrue(ClusterConfig.from(config.configSnapshot()).isEmpty());
        assertEquals(members, config.routedTargetCount());
        assertEquals(members - 1, config.routingBoundaries().size());
        if (!b1) {
            assertEquals(1, members);
            return;
        }

        var table = new HashRangeScheme(tabletsPerNode).initial(
                IntStream.range(0, members).mapToObj(NodeId::new).toList());
        for (var tablet : table.tablets()) {
            assertEquals(tablet.owner().value(), config.targetIndex(tablet.range().start()));
            assertEquals(tablet.owner().value(), config.targetIndex(tablet.range().end() - 1));
        }
        for (int node = 1; node < members; node++) {
            assertEquals(table.tablets().get(node * tabletsPerNode).range().start(),
                    config.routingBoundaries().get(node - 1));
        }
    }

    @Test
    void rehearsesE1LowStageAgainstKacheAndB1() throws Exception {
        int client = freePort();
        var server = ServerApplication.start(new ServerConfig(new NodeId(0), "e1-rehearsal",
                "127.0.0.1", "127.0.0.1", client, freePort(), freePort(), 2, Duration.ofSeconds(2)));
        try {
            Properties config = forRunner(load(E1.resolve("01-kache-n1-c8.properties")), client);
            assertClosedLoopResult(new ExperimentRunner(RunnerConfig.from(small(config))).run(), 1);
        } finally {
            server.close();
        }

        Properties config = load(E1.resolve("05-b1-n4-c8.properties"));
        withB1(config, (clientPort, peerPort) -> {
            Properties runner = small(forRunner(config, clientPort));
            assertClosedLoopResult(new ExperimentRunner(RunnerConfig.from(runner)).run(), 4);
        });
    }

    @Test
    void rehearsesFirstThreeE2PreCopyMovesAndChecksFullSchedule() throws Exception {
        Properties config = load(E2);
        int members = members(config);
        int tabletsPerNode = Integer.parseInt(config.getProperty("deploy.b1.tablets.per.node"));
        RunnerConfig full = RunnerConfig.from(forRunner(config, 7000));
        ClusterConfig cluster = ClusterConfig.from(full.configSnapshot()).orElseThrow();
        var moves = cluster.migrations();
        assertEquals(8, members);
        assertEquals(20, moves.size());
        assertEquals(20, moves.stream().map(ClusterConfig.Migration::tablet).distinct().count());
        assertEquals(members, moves.stream().map(ClusterConfig.Migration::to).distinct().count());
        assertTrue(moves.getLast().atMs() < full.measuredRequests() * 1000 / cluster.loadRate());
        for (var move : moves) {
            assertTrue(move.tablet() < members * tabletsPerNode);
            assertTrue(move.to() < members);
            assertNotEquals(move.tablet() / tabletsPerNode, move.to());
            assertEquals(ClusterConfig.Method.PRE_COPY, move.method());
            assertEquals(0, move.rateBytesPerSec());
            assertEquals(65536, move.batchBytes());
            assertEquals(262144, move.stopThresholdBytes());
            assertEquals(10, move.maxRounds());
        }

        withB1(config, (clientPort, peerPort) -> {
            Properties runner = small(forRunner(config, clientPort));
            runner.setProperty("cluster.peer.port", Integer.toString(peerPort));
            runner.setProperty("load.rate", "1000");
            runner.setProperty("measured.requests", "1600");
            runner.setProperty("migration.at.ms", "150,450,750");
            for (String key : List.of("migration.tablet", "migration.to")) {
                runner.setProperty(key, String.join(",", Arrays.copyOf(runner.getProperty(key).split(","), 3)));
            }
            var result = new ExperimentRunner(RunnerConfig.from(runner)).run();
            String json = Files.readString(results.resolve("cluster-e2-moved")
                    .resolve("cluster-" + result.executionId() + ".json"));
            assertEquals(3, Pattern.compile("\"phase\": 4").matcher(json).results().count(), json);
            assertTrue(json.contains("\"tableVersionEnd\": 4"), json);
            for (String field : List.of("latencyByHops", "2+", "redirectExhausted", "redirectedRequests")) {
                assertTrue(json.contains("\"" + field + "\":"), field);
            }
            for (String counter : List.of("lostSet", "wrongValue", "staleHit", "unverified")) {
                assertTrue(Pattern.compile("\"" + counter + "\": 0\\b").matcher(json).find(), json);
            }
            assertTrue(Pattern.compile("\"checked\": [1-9]\\d*").matcher(json).find(), json);
            assertEquals(0L, result.outcomeCounts().get(Outcome.CLIENT_FAILURE));
        });
    }

    private static void assertClosedLoopResult(ExperimentResult result, int targets) {
        assertEquals(targets, result.targets().size());
        assertTrue(result.targets().stream().allMatch(t -> t.requests() > 0));
        assertEquals(result.measuredRequests(), result.targets().stream().mapToLong(TargetResult::requests).sum());
        for (Outcome failure : List.of(Outcome.SERVER_ERROR, Outcome.CLIENT_FAILURE, Outcome.BUSY, Outcome.TIMEOUT)) {
            assertEquals(0L, result.outcomeCounts().get(failure), failure.toString());
        }
    }

    private Properties small(Properties properties) {
        properties.setProperty("key.count", "1000");
        properties.setProperty("warmup.requests", "200");
        properties.setProperty("measured.requests", "1000");
        properties.setProperty("results.dir", results.toString());
        return properties;
    }

    private static Properties load(Path file) throws IOException {
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(file)) {
            properties.load(reader);
        }
        return properties;
    }

    private static int members(Properties properties) {
        return Integer.parseInt(properties.getProperty("deploy.members"));
    }

    private static List<String> hosts(int count) {
        return IntStream.rangeClosed(1, count).mapToObj(i -> "127.0.0." + i).toList();
    }

    private static Properties forRunner(Properties properties, int clientPort) {
        Properties runner = new Properties();
        runner.putAll(properties);
        runner.keySet().removeIf(k -> k.toString().startsWith("deploy."));
        runner.setProperty("target.hosts", String.join(",",
                hosts(members(properties)).stream().map(h -> h + ":" + clientPort).toList()));
        return runner;
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void withB1(Properties config, B1Run run) throws Exception {
        List<String> hosts = hosts(members(config));
        int tablets = Integer.parseInt(config.getProperty("deploy.b1.tablets.per.node"));
        int client = freePort();
        int peer = freePort();
        while (peer == client) peer = freePort();
        List<B1Server> servers = new ArrayList<>();
        try {
            for (int i = 0; i < hosts.size(); i++) {
                servers.add(B1Server.start(new B1NodeConfig(i, hosts, client, peer, tablets)));
            }
            run.run(client, peer);
        } finally {
            servers.forEach(B1Server::close);
        }
    }

    @FunctionalInterface
    private interface B1Run {
        void run(int clientPort, int peerPort) throws Exception;
    }
}
