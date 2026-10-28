package io.openkache.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ServerConfigTest {

    private static Properties valid() {
        Properties p = new Properties();
        p.setProperty("node.id", "1");
        p.setProperty("cluster.id", "openkache-local");
        p.setProperty("bind.host", "127.0.0.1");
        p.setProperty("admin.host", "127.0.0.1");
        p.setProperty("client.port", "7000");
        p.setProperty("peer.port", "7001");
        p.setProperty("admin.port", "9091");
        p.setProperty("workers", "2");
        p.setProperty("request.timeout.ms", "2000");
        p.setProperty("cluster.members", "1@127.0.0.1:7001,2@127.0.0.1:7011");
        p.setProperty("swim.probe-interval.ms", "500");
        p.setProperty("swim.direct-timeout.ms", "100");
        p.setProperty("swim.indirect-timeout.ms", "100");
        p.setProperty("swim.suspicion-timeout.ms", "1500");
        p.setProperty("swim.indirect-checks", "3");
        p.setProperty("swim.gossip-retransmit-multiplier", "3");
        p.setProperty("swim.max-piggyback-updates", "8");
        return p;
    }

    private static String errorOf(Properties p) {
        return assertThrows(ConfigException.class, () -> ServerConfig.from(p)).getMessage();
    }

    @Test
    void parsesValidConfig() {
        ServerConfig config = ServerConfig.from(valid());

        assertEquals(1, config.nodeId().value());
        assertEquals(7000, config.clientPort());
        assertEquals(2, config.workers());
        assertEquals(Duration.ofMillis(2000), config.requestTimeout());
        assertEquals(2, config.peerAddresses().size());
        assertEquals(7001, config.peerAddresses().get(config.nodeId()).port());
        assertEquals(Duration.ofMillis(500), config.swimConfig().probeInterval());
    }

    @Test
    void bundledDefaultConfigIsValid() {
        ServerConfig config = ServerConfig.load(null);

        assertEquals("127.0.0.1", config.adminHost());
    }

    @Test
    void rejectsPortOutOfRange() {
        Properties p = valid();
        p.setProperty("client.port", "70000");

        assertTrue(errorOf(p).contains("client.port"));
    }

    @Test
    void rejectsZeroPort() {
        Properties p = valid();
        p.setProperty("peer.port", "0");

        assertTrue(errorOf(p).contains("peer.port"));
    }

    @Test
    void rejectsDuplicatePorts() {
        Properties p = valid();
        p.setProperty("admin.port", "7000");

        assertTrue(errorOf(p).contains("duplicate 7000"));
    }

    @Test
    void rejectsInvalidWorkerCount() {
        Properties zero = valid();
        zero.setProperty("workers", "0");
        Properties tooMany = valid();
        tooMany.setProperty("workers", "257");

        assertTrue(errorOf(zero).contains("workers"));
        assertTrue(errorOf(tooMany).contains("workers"));
    }

    @Test
    void rejectsNonNumericValue() {
        Properties p = valid();
        p.setProperty("request.timeout.ms", "2s");

        assertTrue(errorOf(p).contains("must be an integer"));
    }

    @Test
    void rejectsMissingAndUnknownKeysTogether() {
        Properties p = valid();
        p.remove("cluster.id");
        p.setProperty("worker", "2"); // 오타

        String message = errorOf(p);
        assertTrue(message.contains("'cluster.id' is required"));
        assertTrue(message.contains("unknown key 'worker'"));
    }

    @Test
    void rejectsDuplicateMemberIdAndAddress() {
        Properties duplicateId = valid();
        duplicateId.setProperty("cluster.members", "1@127.0.0.1:7001,1@127.0.0.1:7011");
        Properties duplicateAddress = valid();
        duplicateAddress.setProperty("cluster.members", "1@127.0.0.1:7001,2@127.0.0.1:7001");

        assertTrue(errorOf(duplicateId).contains("duplicate NodeId"));
        assertTrue(errorOf(duplicateAddress).contains("duplicate peer address"));
    }

    @Test
    void rejectsMissingLocalMemberAndMismatchedLocalPort() {
        Properties missing = valid();
        missing.setProperty("cluster.members", "2@127.0.0.1:7011");
        Properties wrongPort = valid();
        wrongPort.setProperty("cluster.members", "1@127.0.0.1:7011");

        assertTrue(errorOf(missing).contains("must contain local node 1"));
        assertTrue(errorOf(wrongPort).contains("must equal peer.port 7001"));
    }

    @Test
    void rejectsProbeIntervalShorterThanProbeTimeouts() {
        Properties p = valid();
        p.setProperty("swim.probe-interval.ms", "100");
        p.setProperty("swim.direct-timeout.ms", "80");
        p.setProperty("swim.indirect-timeout.ms", "80");

        assertTrue(errorOf(p).contains("invalid SWIM config"));
    }

    @Test
    void rejectsPiggybackLimitAboveCodecLimit() {
        Properties p = valid();
        p.setProperty("swim.max-piggyback-updates", "1025");

        assertTrue(errorOf(p).contains("swim.max-piggyback-updates"));
    }
}
