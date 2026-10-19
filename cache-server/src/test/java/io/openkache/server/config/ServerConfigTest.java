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
}
