package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class RunnerConfigTest {

    private static Properties valid() {
        Properties p = new Properties();
        p.setProperty("target.host", "127.0.0.1");
        p.setProperty("target.port", "7000");
        p.setProperty("study", "baseline");
        p.setProperty("variant", "single-node");
        p.setProperty("scenario", "none");
        p.setProperty("seed", "42");
        p.setProperty("key.count", "100");
        p.setProperty("key.length", "16");
        p.setProperty("value.size", "64");
        p.setProperty("mix.get", "70");
        p.setProperty("mix.set", "20");
        p.setProperty("mix.delete", "10");
        p.setProperty("distribution", "uniform");
        p.setProperty("concurrency", "4");
        p.setProperty("warmup.requests", "100");
        p.setProperty("measured.requests", "1000");
        p.setProperty("request.timeout.ms", "2000");
        p.setProperty("results.dir", "/tmp/openkache-results");
        return p;
    }

    private static String errorOf(Properties p) {
        return assertThrows(ConfigException.class, () -> RunnerConfig.from(p)).getMessage();
    }

    @Test
    void parsesValidConfig() {
        RunnerConfig config = RunnerConfig.from(valid());

        assertEquals("127.0.0.1", config.targetHost());
        assertEquals(7000, config.targetPort());
        assertEquals(42, config.seed());
        assertEquals(Distribution.UNIFORM, config.distribution());
        assertEquals(4, config.concurrency());
        assertEquals(1, config.repeatCount());
        assertEquals(600_000, config.runTimeoutMs());
        assertEquals(3_600_000, config.executionTimeoutMs());
        assertEquals("baseline", config.configSnapshot().get("study"));
        assertEquals(-1, config.serverWorkerCountHint()); // 적지 않으면 미측정
    }

    @Test
    void rejectsUnknownKey() {
        Properties p = valid();
        p.setProperty("typo.key", "x");

        assertEquals("invalid config: unknown key 'typo.key'", errorOf(p));
    }

    @Test
    void rejectsMixNotSummingToHundred() {
        Properties p = valid();
        p.setProperty("mix.get", "50");

        assertEquals(
                "invalid config: mix.get + mix.set + mix.delete must equal 100: 80", errorOf(p));
    }

    @Test
    void rejectsZipfianAsUnsupportedDistribution() {
        Properties p = valid();
        p.setProperty("distribution", "zipfian");

        assertEquals(
                "invalid config: 'distribution' must be one of [UNIFORM, FIXED_HOT_KEY] (zipfian is not"
                        + " supported yet): zipfian",
                errorOf(p));
    }

    @Test
    void rejectsKeyLengthTooShortToDistinguishAllKeys() {
        Properties p = valid();
        p.setProperty("key.count", "1000"); // "key-999"는 7글자
        p.setProperty("key.length", "5");

        String error = errorOf(p);
        assertEquals(true, error.contains("key.length"));
    }

    @Test
    void acceptsServerWorkerCountHintWhenProvided() {
        Properties p = valid();
        p.setProperty("server.workers.hint", "2");

        assertEquals(2, RunnerConfig.from(p).serverWorkerCountHint());
    }

    @Test
    void acceptsRepeatCountWhenProvided() {
        Properties p = valid();
        p.setProperty("repeat.count", "3");

        assertEquals(3, RunnerConfig.from(p).repeatCount());
    }

    @Test
    void acceptsExecutionTimeoutWhenProvided() {
        Properties p = valid();
        p.setProperty("execution.timeout.ms", "5000");

        assertEquals(5000, RunnerConfig.from(p).executionTimeoutMs());
    }

    @Test
    void acceptsRunTimeoutWhenProvided() {
        Properties p = valid();
        p.setProperty("run.timeout.ms", "5000");

        assertEquals(5000, RunnerConfig.from(p).runTimeoutMs());
    }
}
