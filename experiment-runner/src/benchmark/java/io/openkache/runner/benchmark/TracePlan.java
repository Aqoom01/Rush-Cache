package io.openkache.runner.benchmark;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;

/** generator 전용 설정. benchmark run manifest나 네트워크 설정을 읽지 않는다. */
record TracePlan(TraceConfig config, long warmupRequests, long measuredRequests) {
    private static final Set<String> FIELDS = Set.of(
            "seed", "key.count", "key.bytes", "value.bytes", "mix.get", "mix.set",
            "mix.delete", "stream.count", "distribution", "hot.probability",
            "warmup.requests", "measured.requests");

    TracePlan {
        if (warmupRequests < 0 || measuredRequests < 0) {
            throw new IllegalArgumentException("phase requests must be nonnegative");
        }
    }

    static TracePlan read(Path path) throws IOException {
        Properties p = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            p.load(reader);
        }
        if (!p.stringPropertyNames().equals(FIELDS)) {
            throw new IllegalArgumentException("required fields exactly: " + FIELDS);
        }
        try {
            TraceConfig config = new TraceConfig(
                    Long.parseLong(p.getProperty("seed").trim()),
                    Long.parseLong(p.getProperty("key.count").trim()),
                    Integer.parseInt(p.getProperty("key.bytes").trim()),
                    Integer.parseInt(p.getProperty("value.bytes").trim()),
                    Integer.parseInt(p.getProperty("mix.get").trim()),
                    Integer.parseInt(p.getProperty("mix.set").trim()),
                    Integer.parseInt(p.getProperty("mix.delete").trim()),
                    Integer.parseInt(p.getProperty("stream.count").trim()),
                    TraceConfig.Distribution.valueOf(p.getProperty("distribution").trim()),
                    Double.parseDouble(p.getProperty("hot.probability").trim()));
            return new TracePlan(config,
                    Long.parseLong(p.getProperty("warmup.requests").trim()),
                    Long.parseLong(p.getProperty("measured.requests").trim()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid numeric generator field", e);
        }
    }
}

