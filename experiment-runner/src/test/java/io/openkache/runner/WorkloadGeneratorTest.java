package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class WorkloadGeneratorTest {

    private static RunnerConfig config(String distribution, int mixGet, int mixSet, int mixDelete) {
        Properties p = new Properties();
        p.setProperty("target.host", "127.0.0.1");
        p.setProperty("target.port", "7000");
        p.setProperty("study", "s");
        p.setProperty("variant", "v");
        p.setProperty("scenario", "none");
        p.setProperty("seed", "1");
        p.setProperty("key.count", "50");
        p.setProperty("key.length", "16");
        p.setProperty("value.size", "8");
        p.setProperty("mix.get", String.valueOf(mixGet));
        p.setProperty("mix.set", String.valueOf(mixSet));
        p.setProperty("mix.delete", String.valueOf(mixDelete));
        p.setProperty("distribution", distribution);
        p.setProperty("hot.key.fraction", "0.9");
        p.setProperty("concurrency", "1");
        p.setProperty("warmup.requests", "0");
        p.setProperty("measured.requests", "1");
        p.setProperty("request.timeout.ms", "1000");
        p.setProperty("results.dir", "/tmp/openkache-results");
        return RunnerConfig.from(p);
    }

    @Test
    void sameSeedProducesSameSequence() {
        RunnerConfig cfg = config("UNIFORM", 50, 30, 20);
        WorkloadGenerator a = new WorkloadGenerator(cfg, new Random(7));
        WorkloadGenerator b = new WorkloadGenerator(cfg, new Random(7));

        for (int i = 0; i < 200; i++) {
            assertEquals(a.next(), b.next());
        }
    }

    @Test
    void mixRatioIsApproximatelyRespectedOverManyRequests() {
        RunnerConfig cfg = config("UNIFORM", 70, 20, 10);
        WorkloadGenerator generator = new WorkloadGenerator(cfg, new Random(123));

        int gets = 0;
        int sets = 0;
        int deletes = 0;
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            Command command = generator.next();
            if (command instanceof Command.Get) {
                gets++;
            } else if (command instanceof Command.Set) {
                sets++;
            } else {
                deletes++;
            }
        }

        assertTrue(Math.abs(gets / (double) total - 0.70) < 0.01);
        assertTrue(Math.abs(sets / (double) total - 0.20) < 0.01);
        assertTrue(Math.abs(deletes / (double) total - 0.10) < 0.01);
    }

    @Test
    void uniformDistributionCoversAllKeys() {
        RunnerConfig cfg = config("UNIFORM", 100, 0, 0);
        WorkloadGenerator generator = new WorkloadGenerator(cfg, new Random(5));

        Set<String> seenKeys = new HashSet<>();
        for (int i = 0; i < 5_000; i++) {
            Command.Get get = (Command.Get) generator.next();
            seenKeys.add(get.key().toString());
        }

        assertEquals(50, seenKeys.size()); // key.count=50
    }

    @Test
    void fixedHotKeyStronglyFavorsOneKey() {
        RunnerConfig cfg = config("FIXED_HOT_KEY", 100, 0, 0);
        WorkloadGenerator generator = new WorkloadGenerator(cfg, new Random(9));

        // index 0이 고정 hot key다. key 인코딩은 "key-<index>"를 key.length로 0-padding한 것이다.
        byte[] hotKeyBytes = new byte[cfg.keyLength()];
        System.arraycopy("key-0".getBytes(StandardCharsets.UTF_8), 0, hotKeyBytes, 0, 5);
        String hotKey = Bytes.copyOf(hotKeyBytes).toString();

        int hotCount = 0;
        int total = 10_000;
        for (int i = 0; i < total; i++) {
            Command.Get get = (Command.Get) generator.next();
            if (get.key().toString().equals(hotKey)) {
                hotCount++;
            }
        }

        // hot.key.fraction=0.9 + 나머지 균등(1/50) 중 일부도 index 0을 고를 수 있어 0.9보다 약간 높게 나온다.
        assertTrue(hotCount / (double) total > 0.85);
    }
}
