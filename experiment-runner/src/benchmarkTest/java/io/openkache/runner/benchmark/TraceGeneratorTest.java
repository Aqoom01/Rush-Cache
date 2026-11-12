package io.openkache.runner.benchmark;

import io.openkache.core.replication.ionia.IoniaCommands;
import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TraceGeneratorTest {
    static TraceConfig config(long seed, int get, int set, int delete, int streams) {
        return new TraceConfig(seed, 257, 24, 100, get, set, delete, streams,
                TraceConfig.Distribution.UNIFORM, 0);
    }

    @Test
    void randomAccessIsIndependentOfInvocationOrderAndConcurrency() throws Exception {
        TraceGenerator a = new TraceGenerator(config(101, 80, 15, 5, 7));
        TraceGenerator b = new TraceGenerator(config(101, 80, 15, 5, 7));
        try (var pool = java.util.concurrent.Executors.newFixedThreadPool(4)) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<LogicalRequest>>();
            for (int i = 999; i >= 0; i--) {
                long index = i;
                futures.add(pool.submit(() -> b.at(TracePhase.MEASURE, index)));
            }
            for (int i = 0; i < 1000; i++) {
                assertEquals(a.at(TracePhase.MEASURE, 999 - i), futures.get(i).get());
            }
        }
    }

    @Test
    void phaseAndRuntimeNamespacesRemainDistinct() {
        TraceGenerator generator = new TraceGenerator(config(101, 0, 100, 0, 7));
        var load = generator.at(TracePhase.LOAD, 0);
        var warm = generator.at(TracePhase.WARMUP, 0);
        var measure = generator.at(TracePhase.MEASURE, 0);
        assertNotEquals(load.id("run", "load-1"), warm.id("run", "load-1"));
        assertNotEquals(warm.id("run", "load-1"), measure.id("run", "load-1"));
        assertNotEquals(measure.id("run", "load-1"), measure.id("run2", "load-1"));
        assertNotEquals(measure.id("run", "load-1"), measure.id("run", "load-2"));
        assertNotEquals(((Command.Set) warm.command()).value(), ((Command.Set) measure.command()).value());
        assertEquals(measure.id("run", "load-1"), generator.at(TracePhase.MEASURE, 0).id("run", "load-1"));
    }

    @Test
    void partitionsCoverEveryIndexOnceIncludingRemainders() {
        TraceGenerator generator = new TraceGenerator(config(101, 80, 15, 5, 7));
        Set<Long> seen = new HashSet<>();
        Set<LogicalRequest.RequestId> ids = new HashSet<>();
        for (int stream = 0; stream < 7; stream++) {
            Iterator<LogicalRequest> cursor = generator.stream(TracePhase.MEASURE, 1003, stream);
            long sequence = 0;
            while (cursor.hasNext()) {
                LogicalRequest request = cursor.next();
                assertEquals(stream, request.streamId());
                assertEquals(sequence++, request.sequence());
                assertEquals(generator.at(TracePhase.MEASURE, request.globalIndex()), request);
                assertTrue(seen.add(request.globalIndex()));
                assertTrue(ids.add(request.id("run", "load")));
            }
            assertThrows(NoSuchElementException.class, cursor::next);
        }
        assertEquals(1003, seen.size());
        for (long i = 0; i < 1003; i++) { assertTrue(seen.contains(i)); }
    }

    @Test
    void moreStreamsThanRequestsAndEmptyPhaseAreValid() {
        TraceGenerator generator = new TraceGenerator(config(101, 100, 0, 0, 7));
        assertFalse(generator.stream(TracePhase.WARMUP, 0, 0).hasNext());
        assertFalse(generator.stream(TracePhase.MEASURE, 3, 6).hasNext());
        assertTrue(generator.stream(TracePhase.MEASURE, 3, 2).hasNext());
        assertThrows(IllegalArgumentException.class, () -> generator.stream(TracePhase.MEASURE, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> generator.stream(TracePhase.MEASURE, 1, 7));
        assertThrows(IllegalArgumentException.class, () -> generator.stream(TracePhase.LOAD, 10, 0));
    }

    @Test
    void loadSetsEveryKeyExactlyOnceWithVerifiableInitialValue() {
        TraceGenerator generator = new TraceGenerator(config(101, 100, 0, 0, 7));
        Set<Bytes> keys = new HashSet<>();
        for (int i = 0; i < 257; i++) {
            var request = generator.at(TracePhase.LOAD, i);
            var set = assertInstanceOf(Command.Set.class, request.command());
            assertEquals(i, KeyValueCodec.keyIndex(set.key()));
            var meta = KeyValueCodec.inspect(set.value());
            assertEquals(TracePhase.LOAD, meta.phase());
            assertEquals(request.clientId(), meta.clientId());
            assertEquals(request.sequence(), meta.sequence());
            assertEquals(i, meta.keyIndex());
            assertTrue(keys.add(set.key()));
        }
        assertEquals(257, keys.size());
        assertThrows(IllegalArgumentException.class, () -> generator.at(TracePhase.LOAD, 257));
    }

    static Stream<Arguments> mixes() {
        return Stream.of(Arguments.of(100, 0, 0), Arguments.of(0, 100, 0),
                Arguments.of(0, 0, 100), Arguments.of(95, 5, 0),
                Arguments.of(50, 50, 0), Arguments.of(80, 15, 5));
    }

    @ParameterizedTest
    @MethodSource("mixes")
    void operationMixAndKeyRangeMatchContract(int get, int set, int delete) {
        TraceGenerator generator = new TraceGenerator(config(101, get, set, delete, 4));
        long[] counts = new long[3];
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            var request = generator.at(TracePhase.MEASURE, i);
            long key = KeyValueCodec.keyIndex(IoniaCommands.key(request.command()));
            assertTrue(key >= 0 && key < 257);
            switch (request.command()) {
                case Command.Get ignored -> counts[0]++;
                case Command.Set value -> { counts[1]++; KeyValueCodec.inspect(value.value()); }
                case Command.Delete ignored -> counts[2]++;
                default -> throw new IllegalArgumentException("Unsupported benchmark command");
            }
        }
        int[] percentages = {get, set, delete};
        for (int i = 0; i < 3; i++) {
            double p = percentages[i] / 100.0;
            double tolerance = 6 * Math.sqrt(total * p * (1 - p));
            assertTrue(Math.abs(counts[i] - total * p) <= tolerance);
        }
    }

    @Test
    void uniformAndFixedHotHaveSpecifiedProbabilities() {
        TraceGenerator uniform = new TraceGenerator(new TraceConfig(101, 32, 24, 100,
                100, 0, 0, 4, TraceConfig.Distribution.UNIFORM, 0));
        TraceGenerator hot = new TraceGenerator(new TraceConfig(101, 32, 24, 100,
                100, 0, 0, 4, TraceConfig.Distribution.FIXED_HOT, 0.9));
        int[] counts = new int[32];
        int hotCount = 0;
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            counts[(int) KeyValueCodec.keyIndex(IoniaCommands.key(uniform.at(TracePhase.MEASURE, i).command()))]++;
            if (KeyValueCodec.keyIndex(IoniaCommands.key(hot.at(TracePhase.MEASURE, i).command())) == 0) { hotCount++; }
        }
        for (int count : counts) { assertTrue(Math.abs(count - total / 32.0) < 400); }
        double expected = 0.9 + 0.1 / 32;
        assertTrue(Math.abs(hotCount / (double) total - expected) < 0.005);
    }

    @Test
    void hotProbabilityEndpointsAndSingleKeyAreSupported() {
        for (double p : new double[]{0, 1}) {
            TraceGenerator generator = new TraceGenerator(new TraceConfig(101, 1, 16, 44,
                    100, 0, 0, 1, TraceConfig.Distribution.FIXED_HOT, p));
            for (int i = 0; i < 100; i++) {
                assertEquals(0, KeyValueCodec.keyIndex(IoniaCommands.key(generator.at(TracePhase.MEASURE, i).command())));
            }
        }
    }

    @Test
    void largeIndicesAndClientNamespaceDoNotNarrowToInt() {
        TraceGenerator generator = new TraceGenerator(new TraceConfig(Long.MIN_VALUE,
                Long.MAX_VALUE, 16, 44, 0, 100, 0, Integer.MAX_VALUE,
                TraceConfig.Distribution.UNIFORM, 0));
        var request = generator.at(TracePhase.MEASURE, Long.MAX_VALUE);
        assertTrue(request.clientId() > Integer.MAX_VALUE);
        assertEquals(Long.MAX_VALUE / Integer.MAX_VALUE, request.sequence());
        assertTrue(KeyValueCodec.keyIndex(IoniaCommands.key(request.command())) >= 0);
        assertTrue(generator.stream(TracePhase.MEASURE, Long.MAX_VALUE, Integer.MAX_VALUE - 1).hasNext());
        assertThrows(IllegalArgumentException.class, () -> generator.at(TracePhase.MEASURE, -1));
    }

    @Test
    void invalidSizingMixAndDistributionFailBeforeGeneration() {
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 0, 24, 100, 95, 5, 0, 1, TraceConfig.Distribution.UNIFORM, 0));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 15, 100, 95, 5, 0, 1, TraceConfig.Distribution.UNIFORM, 0));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, 43, 95, 5, 0, 1, TraceConfig.Distribution.UNIFORM, 0));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, 100, 96, 5, 0, 1, TraceConfig.Distribution.UNIFORM, 0));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, 100, -1, 101, 0, 1, TraceConfig.Distribution.UNIFORM, 0));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, 100, 95, 5, 0, 0, TraceConfig.Distribution.UNIFORM, 0));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, 100, 95, 5, 0, 1, TraceConfig.Distribution.FIXED_HOT, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, 100, 95, 5, 0, 1, TraceConfig.Distribution.FIXED_HOT, 1.1));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, 100, 95, 5, 0, 1, TraceConfig.Distribution.UNIFORM, .1));
        assertThrows(IllegalArgumentException.class, () -> new TraceConfig(1, 1, 24, TraceConfig.MAX_BYTES + 1, 95, 5, 0, 1, TraceConfig.Distribution.UNIFORM, 0));
    }
}

