package io.openkache.core.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.spi.MetricsSink;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NvmeKvStoreTest {

    @TempDir
    Path dir;

    private final CountingSink metrics = new CountingSink();
    private NvmeKvStore store;

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    /** segment {@code segmentBlocks}블록, 버퍼 1블록, worker 1개가 segment {@code segments}개를 쓰는 저장소. */
    private NvmeKvStore open(int segmentBlocks, int segments, boolean directIo) throws IOException {
        long block = Files.getFileStore(dir).getBlockSize();
        long segmentBytes = segmentBlocks * block;
        KvStorage.Nvme config = new KvStorage.Nvme(dir, segmentBytes * segments, segmentBytes, (int) block, directIo);
        store = NvmeKvStore.open(config, 0, 1, metrics);
        return store;
    }

    private int block() throws IOException {
        return Math.toIntExact(Files.getFileStore(dir).getBlockSize());
    }

    @Test
    void recentWriteIsServedFromBufferAndOlderWriteFromDisk() throws Exception {
        open(8, 2, false);
        Bytes first = value(1, 100);
        store.set(Bytes.utf8("first"), first);
        assertEquals(first, store.get(Bytes.utf8("first")).orElseThrow().value());
        assertEquals(1, metrics.get(NvmeKvStore.BUFFER_HITS));
        assertEquals(0, metrics.get(NvmeKvStore.READ_OPS));

        // 버퍼(1블록)를 넘겨 first가 파일로 내려가게 한다.
        store.set(Bytes.utf8("second"), value(2, block()));
        assertEquals(first, store.get(Bytes.utf8("first")).orElseThrow().value());
        assertEquals(1, metrics.get(NvmeKvStore.READ_OPS));
    }

    @Test
    void recordSpanningFlushedAndBufferedBytesIsReassembled() throws Exception {
        open(8, 2, false);
        int half = block() / 2;
        store.set(Bytes.utf8("a"), value(1, half));
        Bytes spanning = value(2, half + 100);
        store.set(Bytes.utf8("b"), spanning);

        assertEquals(spanning, store.get(Bytes.utf8("b")).orElseThrow().value());
        assertEquals(1, metrics.get(NvmeKvStore.READ_OPS));
    }

    @Test
    void recordsInOlderSegmentsRemainReadableAfterRotation() throws Exception {
        open(2, 4, false);
        Random random = new Random(7);
        Bytes[] values = new Bytes[10];
        for (int i = 0; i < values.length; i++) {
            values[i] = value(i, 300 + random.nextInt(900));
            store.set(Bytes.utf8("k" + i), values[i]);
        }
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], store.get(Bytes.utf8("k" + i)).orElseThrow().value(), "k" + i);
        }
    }

    @Test
    void versionFollowsMemoryStoreRules() throws Exception {
        open(4, 2, false);
        Bytes key = Bytes.utf8("k");
        assertEquals(1, store.set(key, Bytes.utf8("v1")));
        assertEquals(2, store.set(key, Bytes.utf8("v2")));
        assertEquals(2, store.get(key).orElseThrow().version());
        assertTrue(store.delete(key));
        assertFalse(store.delete(key));
        assertTrue(store.get(key).isEmpty());
        assertEquals(1, store.set(key, Bytes.utf8("v3")));
    }

    @Test
    void fullStoreEvictsOldestSegmentButKeepsRewrittenKeys() throws Exception {
        open(1, 2, false);
        // segment(1블록) 하나에 기록이 정확히 4개 들어가는 크기.
        int quarter = block() / 4 - 30;
        for (String key : new String[] {"old", "kept", "a1", "a2"}) {
            store.set(Bytes.utf8(key), value(key.hashCode(), quarter));
        }
        Bytes rewritten = value(5, quarter);
        assertEquals(2, store.set(Bytes.utf8("kept"), rewritten));
        for (String key : new String[] {"b1", "b2", "b3"}) {
            store.set(Bytes.utf8(key), value(key.hashCode(), quarter));
        }
        // 세 번째 segment가 필요해 첫 segment(old, 처음 쓴 kept, a1, a2)를 버린다.
        store.set(Bytes.utf8("c1"), value(6, quarter));

        assertTrue(store.get(Bytes.utf8("old")).isEmpty());
        assertTrue(store.get(Bytes.utf8("a1")).isEmpty());
        assertEquals(rewritten, store.get(Bytes.utf8("kept")).orElseThrow().value());
        assertEquals(2, store.get(Bytes.utf8("kept")).orElseThrow().version());
        assertEquals(1, metrics.get(NvmeKvStore.EVICTED_SEGMENTS));
        assertEquals(3, metrics.get(NvmeKvStore.EVICTED_KEYS));
        assertEquals(5, store.size());
    }

    @Test
    void recordLargerThanSegmentIsCapacityExceeded() throws Exception {
        open(1, 2, false);
        StoreException e = assertThrows(StoreException.class, () -> store.set(Bytes.utf8("big"), value(1, block())));
        assertEquals(ResultStatus.CAPACITY_EXCEEDED, e.status());
        assertEquals(0, store.size());
    }

    @Test
    void openRemovesFilesLeftByPreviousRun() throws Exception {
        Path worker = Files.createDirectories(dir.resolve("worker-0"));
        Files.write(worker.resolve("seg-99.log"), new byte[] {1, 2, 3});
        open(4, 2, false);
        assertFalse(Files.exists(worker.resolve("seg-99.log")));
        assertTrue(store.get(Bytes.utf8("anything")).isEmpty());
    }

    @Test
    void capacityBelowTwoSegmentsPerWorkerIsRejected() throws IOException {
        long block = Files.getFileStore(dir).getBlockSize();
        KvStorage.Nvme config = new KvStorage.Nvme(dir, block * 3, block, (int) block, false);
        assertThrows(IllegalArgumentException.class, () -> NvmeKvStore.open(config, 0, 2, metrics));
    }

    @Test
    void directIoReadsAndWritesAlignedBlocks() throws Exception {
        try {
            open(4, 2, true);
        } catch (IOException e) {
            Assumptions.abort("direct I/O not supported here: " + e);
        }
        Bytes small = value(1, 10);
        Bytes large = value(2, block() + 123);
        store.set(Bytes.utf8("small"), small);
        store.set(Bytes.utf8("large"), large);
        store.set(Bytes.utf8("pad"), value(3, block()));

        assertEquals(small, store.get(Bytes.utf8("small")).orElseThrow().value());
        assertEquals(large, store.get(Bytes.utf8("large")).orElseThrow().value());
        assertTrue(metrics.get(NvmeKvStore.READ_OPS) >= 2);
        assertEquals(0, metrics.get(NvmeKvStore.READ_BYTES) % block());
    }

    @Test
    void scanReadsValuesFromStorage() throws Exception {
        open(2, 4, false);
        for (int i = 0; i < 5; i++) {
            store.set(Bytes.utf8("k" + i), value(i, 500));
        }
        KvStore.Partial partial = store.scan(key -> true, 3);
        assertTrue(partial.truncated());
        assertEquals(3, partial.entries().size());
        assertEquals(Bytes.utf8("k0"), partial.entries().get(0).key());
        assertArrayEquals(value(0, 500).toByteArray(), partial.entries().get(0).value().toByteArray());
    }

    private static Bytes value(int seed, int size) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return Bytes.copyOf(bytes);
    }

    private static final class CountingSink implements MetricsSink {
        private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

        long get(String name) {
            AtomicLong counter = counters.get(name);
            return counter == null ? 0 : counter.get();
        }

        @Override
        public void increment(String name, long delta) {
            counters.computeIfAbsent(name, n -> new AtomicLong()).addAndGet(delta);
        }

        @Override
        public void recordNanos(String name, long nanos) {}

        @Override
        public void gauge(String name, double value) {}
    }
}
