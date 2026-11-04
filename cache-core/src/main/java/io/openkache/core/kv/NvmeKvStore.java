package io.openkache.core.kv;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.spi.MetricsSink;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * NVMe 저장소. worker 하나가 고정 크기 segment 파일들에 기록을 이어 쓰고, key별 위치는 메모리 색인에 둔다.
 *
 * <p>기록 형식은 {@code keyLength u16 | valueLength u32 | version u64 | key | value}이고 segment 안에 빈틈없이 붙인다. 기록은
 * 쓰기 버퍼에 모았다가 버퍼가 차거나 segment를 바꿀 때 블록 단위로 쓴다. 용량이 차면 가장 오래된 segment를 지우고 색인이 아직 그
 * segment를 가리키는 key를 뺀다(쓰기 순서 FIFO). 재시작 복구가 없으므로 tombstone·CRC가 없다. 설계는
 * {@code docs/storage-nvme.md}.
 */
final class NvmeKvStore implements KvStore {

    static final int HEADER_BYTES = Short.BYTES + Integer.BYTES + Long.BYTES;
    private static final int MAX_KEY_BYTES = 0xFFFF;

    static final String READ_OPS = "storage_read_ops";
    static final String READ_BYTES = "storage_read_bytes";
    static final String READ_NANOS = "storage_read_nanos";
    static final String WRITE_OPS = "storage_write_ops";
    static final String WRITE_BYTES = "storage_write_bytes";
    static final String BUFFER_HITS = "storage_buffer_hits";
    static final String EVICTED_SEGMENTS = "storage_evicted_segments";
    static final String EVICTED_KEYS = "storage_evicted_keys";
    static final String ERRORS = "storage_errors";

    private static final class Segment {
        final Path path;
        final FileChannel channel;
        /** 이 segment에 쓴 key. 덮어쓴 key도 남아 있으므로 축출할 때 색인과 대조한다. */
        final List<Bytes> keys = new ArrayList<>();

        Segment(Path path, FileChannel channel) {
            this.path = path;
            this.channel = channel;
        }
    }

    private record Loc(Segment segment, long offset, int length, long version) {}

    private final Path dir;
    private final long segmentBytes;
    private final long maxSegments;
    private final int align;
    private final OpenOption[] openOptions;
    private final MetricsSink metrics;
    private final Map<Bytes, Loc> index = new HashMap<>();
    private final ArrayDeque<Segment> segments = new ArrayDeque<>();
    private final ByteBuffer writeBuffer;
    private ByteBuffer readBuffer;
    private Segment active;
    /** active segment에서 쓰기 버퍼 0번 byte의 위치. 블록 크기의 배수이고, 그 앞은 모두 파일에 쓰였다. */
    private long bufferStart;
    /** active segment에 지금까지 붙인 byte 수(버퍼에 있는 것 포함). */
    private long writeOffset;
    private int nextSegmentId;

    private NvmeKvStore(Path dir, KvStorage.Nvme config, long maxSegments, int align, MetricsSink metrics)
            throws IOException {
        this.dir = dir;
        this.segmentBytes = config.segmentBytes();
        this.maxSegments = maxSegments;
        this.align = align;
        this.openOptions = config.directIo()
                ? new OpenOption[] {StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE, directOption()}
                : new OpenOption[] {StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE};
        this.metrics = metrics;
        this.writeBuffer = aligned(config.writeBufferBytes(), align);
        this.readBuffer = aligned(align * 2, align);
        this.active = newSegment();
    }

    /**
     * worker {@code worker}의 저장소를 연다. {@code path/worker-<worker>} 아래의 이전 파일을 지우고 첫 segment를 만든다. direct
     * I/O를 쓸 수 없으면 여기서 실패한다.
     */
    static NvmeKvStore open(KvStorage.Nvme config, int worker, int workerCount, MetricsSink metrics) throws IOException {
        long maxSegments = config.segmentsPerWorker(workerCount);
        if (maxSegments < 2) {
            throw new IllegalArgumentException(
                    "capacityBytes / workers must hold at least 2 segments: " + config + " workers=" + workerCount);
        }
        Path dir = config.path().resolve("worker-" + worker);
        Files.createDirectories(dir);
        try (DirectoryStream<Path> old = Files.newDirectoryStream(dir, "seg-*.log")) {
            for (Path file : old) {
                Files.delete(file);
            }
        }
        int align = Math.toIntExact(Files.getFileStore(dir).getBlockSize());
        if (config.segmentBytes() % align != 0 || config.writeBufferBytes() % align != 0) {
            throw new IllegalArgumentException(
                    "segmentBytes and writeBufferBytes must be multiples of the block size " + align + ": " + config);
        }
        return new NvmeKvStore(dir, config, maxSegments, align, metrics);
    }

    @Override
    public Optional<Entry> get(Bytes key) throws StoreException {
        Loc loc = index.get(key);
        if (loc == null) {
            return Optional.empty();
        }
        byte[] record = read(loc);
        ByteBuffer header = ByteBuffer.wrap(record);
        int keyLength = Short.toUnsignedInt(header.getShort());
        int valueLength = header.getInt();
        long version = header.getLong();
        if (!sameKey(record, keyLength, key) || version != loc.version()) {
            metrics.increment(ERRORS, 1);
            throw new StoreException(ResultStatus.UNAVAILABLE, "record does not match index for key " + key, null);
        }
        int valueStart = HEADER_BYTES + keyLength;
        return Optional.of(new Entry(Bytes.copyOf(Arrays.copyOfRange(record, valueStart, valueStart + valueLength)), version));
    }

    @Override
    public long set(Bytes key, Bytes value) throws StoreException {
        long recordLength = (long) HEADER_BYTES + key.size() + value.size();
        if (key.size() > MAX_KEY_BYTES || recordLength > segmentBytes) {
            throw new StoreException(
                    ResultStatus.CAPACITY_EXCEEDED, "record of " + recordLength + " bytes exceeds the segment", null);
        }
        try {
            if (writeOffset + recordLength > segmentBytes) {
                rotate();
            }
            Loc previous = index.get(key);
            long version = previous == null ? 1 : previous.version() + 1;
            long offset = writeOffset;
            ByteBuffer record = ByteBuffer.allocate((int) recordLength);
            record.putShort((short) key.size()).putInt(value.size()).putLong(version);
            record.put(key.toByteArray()).put(value.toByteArray());
            append(record.array());
            active.keys.add(key);
            index.put(key, new Loc(active, offset, (int) recordLength, version));
            return version;
        } catch (IOException e) {
            throw ioFailure(e);
        }
    }

    @Override
    public boolean delete(Bytes key) {
        return index.remove(key) != null;
    }

    @Override
    public int size() {
        return index.size();
    }

    @Override
    public Partial scan(Predicate<Bytes> keyFilter, int limit) throws StoreException {
        List<KvEntry> matched = new ArrayList<>();
        for (Bytes key : index.keySet()) {
            if (keyFilter.test(key)) {
                Entry entry = get(key).orElseThrow();
                matched.add(new KvEntry(key, entry.value(), entry.version()));
            }
        }
        return KvStore.sorted(matched, limit);
    }

    @Override
    public void close() {
        for (Segment segment : segments) {
            try {
                segment.channel.close();
            } catch (IOException e) {
                // 닫는 중의 오류는 데이터에 영향이 없다(다음 시작 때 파일을 지운다).
            }
        }
    }

    /** {@code bytes}를 active segment 끝에 붙인다. 버퍼가 차면 그때마다 파일에 쓴다. */
    private void append(byte[] bytes) throws IOException {
        int position = 0;
        while (position < bytes.length) {
            int n = Math.min(writeBuffer.remaining(), bytes.length - position);
            writeBuffer.put(bytes, position, n);
            position += n;
            writeOffset += n;
            if (!writeBuffer.hasRemaining()) {
                writeBuffer.flip();
                writeFully(writeBuffer, bufferStart);
                bufferStart += writeBuffer.capacity();
                writeBuffer.clear();
            }
        }
    }

    /** 버퍼에 남은 기록을 블록 크기로 채워 쓰고, 자리가 없으면 가장 오래된 segment를 버린 뒤 새 segment를 연다. */
    private void rotate() throws IOException {
        if (writeBuffer.position() > 0) {
            int padded = roundUp(writeBuffer.position(), align);
            while (writeBuffer.position() < padded) {
                writeBuffer.put((byte) 0);
            }
            writeBuffer.flip();
            writeFully(writeBuffer, bufferStart);
        }
        if (segments.size() >= maxSegments) {
            evictOldest();
        }
        active = newSegment();
    }

    private void evictOldest() throws IOException {
        Segment oldest = segments.removeFirst();
        long removed = 0;
        for (Bytes key : oldest.keys) {
            Loc loc = index.get(key);
            if (loc != null && loc.segment() == oldest) {
                index.remove(key);
                removed++;
            }
        }
        oldest.channel.close();
        Files.deleteIfExists(oldest.path);
        metrics.increment(EVICTED_SEGMENTS, 1);
        metrics.increment(EVICTED_KEYS, removed);
    }

    private Segment newSegment() throws IOException {
        Path path = dir.resolve("seg-" + nextSegmentId++ + ".log");
        Segment segment = new Segment(path, FileChannel.open(path, openOptions));
        segments.addLast(segment);
        bufferStart = 0;
        writeOffset = 0;
        writeBuffer.clear();
        return segment;
    }

    /** 기록 하나를 읽는다. active segment에서 아직 버퍼에 있는 부분은 메모리에서 가져온다. */
    private byte[] read(Loc loc) throws StoreException {
        byte[] out = new byte[loc.length()];
        long start = loc.offset();
        long end = start + loc.length();
        long diskEnd = end;
        if (loc.segment() == active && end > bufferStart) {
            long bufferedFrom = Math.max(start, bufferStart);
            writeBuffer.get((int) (bufferedFrom - bufferStart), out, (int) (bufferedFrom - start), (int) (end - bufferedFrom));
            if (start >= bufferStart) {
                metrics.increment(BUFFER_HITS, 1);
                return out;
            }
            diskEnd = bufferStart;
        }
        try {
            readDisk(loc.segment(), start, diskEnd, out);
        } catch (IOException e) {
            throw ioFailure(e);
        }
        return out;
    }

    /** {@code [start, end)}를 덮는 블록을 읽어 {@code out} 앞쪽에 복사한다. */
    private void readDisk(Segment segment, long start, long end, byte[] out) throws IOException {
        long blockStart = start - start % align;
        int length = roundUp((int) (end - blockStart), align);
        if (readBuffer.capacity() < length) {
            readBuffer = aligned(length, align);
        }
        readBuffer.clear().limit(length);
        long began = System.nanoTime();
        int position = 0;
        while (readBuffer.hasRemaining()) {
            int n = segment.channel.read(readBuffer, blockStart + position);
            if (n < 0) {
                throw new IOException("unexpected end of " + segment.path + " at " + (blockStart + position));
            }
            position += n;
        }
        metrics.recordNanos(READ_NANOS, System.nanoTime() - began);
        metrics.increment(READ_OPS, 1);
        metrics.increment(READ_BYTES, length);
        readBuffer.get((int) (start - blockStart), out, 0, (int) (end - start));
    }

    private void writeFully(ByteBuffer buffer, long position) throws IOException {
        int length = buffer.remaining();
        long at = position;
        while (buffer.hasRemaining()) {
            at += active.channel.write(buffer, at);
        }
        metrics.increment(WRITE_OPS, 1);
        metrics.increment(WRITE_BYTES, length);
    }

    private StoreException ioFailure(IOException e) {
        metrics.increment(ERRORS, 1);
        return new StoreException(ResultStatus.UNAVAILABLE, "storage I/O failed in " + dir, e);
    }

    private static boolean sameKey(byte[] record, int keyLength, Bytes key) {
        if (keyLength != key.size()) {
            return false;
        }
        for (int i = 0; i < keyLength; i++) {
            if (record[HEADER_BYTES + i] != key.get(i)) {
                return false;
            }
        }
        return true;
    }

    private static int roundUp(int value, int align) {
        return (value + align - 1) / align * align;
    }

    private static ByteBuffer aligned(int capacity, int align) {
        return ByteBuffer.allocateDirect(capacity + align).alignedSlice(align).limit(capacity).slice();
    }

    /**
     * {@code ExtendedOpenOption.DIRECT}. 모든 JDK의 {@code jdk.unsupported} 모듈에 있지만 이름으로 참조하면 javac가 끌 수
     * 없는 경고를 내고 {@code -Werror}에 걸리므로 실행 시점에 찾는다.
     */
    private static OpenOption directOption() throws IOException {
        try {
            return (OpenOption) Class.forName("com.sun.nio.file.ExtendedOpenOption").getField("DIRECT").get(null);
        } catch (ReflectiveOperationException e) {
            throw new IOException("direct I/O is not available on this JDK", e);
        }
    }
}
