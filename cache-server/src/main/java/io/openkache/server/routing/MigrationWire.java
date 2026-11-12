package io.openkache.server.routing;

import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.precopy.DeltaBatch;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.TabletRange;
import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * B1+B3 Tablet 이동 peer 확장(protocol-v1.md §6-5). 모든 정수는 big-endian이다.
 * 잘못된 길이·종류는 IllegalArgumentException으로 거절한다.
 */
final class MigrationWire {

    /** 실험 스크립트 → source 노드: 이동 시작. */
    static final int MIGRATE = 10;
    /** 실험 스크립트 → source 노드: 이동 상태 조회. */
    static final int MIGRATE_STATUS = 11;
    /** source → destination: 준비·데이터 batch·폐기. 응답 OK가 적용 완료 ACK다. */
    static final int TRANSFER = 12;

    static final int PREPARE = 0;
    static final int SNAPSHOT = 1;
    static final int DELTA = 2;
    static final int DISCARD = 3;

    /** 복사 방식. CUTOVER는 복사 없이 소유권만 넘기고 source 범위를 지운다(E2·E3 참고선). */
    enum Method {
        STOP_AND_COPY, PRE_COPY, CUTOVER;

        static Method of(int code) {
            Method[] all = values();
            if (code < 0 || code >= all.length) throw new IllegalArgumentException("unknown method: " + code);
            return all[code];
        }
    }

    /**
     * MIGRATE body.
     *
     * @param rateBytesPerSec 이동 대역 상한(0 = 상한 없음)
     * @param batchBytes 헤더 포함 batch 목표 크기
     * @param stopThresholdBytes pre-copy 종료 임계(pre-copy만)
     * @param maxRounds pre-copy live 라운드 상한(pre-copy만)
     */
    record Migrate(Method method, long tabletId, long to, long rateBytesPerSec, int batchBytes,
                   long stopThresholdBytes, int maxRounds) {}

    /** TRANSFER 공통 머리. */
    record Header(long from, long to, long jobId, long tabletId, long epoch, int kind) {}

    record Transfer(Header header, TabletRange range, TransferBatch snapshot, DeltaBatch delta) {}

    private MigrationWire() {}

    static byte[] encodeMigrate(Migrate m) {
        return ByteBuffer.allocate(41).put((byte) m.method().ordinal()).putLong(m.tabletId()).putLong(m.to())
                .putLong(m.rateBytesPerSec()).putInt(m.batchBytes()).putLong(m.stopThresholdBytes())
                .putInt(m.maxRounds()).array();
    }

    static Migrate decodeMigrate(byte[] body) {
        ByteBuffer b = ByteBuffer.wrap(body);
        try {
            Migrate m = new Migrate(Method.of(Byte.toUnsignedInt(b.get())), b.getLong(), b.getLong(), b.getLong(),
                    b.getInt(), b.getLong(), b.getInt());
            end(b);
            if (m.rateBytesPerSec() < 0 || m.batchBytes() <= TransferBatch.HEADER_BYTES + 1
                    || m.batchBytes() > 1024 * 1024 || m.stopThresholdBytes() < 0 || m.maxRounds() < 0) {
                throw new IllegalArgumentException("invalid migrate settings");
            }
            return m;
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("short migrate body");
        }
    }

    static byte[] prepare(Header h, TabletRange range) {
        return header(h, 16).putLong(range.start()).putLong(range.end()).array();
    }

    static byte[] discard(Header h) {
        return header(h, 0).array();
    }

    static byte[] snapshot(Header h, TransferBatch batch) {
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) batch.serializedBytes() + 64);
        out.writeBytes(header(h, 0).array());
        out.writeBytes(ByteBuffer.allocate(21).putLong(batch.batchId()).put((byte) (batch.last() ? 1 : 0))
                .putLong(batch.checksum()).putInt(batch.recordCount()).array());
        for (TransferBatch.Record r : batch.records()) {
            bytes(out, r.key());
            bytes(out, r.value());
        }
        return out.toByteArray();
    }

    static byte[] delta(Header h, DeltaBatch batch) {
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) batch.serializedBytes() + 64);
        out.writeBytes(header(h, 0).array());
        out.writeBytes(ByteBuffer.allocate(22).putLong(batch.batchId()).put((byte) (batch.last() ? 1 : 0))
                .put((byte) (batch.finalRound() ? 1 : 0)).putLong(batch.checksum()).putInt(batch.recordCount()).array());
        for (DeltaBatch.Delta d : batch.records()) {
            out.writeBytes(ByteBuffer.allocate(9).putLong(d.seq()).put((byte) (d.value().isPresent() ? 1 : 0)).array());
            bytes(out, d.key());
            bytes(out, d.value().orElse(Bytes.empty()));
        }
        return out.toByteArray();
    }

    /** checksum을 다시 계산해 보낸 값과 다르면 거절한다. */
    static Transfer decodeTransfer(byte[] body) {
        ByteBuffer b = ByteBuffer.wrap(body);
        try {
            Header h = new Header(b.getLong(), b.getLong(), b.getLong(), b.getLong(), b.getLong(),
                    Byte.toUnsignedInt(b.get()));
            Transfer t = switch (h.kind()) {
                case PREPARE -> new Transfer(h, new TabletRange(b.getLong(), b.getLong()), null, null);
                case DISCARD -> new Transfer(h, null, null, null);
                case SNAPSHOT -> {
                    long id = b.getLong();
                    boolean last = flag(b);
                    long checksum = b.getLong();
                    int count = count(b);
                    List<TransferBatch.Record> records = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) records.add(new TransferBatch.Record(bytes(b), bytes(b)));
                    TransferBatch batch = TransferBatch.create(id, last, records);
                    if (batch.checksum() != checksum) throw new IllegalArgumentException("snapshot checksum mismatch");
                    yield new Transfer(h, null, batch, null);
                }
                case DELTA -> {
                    long id = b.getLong();
                    boolean last = flag(b);
                    boolean finalRound = flag(b);
                    long checksum = b.getLong();
                    int count = count(b);
                    List<DeltaBatch.Delta> records = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        long seq = b.getLong();
                        boolean present = flag(b);
                        Bytes key = bytes(b);
                        Bytes value = bytes(b);
                        if (!present && !value.isEmpty()) throw new IllegalArgumentException("tombstone with value");
                        records.add(new DeltaBatch.Delta(seq, key, present ? Optional.of(value) : Optional.empty()));
                    }
                    DeltaBatch batch = DeltaBatch.create(id, last, finalRound, records);
                    if (batch.checksum() != checksum) throw new IllegalArgumentException("delta checksum mismatch");
                    yield new Transfer(h, null, null, batch);
                }
                default -> throw new IllegalArgumentException("unknown transfer kind: " + h.kind());
            };
            end(b);
            return t;
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("short transfer body");
        }
    }

    private static ByteBuffer header(Header h, int extra) {
        return ByteBuffer.allocate(41 + extra).putLong(h.from()).putLong(h.to()).putLong(h.jobId())
                .putLong(h.tabletId()).putLong(h.epoch()).put((byte) h.kind());
    }

    private static void bytes(ByteArrayOutputStream out, Bytes value) {
        out.writeBytes(ByteBuffer.allocate(4).putInt(value.size()).array());
        out.writeBytes(value.toByteArray());
    }

    private static Bytes bytes(ByteBuffer b) {
        int length = b.getInt();
        if (length < 0 || length > b.remaining()) throw new IllegalArgumentException("invalid field length: " + length);
        byte[] data = new byte[length];
        b.get(data);
        return Bytes.copyOf(data);
    }

    private static int count(ByteBuffer b) {
        int count = b.getInt();
        // record 하나는 길이 필드만으로도 8 bytes 이상이다
        if (count < 0 || count > b.remaining() / 8) throw new IllegalArgumentException("invalid record count: " + count);
        return count;
    }

    private static boolean flag(ByteBuffer b) {
        int v = Byte.toUnsignedInt(b.get());
        if (v > 1) throw new IllegalArgumentException("invalid flag: " + v);
        return v == 1;
    }

    private static void end(ByteBuffer b) {
        if (b.hasRemaining()) throw new IllegalArgumentException("trailing bytes: " + b.remaining());
    }
}
