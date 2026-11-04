package io.openkache.core.migration.precopy;

import io.openkache.core.model.Bytes;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Pre-copy 변경분 batch. seq는 job 안에서 1부터 증가하고 빈 value와 삭제를 구별한다.
 * 실제 peer wire가 아닌 in-process 전송 크기·무결성 모델이다. 모든 호출은 controller thread에서 한다.
 */
public record DeltaBatch(long batchId, int recordCount, long serializedBytes, long checksum,
        boolean last, boolean finalRound, List<Delta> records) {
    public static final int HEADER_BYTES = 30;

    /** value가 비어 있으면 DELETE tombstone. 빈 바이트열을 PUT하는 것과 다르다. */
    public record Delta(long seq, Bytes key, Optional<Bytes> value) {
        public Delta {
            if (seq <= 0) throw new IllegalArgumentException("non-positive sequence");
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }

        /** seq 8 + PUT 여부 1 + key/value 길이 각 4 + payload bytes. */
        public long serializedBytes() { return 17L + key.size() + value.map(Bytes::size).orElse(0); }
    }

    /** 적용 검증 metadata만 ACK에 보존하며 과거 value payload를 붙잡아 두지 않는다. */
    public record Acknowledgement(long batchId, int recordCount, long serializedBytes, long checksum,
            boolean last, boolean finalRound, long lastSequence) {
        public static Acknowledgement of(DeltaBatch batch) {
            return new Acknowledgement(batch.batchId(), batch.recordCount(), batch.serializedBytes(), batch.checksum(),
                    batch.last(), batch.finalRound(), batch.records().isEmpty() ? 0 : batch.records().getLast().seq());
        }
        public boolean matches(DeltaBatch batch) { return equals(of(batch)); }
    }

    public DeltaBatch {
        if (batchId < 0) throw new IllegalArgumentException("negative batch id");
        records = List.copyOf(records);
    }

    /** payload와 metadata로 CRC32C를 계산한다. */
    public static DeltaBatch create(long id, boolean last, boolean finalRound, List<Delta> records) {
        DeltaBatch batch = new DeltaBatch(id, records.size(), size(records), 0, last, finalRound, records);
        return new DeltaBatch(id, records.size(), batch.serializedBytes(), batch.crc(), last, finalRound, records);
    }

    /** 수신 적용 전에 검증한다. */
    public boolean valid() {
        return recordCount == records.size() && serializedBytes == size(records) && checksum == crc();
    }

    private static long size(List<Delta> records) {
        long size = HEADER_BYTES;
        for (Delta delta : records) size = Math.addExact(size, delta.serializedBytes());
        return size;
    }

    private long crc() {
        CRC32C crc = new CRC32C();
        number(crc, batchId, 8);
        number(crc, recordCount, 4);
        number(crc, serializedBytes, 8);
        crc.update(last ? 1 : 0);
        crc.update(finalRound ? 1 : 0);
        for (Delta delta : records) {
            number(crc, delta.seq(), 8);
            crc.update(delta.value().isPresent() ? 1 : 0);
            number(crc, delta.key().size(), 4);
            number(crc, delta.value().map(Bytes::size).orElse(0), 4);
            for (int i = 0; i < delta.key().size(); i++) crc.update(delta.key().get(i));
            delta.value().ifPresent(value -> {
                for (int i = 0; i < value.size(); i++) crc.update(value.get(i));
            });
        }
        return crc.getValue();
    }

    private static void number(CRC32C crc, long value, int width) {
        for (int i = width - 1; i >= 0; i--) crc.update((int) (value >>> (8 * i)) & 255);
    }
}
