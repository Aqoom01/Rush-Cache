package io.openkache.core.migration;

import io.openkache.core.model.Bytes;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Snapshot batch의 검증 metadata와 불변 payload. 설계 §3을 따른다.
 *
 * <p>실험 framing: id 8 + count 4 + bytes 8 + checksum 8 + last 1 = 29 bytes.
 * record는 key/value 길이 각 4 bytes와 내용이다. CRC32C는 checksum을 제외한 metadata와 payload를 검증한다.
 * 실제 peer wire 규약은 아니며 job session과 Tablet epoch는 adapter에 별도로 전달한다.
 */
public record TransferBatch(long batchId, int recordCount, long serializedBytes, long checksum,
        boolean last, List<Record> records) {
    public static final int HEADER_BYTES = 29;

    /** 불변 snapshot record. */
    public record Record(Bytes key, Bytes value) {
        public Record {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }

        /** 길이 필드를 포함한 bytes. */
        public long serializedBytes() { return 8L + key.size() + value.size(); }
    }

    public TransferBatch {
        if (batchId < 0) throw new IllegalArgumentException("negative batch id");
        records = List.copyOf(records);
    }

    /** payload에서 count, 크기, checksum을 계산한다. */
    public static TransferBatch create(long id, boolean last, List<Record> records) {
        TransferBatch metadata = new TransferBatch(id, records.size(), size(records), 0, last, records);
        return new TransferBatch(id, records.size(), metadata.serializedBytes, metadata.crc(), last, records);
    }

    /** 적용 전에 count, byte 수, checksum을 모두 검사한다. */
    public boolean valid() {
        return recordCount == records.size() && serializedBytes == size(records) && checksum == crc();
    }

    private static long size(List<Record> records) {
        long bytes = HEADER_BYTES;
        for (Record record : records) bytes = Math.addExact(bytes, record.serializedBytes());
        return bytes;
    }

    private long crc() {
        CRC32C crc = new CRC32C();
        number(crc, batchId, 8);
        number(crc, recordCount, 4);
        number(crc, serializedBytes, 8);
        crc.update(last ? 1 : 0);
        for (Record record : records) {
            number(crc, record.key.size(), 4);
            number(crc, record.value.size(), 4);
            for (int i = 0; i < record.key.size(); i++) crc.update(record.key.get(i));
            for (int i = 0; i < record.value.size(); i++) crc.update(record.value.get(i));
        }
        return crc.getValue();
    }

    private static void number(CRC32C crc, long value, int width) {
        for (int i = width - 1; i >= 0; i--) crc.update((int) (value >>> (8 * i)) & 255);
    }
}
