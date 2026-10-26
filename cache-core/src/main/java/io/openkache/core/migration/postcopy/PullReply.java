package io.openkache.core.migration.postcopy;

import io.openkache.core.migration.MigrationJob;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * 고정 source의 key 한 건에 대한 pull 응답. 없는 값도 검증 가능한 응답으로 표현한다.
 * session은 in-process job identity이며 실제 peer wire 형식이 아니다.
 */
public record PullReply(MigrationJob session, OwnershipEpoch epoch, long requestId, Bytes key,
        Optional<Bytes> value, long checksum) {
    public PullReply {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(epoch, "epoch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (requestId < 0) throw new IllegalArgumentException("negative request id");
    }

    /** epoch 8 + requestId 8 + checksum 8 + 존재 1 + key/value 길이 각 4 + payload. */
    public long serializedBytes() { return 33L + key.size() + value.map(Bytes::size).orElse(0); }

    public static PullReply create(MigrationJob session, OwnershipEpoch epoch, long requestId,
            Bytes key, Optional<Bytes> value) {
        PullReply reply = new PullReply(session, epoch, requestId, key, value, 0);
        return new PullReply(session, epoch, requestId, key, value, reply.crc());
    }

    public boolean valid() { return checksum == crc(); }

    private long crc() {
        CRC32C crc = new CRC32C();
        number(crc, epoch.value(), 8);
        number(crc, requestId, 8);
        crc.update(value.isPresent() ? 1 : 0);
        number(crc, key.size(), 4);
        number(crc, value.map(Bytes::size).orElse(0), 4);
        for (int i = 0; i < key.size(); i++) crc.update(key.get(i));
        value.ifPresent(bytes -> {
            for (int i = 0; i < bytes.size(); i++) crc.update(bytes.get(i));
        });
        return crc.getValue();
    }

    private static void number(CRC32C crc, long value, int width) {
        for (int i = width - 1; i >= 0; i--) crc.update((int) (value >>> (8 * i)) & 255);
    }
}
