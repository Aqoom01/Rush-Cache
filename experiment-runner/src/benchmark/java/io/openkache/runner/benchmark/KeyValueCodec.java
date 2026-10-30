package io.openkache.runner.benchmark;

import io.openkache.core.model.Bytes;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32C;

/** 고정 big-endian key/value v1. 상태가 없으며 여러 thread에서 사용 가능하다. */
public final class KeyValueCodec {
    public static final int MIN_KEY_BYTES = 16;
    public static final int MIN_VALUE_BYTES = 44;
    private static final long KEY_MAGIC = 0x4f4b4b4559563031L;
    private static final int VALUE_MAGIC = 0x4f4b5631;

    private KeyValueCodec() {}

    /** prefix8B+음이 아닌 index8B+zero padding. index를 절단하지 않고 구분한다. */
    public static Bytes key(long index, int length) {
        if (index < 0 || length < MIN_KEY_BYTES || length > TraceConfig.MAX_BYTES) {
            throw new IllegalArgumentException("invalid key index/length");
        }
        byte[] bytes = new byte[length];
        ByteBuffer.wrap(bytes).putLong(KEY_MAGIC).putLong(index);
        return Bytes.copyOf(bytes);
    }

    /** v1 key의 index를 복호화하고 magic/padding/부호를 검사한다. */
    public static long keyIndex(Bytes key) {
        byte[] bytes = Objects.requireNonNull(key, "key").toByteArray();
        if (bytes.length < MIN_KEY_BYTES || bytes.length > TraceConfig.MAX_BYTES) {
            throw new IllegalArgumentException("invalid key length");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.getLong() != KEY_MAGIC) { throw new IllegalArgumentException("invalid key magic"); }
        long index = buffer.getLong();
        if (index < 0) { throw new IllegalArgumentException("negative key index"); }
        while (buffer.hasRemaining()) {
            if (buffer.get() != 0) { throw new IllegalArgumentException("invalid key padding"); }
        }
        return index;
    }

    static Bytes value(int length, long seed, TracePhase phase, long clientId,
            long sequence, long keyIndex, long payloadSeed) {
        byte[] bytes = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(VALUE_MAGIC).putInt(phase.tag()).putLong(seed)
                .putLong(clientId).putLong(sequence).putLong(keyIndex);
        IndexedRandom random = new IndexedRandom(payloadSeed);
        long word = 0;
        for (int i = 40; i < length - Integer.BYTES; i++) {
            if ((i - 40) % Long.BYTES == 0) { word = random.nextLong(); }
            bytes[i] = (byte) word;
            word >>>= Byte.SIZE;
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, length - Integer.BYTES);
        buffer.putInt(length - Integer.BYTES, (int) crc.getValue());
        return Bytes.copyOf(bytes);
    }

    /** value의 magic/CRC32C/metadata를 검사한다. CRC는 최신성·암호학적 진위를 증명하지 않는다. */
    public static ValueMetadata inspect(Bytes value) {
        byte[] bytes = Objects.requireNonNull(value, "value").toByteArray();
        if (bytes.length < MIN_VALUE_BYTES || bytes.length > TraceConfig.MAX_BYTES) {
            throw new IllegalArgumentException("invalid value length");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.getInt() != VALUE_MAGIC) { throw new IllegalArgumentException("invalid value magic"); }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        if ((int) crc.getValue() != buffer.getInt(bytes.length - Integer.BYTES)) {
            throw new IllegalArgumentException("value CRC32C mismatch");
        }
        TracePhase phase = TracePhase.fromTag(buffer.getInt());
        long seed = buffer.getLong();
        long client = buffer.getLong();
        long sequence = buffer.getLong();
        long key = buffer.getLong();
        if (client < 0 || sequence < 0 || key < 0) {
            throw new IllegalArgumentException("negative value metadata");
        }
        return new ValueMetadata(seed, phase, client, sequence, key);
    }

    /** 검사한 value의 식별 정보. 단위는 logical index이며 thread 간 공유 가능하다. */
    public record ValueMetadata(long seed, TracePhase phase, long clientId,
            long sequence, long keyIndex) {}
}

