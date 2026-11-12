package io.openkache.runner.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KeyValueCodecTest {
    @Test
    void millionKeysRoundTripAndHighIndicesRemainDistinct() {
        for (int i = 0; i < 1_000_000; i++) {
            assertEquals(i, KeyValueCodec.keyIndex(KeyValueCodec.key(i, 24)));
        }
        assertEquals(Long.MAX_VALUE, KeyValueCodec.keyIndex(KeyValueCodec.key(Long.MAX_VALUE, 16)));
        assertNotEquals(KeyValueCodec.key(1, 24), KeyValueCodec.key(10, 24));
        assertNotEquals(KeyValueCodec.key(9_999_999, 24), KeyValueCodec.key(9_999_998, 24));
    }

    @Test
    void invalidKeyMagicPaddingAndLengthAreRejected() {
        byte[] key = KeyValueCodec.key(42, 24).toByteArray();
        key[23] = 1;
        assertThrows(IllegalArgumentException.class, () -> KeyValueCodec.keyIndex(Bytes.copyOf(key)));
        key[23] = 0; key[0] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> KeyValueCodec.keyIndex(Bytes.copyOf(key)));
        assertThrows(IllegalArgumentException.class, () -> KeyValueCodec.keyIndex(Bytes.copyOf(new byte[15])));
        assertThrows(IllegalArgumentException.class, () -> KeyValueCodec.key(-1, 24));
    }

    @ParameterizedTest
    @ValueSource(ints = {44, 100, 1024})
    void valueHasExactLengthAndSelfDescribingIdentity(int length) {
        TraceConfig config = new TraceConfig(101, 1000, 24, length, 0, 100, 0, 7,
                TraceConfig.Distribution.UNIFORM, 0);
        LogicalRequest request = new TraceGenerator(config).at(TracePhase.MEASURE, 999);
        Command.Set set = (Command.Set) request.command();
        assertEquals(length, set.value().size());
        var meta = KeyValueCodec.inspect(set.value());
        assertEquals(101, meta.seed());
        assertEquals(TracePhase.MEASURE, meta.phase());
        assertEquals(request.clientId(), meta.clientId());
        assertEquals(request.sequence(), meta.sequence());
        assertEquals(KeyValueCodec.keyIndex(set.key()), meta.keyIndex());
        byte[] copy = set.value().toByteArray();
        copy[0] ^= 1;
        assertEquals(meta, KeyValueCodec.inspect(set.value()));
    }

    @Test
    void corruptedValueAndInvalidMetadataAreRejected() {
        var config = TraceGeneratorTest.config(101, 0, 100, 0, 1);
        byte[] value = ((Command.Set) new TraceGenerator(config).at(TracePhase.MEASURE, 1).command()).value().toByteArray();
        value[45] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> KeyValueCodec.inspect(Bytes.copyOf(value)));
        value[45] ^= 1;
        ByteBuffer.wrap(value).putLong(16, -1);
        CRC32C crc = new CRC32C();
        crc.update(value, 0, value.length - 4);
        ByteBuffer.wrap(value).putInt(value.length - 4, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> KeyValueCodec.inspect(Bytes.copyOf(value)));
        assertThrows(IllegalArgumentException.class, () -> KeyValueCodec.inspect(Bytes.copyOf(new byte[43])));
    }
}

