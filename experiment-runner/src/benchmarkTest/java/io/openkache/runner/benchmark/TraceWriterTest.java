package io.openkache.runner.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.api.Command;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class TraceWriterTest {
    @Test
    void frozenV1VectorsRequireVersionedChanges() throws Exception {
        var generator = new TraceGenerator(new TraceConfig(101, 17, 24, 100,
                80, 15, 5, 4, TraceConfig.Distribution.UNIFORM, 0));
        assertEquals("f69991256372671ee8bb1abf82fc31cae227549c3e8cb5b88ab29553c950dd39",
                TraceWriter.write(generator, TracePhase.LOAD, 17, OutputStream.nullOutputStream()).sha256());
        assertEquals("233310c772610e1a0e3fbb37f0c7f0a787600cfa433b49daa0f7c13eca53aedd",
                TraceWriter.write(generator, TracePhase.WARMUP, 37, OutputStream.nullOutputStream()).sha256());
        assertEquals("99b5250d809d75050f40468cf59554384ef7ffa9f309ae313d96c44f313dfb44",
                TraceWriter.write(generator, TracePhase.MEASURE, 101, OutputStream.nullOutputStream()).sha256());
    }

    @Test
    void binaryHeaderRecordsHashAndCountsAreComplete() throws Exception {
        TraceConfig config = TraceGeneratorTest.config(101, 80, 15, 5, 7);
        TraceGenerator generator = new TraceGenerator(config);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        var summary = TraceWriter.write(generator, TracePhase.MEASURE, 1003, bytes);
        byte[] encoded = bytes.toByteArray();
        assertEquals(encoded.length, summary.canonicalBytes());
        assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(encoded)), summary.sha256());
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
        assertEquals("OKTRACE1", new String(in.readNBytes(8), StandardCharsets.US_ASCII));
        assertEquals(1, in.readInt());
        assertEquals(config.seed(), in.readLong());
        assertEquals(config.keyCount(), in.readLong());
        assertEquals(config.keyBytes(), in.readInt());
        assertEquals(config.valueBytes(), in.readInt());
        assertEquals(config.getPercent(), in.readInt());
        assertEquals(config.setPercent(), in.readInt());
        assertEquals(config.deletePercent(), in.readInt());
        assertEquals(config.streamCount(), in.readInt());
        assertEquals(0, in.readInt());
        assertEquals(0L, in.readLong());
        assertEquals(TracePhase.MEASURE.tag(), in.readInt());
        assertEquals(1003L, in.readLong());
        long[] counts = new long[3];
        for (int i = 0; i < 1003; i++) {
            var request = generator.at(TracePhase.MEASURE, i);
            assertEquals(i, in.readLong());
            assertEquals(request.streamId(), in.readInt());
            assertEquals(request.clientId(), in.readLong());
            assertEquals(request.sequence(), in.readLong());
            int operation = in.readUnsignedByte();
            counts[operation]++;
            assertEquals(24, in.readInt());
            assertArrayEquals(request.command().key().toByteArray(), in.readNBytes(24));
            int valueSize = in.readInt();
            if (request.command() instanceof Command.Set set) {
                assertEquals(1, operation);
                assertEquals(100, valueSize);
                assertArrayEquals(set.value().toByteArray(), in.readNBytes(valueSize));
            } else {
                assertEquals(-1, valueSize);
                assertEquals(request.command() instanceof Command.Get ? 0 : 2, operation);
            }
        }
        assertEquals(-1, in.read());
        assertEquals(counts[0], summary.gets());
        assertEquals(counts[1], summary.sets());
        assertEquals(counts[2], summary.deletes());
        assertEquals(1003, summary.gets() + summary.sets() + summary.deletes());
    }

    @Test
    void hashOnlyAndEmittedBytesMatchAndConfigChangesAreVisible() throws Exception {
        var generator = new TraceGenerator(TraceGeneratorTest.config(101, 80, 15, 5, 7));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var emitted = TraceWriter.write(generator, TracePhase.MEASURE, 1003, out);
        assertEquals(emitted, TraceWriter.write(generator, TracePhase.MEASURE, 1003, OutputStream.nullOutputStream()));
        assertNotEquals(emitted.sha256(), TraceWriter.write(generator, TracePhase.WARMUP, 1003, OutputStream.nullOutputStream()).sha256());
        assertNotEquals(emitted.sha256(), TraceWriter.write(new TraceGenerator(TraceGeneratorTest.config(202, 80, 15, 5, 7)), TracePhase.MEASURE, 1003, OutputStream.nullOutputStream()).sha256());
        assertNotEquals(emitted.sha256(), TraceWriter.write(new TraceGenerator(TraceGeneratorTest.config(101, 80, 15, 5, 8)), TracePhase.MEASURE, 1003, OutputStream.nullOutputStream()).sha256());
    }

    @Test
    void emptyPhaseHasHeaderHashAndZeroCounts() throws Exception {
        var summary = TraceWriter.write(new TraceGenerator(TraceGeneratorTest.config(101, 80, 15, 5, 7)),
                TracePhase.WARMUP, 0, OutputStream.nullOutputStream());
        assertEquals(76, summary.canonicalBytes());
        assertEquals(0, summary.count());
        assertEquals(0, summary.gets() + summary.sets() + summary.deletes());
        assertEquals(64, summary.sha256().length());
    }

    @Test
    void outputFailurePropagates() {
        OutputStream broken = new OutputStream() {
            @Override
            public void write(int value) throws IOException { throw new IOException("intentional"); }
        };
        assertThrows(IOException.class, () -> TraceWriter.write(
                new TraceGenerator(TraceGeneratorTest.config(101, 80, 15, 5, 7)),
                TracePhase.MEASURE, 10, broken));
    }
}

