package io.openkache.runner.benchmark;

import io.openkache.core.api.Command;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** canonical binary v1 출력/hash. 호출별 독립 상태이며 OutputStream은 한 thread가 소유한다. */
public final class TraceWriter {
    private TraceWriter() {}

    /**
     * globalIndex 순서로 phase를 streaming 출력한다. 모든 byte를 SHA-256에 포함한다.
     * out은 flush하지만 닫지 않는다. nullOutputStream이면 같은 내용의 hash만 계산한다.
     */
    public static Summary write(TraceGenerator generator, TracePhase phase, long count,
            OutputStream out) throws IOException {
        Objects.requireNonNull(generator, "generator").validateCount(phase, count);
        Objects.requireNonNull(out, "out");
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK must provide SHA-256", impossible);
        }
        CountingOutputStream counting = new CountingOutputStream(out);
        DataOutputStream data = new DataOutputStream(
                new BufferedOutputStream(new DigestOutputStream(counting, digest), 65_536));
        TraceConfig c = generator.config();
        data.write("OKTRACE1".getBytes(StandardCharsets.US_ASCII));
        data.writeInt(1);
        data.writeLong(c.seed());
        data.writeLong(c.keyCount());
        data.writeInt(c.keyBytes());
        data.writeInt(c.valueBytes());
        data.writeInt(c.getPercent());
        data.writeInt(c.setPercent());
        data.writeInt(c.deletePercent());
        data.writeInt(c.streamCount());
        data.writeInt(c.distribution() == TraceConfig.Distribution.UNIFORM ? 0 : 1);
        data.writeLong(Double.doubleToLongBits(c.hotProbability()));
        data.writeInt(phase.tag());
        data.writeLong(count);
        long gets = 0;
        long sets = 0;
        long deletes = 0;
        for (long index = 0; index < count; index++) {
            LogicalRequest request = generator.at(phase, index);
            data.writeLong(request.globalIndex());
            data.writeInt(request.streamId());
            data.writeLong(request.clientId());
            data.writeLong(request.sequence());
            byte[] value = null;
            switch (request.command()) {
                case Command.Get ignored -> { data.writeByte(0); gets++; }
                case Command.Set set -> { data.writeByte(1); sets++; value = set.value().toByteArray(); }
                case Command.Delete ignored -> { data.writeByte(2); deletes++; }
            }
            byte[] key = request.command().key().toByteArray();
            data.writeInt(key.length);
            data.write(key);
            data.writeInt(value == null ? -1 : value.length);
            if (value != null) { data.write(value); }
        }
        data.flush();
        return new Summary(phase, count, gets, sets, deletes, counting.count,
                HexFormat.of().formatHex(digest.digest()));
    }

    /** 생성 개수와 canonical bytes/hash. 시간·성능·실제 요청 성공 수를 뜻하지 않는다. */
    public record Summary(TracePhase phase, long count, long gets, long sets,
            long deletes, long canonicalBytes, String sha256) {}

    private static final class CountingOutputStream extends OutputStream {
        private final OutputStream delegate;
        private long count;

        private CountingOutputStream(OutputStream delegate) { this.delegate = delegate; }

        @Override
        public void write(int value) throws IOException {
            delegate.write(value);
            count = Math.addExact(count, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            delegate.write(bytes, offset, length);
            count = Math.addExact(count, length);
        }

        @Override
        public void flush() throws IOException { delegate.flush(); }
    }
}

