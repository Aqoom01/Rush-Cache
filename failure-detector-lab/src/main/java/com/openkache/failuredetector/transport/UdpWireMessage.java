package com.openkache.failuredetector.transport;

import java.nio.ByteBuffer;

/** Small versioned wire frame used only by the real-UDP validation harness. */
public record UdpWireMessage(
        Type type,
        long sequence,
        int sender,
        int origin,
        int target
) {
    public static final int MAGIC = 0x46444C42;
    public static final byte VERSION = 1;
    public static final int WIRE_BYTES = Integer.BYTES + Byte.BYTES * 2
            + Long.BYTES + Integer.BYTES * 3;

    public byte[] encode() {
        return ByteBuffer.allocate(WIRE_BYTES)
                .putInt(MAGIC)
                .put(VERSION)
                .put(type.code)
                .putLong(sequence)
                .putInt(sender)
                .putInt(origin)
                .putInt(target)
                .array();
    }

    public static UdpWireMessage decode(byte[] bytes, int length) {
        if (length != WIRE_BYTES) {
            throw new IllegalArgumentException("unexpected UDP frame length: " + length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, length);
        if (buffer.getInt() != MAGIC) {
            throw new IllegalArgumentException("invalid UDP frame magic");
        }
        if (buffer.get() != VERSION) {
            throw new IllegalArgumentException("unsupported UDP frame version");
        }
        Type type = Type.fromCode(buffer.get());
        return new UdpWireMessage(
                type,
                buffer.getLong(),
                buffer.getInt(),
                buffer.getInt(),
                buffer.getInt()
        );
    }

    public enum Type {
        HEARTBEAT(1),
        PING(2),
        ACK(3),
        PING_REQUEST(4),
        FAULT_START(5);

        private final byte code;

        Type(int code) {
            this.code = (byte) code;
        }

        private static Type fromCode(byte code) {
            for (Type type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("unknown UDP frame type: " + Byte.toUnsignedInt(code));
        }
    }
}
