package io.openkache.server.failure.codec;

import io.openkache.core.failure.MemberStatus;
import io.openkache.core.failure.MembershipUpdate;
import io.openkache.core.failure.SwimMessage;
import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.server.netty.Protocol;
import java.io.Serial;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** {@link PeerEnvelope}에 담긴 SWIM 메시지와 UDP datagram payload를 서로 변환한다. */
public final class SwimMessageCodec {

    static final int MAX_UPDATES = 1_024;
    static final int MAX_DATAGRAM_BYTES = 65_507;
    private static final int ENVELOPE_BYTES = Long.BYTES * 2;
    private static final int UPDATE_BYTES = Long.BYTES * 3 + Byte.BYTES;

    private SwimMessageCodec() {}

    /** 길이, protocol header 또는 SWIM body가 유효하지 않은 UDP payload. */
    public static final class MalformedSwimMessageException extends RuntimeException {
        @Serial private static final long serialVersionUID = 1L;

        MalformedSwimMessageException(String message) {
            super(message);
        }
    }

    /** SWIM envelope을 protocol v1 frame 하나가 담긴 UDP payload로 인코딩한다. */
    public static byte[] encode(PeerEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope");
        if (!(envelope.message() instanceof SwimMessage message)) {
            throw new IllegalArgumentException("not a SWIM message: " + envelope.message().getClass().getName());
        }
        if (message.updates().size() > MAX_UPDATES) {
            throw new IllegalArgumentException("too many membership updates: " + message.updates().size());
        }

        int specificBytes = specificBytes(message);
        int bodyBytes = Math.addExact(
                ENVELOPE_BYTES + Long.BYTES + specificBytes + Integer.BYTES,
                Math.multiplyExact(message.updates().size(), UPDATE_BYTES));
        int frameBytes = Math.addExact(Protocol.HEADER_BYTES, bodyBytes);
        int datagramBytes = Math.addExact(Protocol.LENGTH_FIELD_BYTES, frameBytes);
        if (datagramBytes > MAX_DATAGRAM_BYTES) {
            throw new IllegalArgumentException("SWIM datagram too large: " + datagramBytes + " bytes");
        }

        ByteBuffer payload = ByteBuffer.allocate(datagramBytes);
        writeHeader(payload, frameBytes, opcodeOf(message), message.probeId());
        payload.putLong(envelope.from().value());
        payload.putLong(envelope.to().value());
        payload.putLong(message.probeId());
        switch (message) {
            case SwimMessage.Ping ping -> payload.putLong(ping.origin().value());
            case SwimMessage.Ack ack -> {
                payload.putLong(ack.target().value());
                payload.putLong(ack.origin().value());
            }
            case SwimMessage.PingRequest request -> {
                payload.putLong(request.origin().value());
                payload.putLong(request.target().value());
            }
        }
        writeUpdates(payload, message.updates());
        return payload.array();
    }

    /** UDP payload 하나에서 SWIM envelope 하나를 디코딩한다. */
    public static PeerEnvelope decode(byte[] datagram) {
        Objects.requireNonNull(datagram, "datagram");
        validateDatagramSize(datagram);
        try {
            ByteBuffer payload = ByteBuffer.wrap(datagram);
            int declaredLength = payload.getInt();
            if (declaredLength < Protocol.HEADER_BYTES) {
                throw malformed("length " + declaredLength + " is shorter than header " + Protocol.HEADER_BYTES);
            }
            if (declaredLength != payload.remaining()) {
                throw malformed(
                        "declared length " + declaredLength + " differs from datagram payload " + payload.remaining());
            }

            validateMagicAndVersion(payload);
            int messageClass = Byte.toUnsignedInt(payload.get());
            int opcode = Byte.toUnsignedInt(payload.get());
            int flags = Byte.toUnsignedInt(payload.get());
            int requestStatus = Short.toUnsignedInt(payload.getShort());
            long requestId = payload.getLong();
            long routingVersion = payload.getLong();
            validateHeader(messageClass, flags, requestStatus, routingVersion);

            NodeId from = new NodeId(payload.getLong());
            NodeId to = new NodeId(payload.getLong());
            long probeId = payload.getLong();
            SwimMessage message = readMessage(payload, opcode, probeId);
            if (payload.hasRemaining()) {
                throw malformed("trailing bytes after SWIM body: " + payload.remaining());
            }
            if (requestId != probeId) {
                throw malformed("frame requestId " + requestId + " differs from probeId " + probeId);
            }
            return new PeerEnvelope(from, to, message);
        } catch (MalformedSwimMessageException e) {
            throw e;
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw malformed("malformed SWIM datagram: " + e.getMessage());
        }
    }

    /** metric label에 사용할 protocol opcode를 SWIM 메시지 타입에서 구한다. */
    public static int opcodeOf(SwimMessage message) {
        Objects.requireNonNull(message, "message");
        return switch (message) {
            case SwimMessage.Ping ignored -> Protocol.OP_SWIM_PING;
            case SwimMessage.Ack ignored -> Protocol.OP_SWIM_ACK;
            case SwimMessage.PingRequest ignored -> Protocol.OP_SWIM_PING_REQ;
        };
    }

    private static void writeHeader(ByteBuffer payload, int frameBytes, int opcode, long probeId) {
        payload.putInt(frameBytes);
        payload.putShort((short) Protocol.MAGIC);
        payload.put((byte) Protocol.VERSION);
        payload.put((byte) Protocol.CLASS_PEER);
        payload.put((byte) opcode);
        payload.put((byte) 0);
        payload.putShort((short) 0);
        payload.putLong(probeId);
        payload.putLong(0);
    }

    private static void validateDatagramSize(byte[] datagram) {
        if (datagram.length > MAX_DATAGRAM_BYTES) {
            throw malformed("SWIM datagram too large: " + datagram.length + " bytes");
        }
        int minimumBytes = Protocol.LENGTH_FIELD_BYTES + Protocol.HEADER_BYTES;
        if (datagram.length < minimumBytes) {
            throw malformed("SWIM datagram is shorter than frame header: " + datagram.length + " bytes");
        }
    }

    private static void validateMagicAndVersion(ByteBuffer payload) {
        int magic = Short.toUnsignedInt(payload.getShort());
        if (magic != Protocol.MAGIC) {
            throw malformed("bad magic 0x" + Integer.toHexString(magic));
        }
        int version = Byte.toUnsignedInt(payload.get());
        if (version != Protocol.VERSION) {
            throw malformed("unsupported version " + version);
        }
    }

    private static void validateHeader(int messageClass, int flags, int requestStatus, long routingVersion) {
        if (messageClass != Protocol.CLASS_PEER) {
            throw malformed("SWIM message must use peer class");
        }
        if (flags != 0) {
            throw malformed("flags must be 0 in protocol v1");
        }
        if (requestStatus != 0) {
            throw malformed("request status must be 0");
        }
        if (routingVersion != 0) {
            throw malformed("SWIM routingVersion must be 0");
        }
    }

    private static int specificBytes(SwimMessage message) {
        return switch (message) {
            case SwimMessage.Ping ignored -> Long.BYTES;
            case SwimMessage.Ack ignored -> Long.BYTES * 2;
            case SwimMessage.PingRequest ignored -> Long.BYTES * 2;
        };
    }

    private static SwimMessage readMessage(ByteBuffer payload, int opcode, long probeId) {
        return switch (opcode) {
            case Protocol.OP_SWIM_PING -> {
                NodeId origin = new NodeId(payload.getLong());
                yield new SwimMessage.Ping(probeId, origin, readUpdates(payload));
            }
            case Protocol.OP_SWIM_ACK -> {
                NodeId target = new NodeId(payload.getLong());
                NodeId origin = new NodeId(payload.getLong());
                yield new SwimMessage.Ack(probeId, target, origin, readUpdates(payload));
            }
            case Protocol.OP_SWIM_PING_REQ -> {
                NodeId origin = new NodeId(payload.getLong());
                NodeId target = new NodeId(payload.getLong());
                yield new SwimMessage.PingRequest(probeId, origin, target, readUpdates(payload));
            }
            default -> throw malformed("unsupported SWIM opcode: " + opcode);
        };
    }

    private static void writeUpdates(ByteBuffer body, List<MembershipUpdate> updates) {
        body.putInt(updates.size());
        for (MembershipUpdate update : updates) {
            body.putLong(update.member().value());
            body.putLong(update.incarnation());
            body.put(statusCode(update.status()));
            body.putLong(update.reporter().value());
        }
    }

    private static List<MembershipUpdate> readUpdates(ByteBuffer body) {
        int count = body.getInt();
        if (count < 0 || count > MAX_UPDATES) {
            throw new MalformedSwimMessageException("invalid membership update count: " + count);
        }
        if (body.remaining() < count * UPDATE_BYTES) {
            throw new MalformedSwimMessageException(
                    "membership update count " + count + " exceeds remaining body " + body.remaining());
        }
        List<MembershipUpdate> updates = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            updates.add(new MembershipUpdate(
                    new NodeId(body.getLong()),
                    body.getLong(),
                    status(body.get()),
                    new NodeId(body.getLong())));
        }
        return List.copyOf(updates);
    }

    private static byte statusCode(MemberStatus status) {
        return switch (status) {
            case ALIVE -> 0;
            case SUSPECT -> 1;
            case DEAD -> 2;
        };
    }

    private static MemberStatus status(byte code) {
        return switch (code) {
            case 0 -> MemberStatus.ALIVE;
            case 1 -> MemberStatus.SUSPECT;
            case 2 -> MemberStatus.DEAD;
            default -> throw malformed("unknown member status: " + Byte.toUnsignedInt(code));
        };
    }

    private static MalformedSwimMessageException malformed(String message) {
        return new MalformedSwimMessageException(message);
    }
}
