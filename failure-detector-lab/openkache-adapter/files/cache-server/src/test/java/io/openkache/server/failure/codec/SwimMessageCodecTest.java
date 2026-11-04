package io.openkache.server.failure.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openkache.core.failure.MemberStatus;
import io.openkache.core.failure.MembershipUpdate;
import io.openkache.core.failure.SwimMessage;
import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.server.netty.Protocol;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class SwimMessageCodecTest {

    private static final int OPCODE_OFFSET = Protocol.LENGTH_FIELD_BYTES + Short.BYTES + Byte.BYTES + Byte.BYTES;
    private static final int FLAGS_OFFSET = OPCODE_OFFSET + Byte.BYTES;
    private static final int REQUEST_STATUS_OFFSET = FLAGS_OFFSET + Byte.BYTES;
    private static final int REQUEST_ID_OFFSET = REQUEST_STATUS_OFFSET + Short.BYTES;
    private static final int ROUTING_VERSION_OFFSET = REQUEST_ID_OFFSET + Long.BYTES;
    private static final int PING_UPDATE_COUNT_OFFSET =
            Protocol.LENGTH_FIELD_BYTES + Protocol.HEADER_BYTES + Long.BYTES * 4;
    private static final int PING_FIRST_UPDATE_STATUS_OFFSET = PING_UPDATE_COUNT_OFFSET
            + Integer.BYTES
            + Long.BYTES
            + Long.BYTES;

    private static final NodeId NODE_1 = new NodeId(1);
    private static final NodeId NODE_2 = new NodeId(2);
    private static final NodeId NODE_3 = new NodeId(3);
    private static final List<MembershipUpdate> UPDATES = List.of(
            new MembershipUpdate(NODE_2, 4, MemberStatus.SUSPECT, NODE_1),
            new MembershipUpdate(NODE_3, 7, MemberStatus.DEAD, NODE_2));

    @Test
    void pingRoundTrips() {
        PeerEnvelope envelope =
                new PeerEnvelope(NODE_1, NODE_2, new SwimMessage.Ping(10, NODE_1, UPDATES));

        assertEquals(envelope, SwimMessageCodec.decode(SwimMessageCodec.encode(envelope)));
    }

    @Test
    void ackRoundTrips() {
        PeerEnvelope envelope =
                new PeerEnvelope(NODE_2, NODE_1, new SwimMessage.Ack(11, NODE_3, NODE_1, UPDATES));

        assertEquals(envelope, SwimMessageCodec.decode(SwimMessageCodec.encode(envelope)));
    }

    @Test
    void pingRequestRoundTrips() {
        PeerEnvelope envelope = new PeerEnvelope(
                NODE_1, NODE_2, new SwimMessage.PingRequest(12, NODE_1, NODE_3, UPDATES));

        assertEquals(envelope, SwimMessageCodec.decode(SwimMessageCodec.encode(envelope)));
    }

    @Test
    void rejectsUnsupportedOpcode() {
        byte[] malformed = pingDatagram();
        malformed[OPCODE_OFFSET] = 99;

        assertMalformed(malformed);
    }

    @Test
    void rejectsTruncatedBody() {
        byte[] encoded = SwimMessageCodec.encode(new PeerEnvelope(
                NODE_1,
                NODE_2,
                new SwimMessage.Ping(
                        13,
                        NODE_1,
                        List.of(new MembershipUpdate(NODE_2, 1, MemberStatus.ALIVE, NODE_1)))));
        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
        ByteBuffer.wrap(truncated).putInt(truncated.length - Protocol.LENGTH_FIELD_BYTES);

        assertMalformed(truncated);
    }

    @Test
    void rejectsUnknownMemberStatus() {
        PeerEnvelope envelope = new PeerEnvelope(
                NODE_1,
                NODE_2,
                new SwimMessage.Ping(
                        13,
                        NODE_1,
                        List.of(new MembershipUpdate(NODE_2, 1, MemberStatus.ALIVE, NODE_1))));
        byte[] malformed = SwimMessageCodec.encode(envelope);
        malformed[PING_FIRST_UPDATE_STATUS_OFFSET] = 99;

        assertMalformed(malformed);
    }

    @Test
    void rejectsExcessiveUpdateCountBeforeAllocating() {
        byte[] malformed = pingDatagram();
        ByteBuffer.wrap(malformed).putInt(PING_UPDATE_COUNT_OFFSET, SwimMessageCodec.MAX_UPDATES + 1);

        assertMalformed(malformed);
    }

    @Test
    void rejectsTrailingBytesAndProbeIdMismatch() {
        byte[] encoded = pingDatagram();
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        ByteBuffer.wrap(trailing).putInt(trailing.length - Protocol.LENGTH_FIELD_BYTES);
        byte[] mismatch = encoded.clone();
        ByteBuffer.wrap(mismatch).putLong(REQUEST_ID_OFFSET, 999);

        assertMalformed(trailing);
        assertMalformed(mismatch);
    }

    @Test
    void rejectsDatagramShorterThanProtocolHeader() {
        byte[] truncated = new byte[Protocol.LENGTH_FIELD_BYTES + Protocol.HEADER_BYTES - 1];

        assertMalformed(truncated);
    }

    @Test
    void rejectsLengthMismatchAndLengthShorterThanHeader() {
        byte[] encoded = pingDatagram();
        byte[] lengthMismatch = Arrays.copyOf(encoded, encoded.length + 1);
        byte[] shortDeclaredLength = new byte[Protocol.LENGTH_FIELD_BYTES + Protocol.HEADER_BYTES];
        ByteBuffer.wrap(shortDeclaredLength).putInt(Protocol.HEADER_BYTES - 1);

        assertMalformed(lengthMismatch);
        assertMalformed(shortDeclaredLength);
    }

    @Test
    void rejectsBadMagicAndUnsupportedVersion() {
        byte[] badMagic = pingDatagram();
        badMagic[Protocol.LENGTH_FIELD_BYTES] = 0;
        badMagic[Protocol.LENGTH_FIELD_BYTES + 1] = 0;
        byte[] badVersion = pingDatagram();
        badVersion[Protocol.LENGTH_FIELD_BYTES + Short.BYTES] = 2;

        assertMalformed(badMagic);
        assertMalformed(badVersion);
    }

    @Test
    void rejectsInvalidProtocolHeaderFields() {
        byte[] wrongClass = pingDatagram();
        wrongClass[Protocol.LENGTH_FIELD_BYTES + Short.BYTES + Byte.BYTES] = Protocol.CLASS_CLIENT_REQUEST;
        byte[] nonZeroFlags = pingDatagram();
        nonZeroFlags[FLAGS_OFFSET] = 1;
        byte[] nonZeroStatus = pingDatagram();
        ByteBuffer.wrap(nonZeroStatus).putShort(REQUEST_STATUS_OFFSET, (short) 1);
        byte[] nonZeroRoutingVersion = pingDatagram();
        ByteBuffer.wrap(nonZeroRoutingVersion).putLong(ROUTING_VERSION_OFFSET, 1);

        assertMalformed(wrongClass);
        assertMalformed(nonZeroFlags);
        assertMalformed(nonZeroStatus);
        assertMalformed(nonZeroRoutingVersion);
    }

    @Test
    void rejectsDatagramLargerThanUdpPayloadLimit() {
        byte[] oversized = new byte[SwimMessageCodec.MAX_DATAGRAM_BYTES + 1];

        assertMalformed(oversized);
    }

    private static byte[] pingDatagram() {
        return SwimMessageCodec.encode(
                new PeerEnvelope(NODE_1, NODE_2, new SwimMessage.Ping(15, NODE_1, List.of())));
    }

    private static void assertMalformed(byte[] datagram) {
        assertThrows(
                SwimMessageCodec.MalformedSwimMessageException.class,
                () -> SwimMessageCodec.decode(datagram));
    }
}
