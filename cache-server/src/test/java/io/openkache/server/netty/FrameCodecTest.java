package io.openkache.server.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.openkache.core.api.Redirect;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class FrameCodecTest {

    /** 서버로 들어오는 PING 요청 byte (28 bytes). */
    private static ByteBuf pingBytes(long requestId) {
        return header(Protocol.HEADER_BYTES, Protocol.MAGIC, Protocol.VERSION, requestId);
    }

    private static ByteBuf header(int length, int magic, int version, long requestId) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(length);
        buf.writeShort(magic);
        buf.writeByte(version);
        buf.writeByte(Protocol.CLASS_CLIENT_REQUEST);
        buf.writeByte(Protocol.OP_PING);
        buf.writeByte(0); // flags
        buf.writeShort(0); // status
        buf.writeLong(requestId);
        buf.writeLong(0); // routingVersion
        return buf;
    }

    @Test
    void decodesSingleFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());

        channel.writeInbound(pingBytes(42));

        Frame frame = channel.readInbound();
        assertEquals(Frame.request(Protocol.OP_PING, 42), frame);
        channel.finishAndReleaseAll();
    }

    @Test
    void handlesFrameSplitIntoSingleBytes() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        ByteBuf whole = pingBytes(7);

        while (whole.isReadable()) {
            channel.writeInbound(whole.readRetainedSlice(1));
            if (whole.isReadable()) {
                assertNull(channel.readInbound(), "frame must not be emitted before all bytes arrive");
            }
        }
        whole.release();

        assertEquals(7, ((Frame) channel.readInbound()).requestId());
        channel.finishAndReleaseAll();
    }

    @Test
    void handlesMultipleFramesInOneRead() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        ByteBuf combined = Unpooled.wrappedBuffer(pingBytes(1), pingBytes(2), pingBytes(3));

        channel.writeInbound(combined);

        assertEquals(1, ((Frame) channel.readInbound()).requestId());
        assertEquals(2, ((Frame) channel.readInbound()).requestId());
        assertEquals(3, ((Frame) channel.readInbound()).requestId());
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void rejectsBadMagic() {
        ProtocolException error = decodeError(header(Protocol.HEADER_BYTES, 0x1234, Protocol.VERSION, 9));

        assertEquals(9, error.requestId());
    }

    @Test
    void rejectsUnsupportedVersion() {
        decodeError(header(Protocol.HEADER_BYTES, Protocol.MAGIC, 2, 9));
    }

    @Test
    void rejectsLengthShorterThanHeader() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(23);
        buf.writeZero(23);

        decodeError(buf);
    }

    @Test
    void rejectsFrameLargerThanTwoMebibytes() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(Protocol.MAX_FRAME_BYTES - Protocol.LENGTH_FIELD_BYTES + 1);

        assertThrows(TooLongFrameException.class, () -> channel.writeInbound(buf));
        channel.finishAndReleaseAll();
    }

    @Test
    void encodedResponseHasDocumentedLayout() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameEncoder());

        channel.writeOutbound(Frame.response(42, Protocol.OP_PING, ResultStatus.OK, ""));
        ByteBuf out = channel.readOutbound();

        assertEquals(52, out.readableBytes()); // length 4 + header 24 + body 24
        assertEquals(48, out.readInt()); // length
        assertEquals(Protocol.MAGIC, out.readUnsignedShort());
        assertEquals(Protocol.VERSION, out.readUnsignedByte());
        assertEquals(Protocol.CLASS_RESPONSE, out.readUnsignedByte());
        assertEquals(Protocol.OP_PING, out.readUnsignedByte());
        assertEquals(0, out.readUnsignedByte()); // flags
        assertEquals(0, out.readUnsignedShort()); // status OK
        assertEquals(42, out.readLong()); // requestId
        assertEquals(0, out.readLong()); // routingVersion
        assertEquals(0, out.readLong()); // ownershipEpoch
        assertEquals(0, out.readLong()); // valueVersion
        assertEquals(0, out.readInt()); // detailLength
        assertEquals(0, out.readInt()); // valueLength
        out.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void engineResultResponseCarriesValueAndVersions() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameEncoder());
        Result result =
                Result.ok(Bytes.utf8("stored-value"), 3, new RoutingVersion(7), new OwnershipEpoch(9));

        channel.writeOutbound(Frame.response(42, Protocol.OP_GET, result, ""));
        ByteBuf out = channel.readOutbound();

        out.readInt(); // length
        out.readUnsignedShort(); // magic
        out.readUnsignedByte(); // version
        assertEquals(Protocol.CLASS_RESPONSE, out.readUnsignedByte());
        assertEquals(Protocol.OP_GET, out.readUnsignedByte());
        out.readUnsignedByte(); // flags
        assertEquals(0, out.readUnsignedShort()); // status OK
        assertEquals(42, out.readLong()); // requestId
        assertEquals(7, out.readLong()); // routingVersion (header)
        assertEquals(9, out.readLong()); // ownershipEpoch
        assertEquals(3, out.readLong()); // valueVersion
        assertEquals(0, out.readInt()); // detailLength
        byte[] expectedValue = "stored-value".getBytes(StandardCharsets.UTF_8);
        assertEquals(expectedValue.length, out.readInt());
        byte[] value = new byte[expectedValue.length];
        out.readBytes(value);
        assertArrayEquals(expectedValue, value);
        out.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void movedResultRedirectUsesTaggedDetail() {
        Result moved =
                Result.redirect(
                        ResultStatus.MOVED,
                        new Redirect(new NodeId(1)),
                        RoutingVersion.ZERO,
                        OwnershipEpoch.ZERO);

        Frame frame=Frame.response(1, Protocol.OP_GET, moved, "");
        java.nio.ByteBuffer body=java.nio.ByteBuffer.wrap(frame.body().toByteArray());
        body.getLong(); body.getLong(); byte[] detail=new byte[body.getInt()]; body.get(detail);
        assertEquals("redirect-node=1",new String(detail,StandardCharsets.UTF_8));
    }

    @Test
    void encoderAndDecoderRoundTrip() {
        Frame original = new Frame(Protocol.CLASS_PEER, 9, 0, 0, 123, 5, Bytes.utf8("body"));
        EmbeddedChannel encoder = new EmbeddedChannel(new FrameEncoder());
        EmbeddedChannel decoder = new EmbeddedChannel(new FrameDecoder());

        encoder.writeOutbound(original);
        decoder.writeInbound((ByteBuf) encoder.readOutbound());

        assertEquals(original, decoder.readInbound());
        encoder.finishAndReleaseAll();
        decoder.finishAndReleaseAll();
    }

    private static ProtocolException decodeError(ByteBuf input) {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        DecoderException error = assertThrows(DecoderException.class, () -> channel.writeInbound(input));
        channel.finishAndReleaseAll();
        return assertInstanceOf(ProtocolException.class, error.getCause());
    }
}
