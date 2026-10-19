package io.openkache.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.openkache.core.model.Bytes;

/**
 * byte stream을 {@link Frame}으로 나눈다.
 *
 * <p>{@link LengthFieldBasedFrameDecoder}가 분할 수신과 여러 frame의 동시 수신을 처리하고, 이 클래스는 고정 헤더를 검사한다.
 * 2 MiB를 넘는 frame은 {@code TooLongFrameException}, 헤더 오류는 {@link ProtocolException}으로 알린다. 채널마다 새
 * 인스턴스를 써야 한다.
 */
public final class FrameDecoder extends LengthFieldBasedFrameDecoder {

    public FrameDecoder() {
        super(Protocol.MAX_FRAME_BYTES, 0, Protocol.LENGTH_FIELD_BYTES, 0, Protocol.LENGTH_FIELD_BYTES);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf payload = (ByteBuf) super.decode(ctx, in);
        if (payload == null) {
            return null;
        }
        try {
            return parse(payload);
        } finally {
            payload.release();
        }
    }

    /** length 필드를 제외한 frame을 해석한다. */
    static Frame parse(ByteBuf payload) throws ProtocolException {
        int length = payload.readableBytes();
        if (length < Protocol.HEADER_BYTES) {
            throw new ProtocolException(
                    "length " + length + " is shorter than header " + Protocol.HEADER_BYTES, 0);
        }
        int start = payload.readerIndex();
        long requestId = payload.getLong(start + 8);

        int magic = payload.readUnsignedShort();
        if (magic != Protocol.MAGIC) {
            throw new ProtocolException("bad magic 0x" + Integer.toHexString(magic), requestId);
        }
        int version = payload.readUnsignedByte();
        if (version != Protocol.VERSION) {
            throw new ProtocolException("unsupported version " + version, requestId);
        }
        int messageClass = payload.readUnsignedByte();
        int opcode = payload.readUnsignedByte();
        int flags = payload.readUnsignedByte();
        int status = payload.readUnsignedShort();
        payload.skipBytes(8); // requestId는 위에서 읽었다.
        long routingVersion = payload.readLong();
        Bytes body = Bytes.copyOf(ByteBufUtil.getBytes(payload));

        return new Frame(messageClass, opcode, flags, status, requestId, routingVersion, body);
    }
}
