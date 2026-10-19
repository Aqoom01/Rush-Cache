package io.openkache.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/** {@link Frame}을 length 필드를 포함한 byte로 쓴다. 상태가 없어 채널 간 공유할 수 있다. */
@Sharable
public final class FrameEncoder extends MessageToByteEncoder<Frame> {

    public FrameEncoder() {
        super(Frame.class);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) {
        out.writeInt(Protocol.HEADER_BYTES + frame.body().size());
        out.writeShort(Protocol.MAGIC);
        out.writeByte(Protocol.VERSION);
        out.writeByte(frame.messageClass());
        out.writeByte(frame.opcode());
        out.writeByte(frame.flags());
        out.writeShort(frame.status());
        out.writeLong(frame.requestId());
        out.writeLong(frame.routingVersion());
        out.writeBytes(frame.body().toByteArray());
    }
}
