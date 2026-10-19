package io.openkache.server.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.openkache.core.api.ResultStatus;
import io.openkache.server.metrics.ServerMetrics;

/**
 * peer listener의 메시지 처리.
 *
 * <p><b>복제·장애 감지 등 연구별 peer 프로토콜은 아직 구현되지 않았다.</b> 지금은 peer class PING에만 OK로 응답하고, 그 밖의
 * peer 메시지는 {@code NOT_IMPLEMENTED}로 응답한다. 연구 메시지는 이 handler에서 해석해 {@code CacheEngine.onPeerMessage}로
 * 넘기도록 확장한다.
 */
@Sharable
public final class PeerMessageHandler extends AbstractFrameHandler {

    public PeerMessageHandler(ServerMetrics metrics) {
        super("peer", metrics);
    }

    @Override
    void handle(ChannelHandlerContext ctx, Frame frame) {
        if (frame.messageClass() != Protocol.CLASS_PEER) {
            respond(ctx, frame, ResultStatus.BAD_REQUEST,
                    "message class " + frame.messageClass() + " is not accepted on the peer listener");
        } else if (frame.opcode() == Protocol.OP_PING && frame.body().isEmpty()) {
            respond(ctx, frame, ResultStatus.OK, "");
        } else {
            respond(ctx, frame, ResultStatus.NOT_IMPLEMENTED, "peer protocol is not implemented yet");
        }
    }
}
