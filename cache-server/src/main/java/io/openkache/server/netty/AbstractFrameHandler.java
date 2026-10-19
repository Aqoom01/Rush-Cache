package io.openkache.server.netty;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.server.metrics.ServerMetrics;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * client·peer listener 공통 처리: 응답 전송, 요청 지표 기록, frame 오류 시 연결 종료.
 *
 * <p>Netty EventLoop에서 실행되므로 block하지 않는다.
 */
abstract class AbstractFrameHandler extends SimpleChannelInboundHandler<Frame> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFrameHandler.class);

    private final String listener;
    private final ServerMetrics metrics;

    AbstractFrameHandler(String listener, ServerMetrics metrics) {
        super(Frame.class);
        this.listener = listener;
        this.metrics = metrics;
    }

    /** 상태 코드 검사를 통과한 frame을 처리하고 응답을 보낸다. */
    abstract void handle(ChannelHandlerContext ctx, Frame frame);

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        if (frame.flags() != 0) {
            respond(ctx, frame, ResultStatus.BAD_REQUEST, "flags must be 0 in protocol v1");
        } else if (frame.status() != 0) {
            respond(ctx, frame, ResultStatus.BAD_REQUEST, "request status must be 0");
        } else {
            handle(ctx, frame);
        }
    }

    final void respond(ChannelHandlerContext ctx, Frame request, ResultStatus status, String detail) {
        metrics.recordRequest(listener, Protocol.opcodeName(request.opcode()), status);
        ctx.writeAndFlush(Frame.response(request.requestId(), request.opcode(), status, detail));
    }

    /** 실제 {@link Result}로 응답한다 (GET/SET/DELETE 등 엔진이 처리한 요청). */
    final void respond(ChannelHandlerContext ctx, Frame request, Result result) {
        metrics.recordRequest(listener, Protocol.opcodeName(request.opcode()), result.status());
        ctx.writeAndFlush(Frame.response(request.requestId(), request.opcode(), result, detailFor(result.status())));
    }

    private static String detailFor(ResultStatus status) {
        return switch (status) {
            case BUSY -> "worker mailbox is full";
            case TIMEOUT -> "deadline exceeded before completion";
            case UNAVAILABLE -> "engine is shutting down";
            case INTERNAL_ERROR -> "internal error";
            default -> "";
        };
    }

    @Override
    public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Throwable error = cause instanceof DecoderException && cause.getCause() != null ? cause.getCause() : cause;

        if (error instanceof ProtocolException protocolError) {
            rejectAndClose(ctx, protocolError.requestId(), protocolError.getMessage());
        } else if (error instanceof TooLongFrameException || error instanceof CorruptedFrameException) {
            rejectAndClose(ctx, 0, error.getMessage());
        } else if (error instanceof IOException) {
            LOG.debug("{} connection {} closed: {}", listener, ctx.channel().remoteAddress(), error.toString());
            ctx.close();
        } else {
            LOG.error("{} connection {} failed", listener, ctx.channel().remoteAddress(), error);
            ctx.close();
        }
    }

    // 헤더가 깨지면 이후 byte stream의 frame 경계를 믿을 수 없으므로 오류를 알리고 연결을 닫는다.
    private void rejectAndClose(ChannelHandlerContext ctx, long requestId, String reason) {
        LOG.warn("{} connection {} rejected: {}", listener, ctx.channel().remoteAddress(), reason);
        metrics.recordRequest(listener, "unknown", ResultStatus.BAD_REQUEST);
        ctx.writeAndFlush(Frame.response(requestId, 0, ResultStatus.BAD_REQUEST, reason))
                .addListener(ChannelFutureListener.CLOSE);
    }
}
