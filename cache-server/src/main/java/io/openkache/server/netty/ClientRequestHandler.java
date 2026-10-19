package io.openkache.server.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.openkache.core.api.CacheEngine;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.metrics.ServerMetrics;
import java.time.Duration;
import java.util.Objects;

/**
 * client listener의 요청 처리.
 *
 * <ul>
 *   <li>client request class만 받는다. peer class 등은 {@code BAD_REQUEST}로 거절한다.
 *   <li>PING: 연결 확인용으로 즉시 OK를 응답한다. SWIM 장애 감지가 아니다.
 *   <li>GET·SET·DELETE: {@link CacheEngine#execute}로 넘긴다. body가 잘못되면 연결을 유지한 채 {@code BAD_REQUEST}로
 *       응답한다. {@code timeoutMs}가 0이면 {@code defaultTimeout}을 쓴다.
 * </ul>
 *
 * <p>engine을 포함해 상태가 없어(engine 자체가 thread-safe) 채널 간 공유한다.
 */
@Sharable
public final class ClientRequestHandler extends AbstractFrameHandler {

    private static final TimeSource TIME = TimeSource.system();

    private final CacheEngine engine;
    private final Duration defaultTimeout;

    public ClientRequestHandler(ServerMetrics metrics, CacheEngine engine, Duration defaultTimeout) {
        super("client", metrics);
        this.engine = Objects.requireNonNull(engine, "engine");
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "defaultTimeout");
    }

    @Override
    void handle(ChannelHandlerContext ctx, Frame frame) {
        if (frame.messageClass() != Protocol.CLASS_CLIENT_REQUEST) {
            respond(ctx, frame, ResultStatus.BAD_REQUEST,
                    "message class " + frame.messageClass() + " is not accepted on the client listener");
            return;
        }
        switch (frame.opcode()) {
            case Protocol.OP_PING -> {
                if (frame.body().isEmpty()) {
                    respond(ctx, frame, ResultStatus.OK, "");
                } else {
                    respond(ctx, frame, ResultStatus.BAD_REQUEST, "PING body must be empty");
                }
            }
            case Protocol.OP_GET, Protocol.OP_SET, Protocol.OP_DELETE -> handleCommand(ctx, frame);
            default -> respond(ctx, frame, ResultStatus.BAD_REQUEST, "unknown opcode " + frame.opcode());
        }
    }

    private void handleCommand(ChannelHandlerContext ctx, Frame frame) {
        CommandCodec.DecodedRequest decoded;
        try {
            decoded = CommandCodec.decode(frame.opcode(), frame.body());
        } catch (CommandCodec.MalformedBodyException e) {
            respond(ctx, frame, ResultStatus.BAD_REQUEST, e.getMessage());
            return;
        }

        Duration timeout = decoded.timeoutMs() > 0 ? Duration.ofMillis(decoded.timeoutMs()) : defaultTimeout;
        RequestContext context =
                new RequestContext(
                        decoded.clientId(),
                        decoded.operationId(),
                        new RoutingVersion(frame.routingVersion()),
                        decoded.ownershipEpoch(),
                        Deadline.after(TIME, timeout));

        // engine.execute는 block하지 않는다. 완료된 thread(owner worker 등)와 무관하게 Netty Channel에 쓰는 것은
        // 안전하므로 EventLoop로 되돌아갈 필요가 없다.
        engine.execute(decoded.command(), context)
                .whenComplete(
                        (result, error) -> {
                            if (error != null) {
                                respond(ctx, frame, ResultStatus.INTERNAL_ERROR, "engine failed: " + error);
                            } else {
                                respond(ctx, frame, result);
                            }
                        });
    }
}
