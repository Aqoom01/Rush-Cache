package io.openkache.server.failure.netty;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.failure.SwimMessage;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.server.failure.SwimRuntime;
import io.openkache.server.failure.codec.SwimMessageCodec;
import io.openkache.server.metrics.ServerMetrics;
import io.openkache.server.netty.Protocol;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** UDP datagram을 SWIM 메시지로 변환해 전용 owner mailbox로 전달한다. */
public final class SwimDatagramHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private static final Logger LOG = LoggerFactory.getLogger(SwimDatagramHandler.class);
    private static final String LISTENER = "swim-udp";

    private final SwimRuntime runtime;
    private final ServerMetrics metrics;

    public SwimDatagramHandler(SwimRuntime runtime, ServerMetrics metrics) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
        PeerEnvelope envelope;
        try {
            envelope = SwimMessageCodec.decode(ByteBufUtil.getBytes(packet.content()));
        } catch (SwimMessageCodec.MalformedSwimMessageException e) {
            reject(packet, "unknown", ResultStatus.BAD_REQUEST, e.getMessage());
            return;
        }

        SwimMessage message = (SwimMessage) envelope.message();
        String opcode = Protocol.opcodeName(SwimMessageCodec.opcodeOf(message));
        if (!envelope.to().equals(runtime.localNode())) {
            reject(packet, opcode, ResultStatus.BAD_REQUEST, "SWIM message is addressed to another node");
        } else if (!runtime.isMember(envelope.from())) {
            reject(
                    packet,
                    opcode,
                    ResultStatus.BAD_REQUEST,
                    "unknown SWIM sender " + envelope.from().value());
        } else if (!runtime.onMessage(envelope.from(), message)) {
            reject(packet, opcode, ResultStatus.BUSY, "SWIM owner mailbox is unavailable");
        } else {
            metrics.recordRequest(LISTENER, opcode, ResultStatus.OK);
        }
    }

    private void reject(DatagramPacket packet, String opcode, ResultStatus status, String detail) {
        metrics.recordRequest(LISTENER, opcode, status);
        LOG.debug("rejected SWIM datagram from {}: {}", packet.sender(), detail);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("SWIM UDP listener failed to process a datagram", cause);
    }
}
