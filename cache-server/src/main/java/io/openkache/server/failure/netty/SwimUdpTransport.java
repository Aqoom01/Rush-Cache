package io.openkache.server.failure.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.Transport;
import io.openkache.server.failure.codec.SwimMessageCodec;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** 하나의 Netty UDP channel로 SWIM datagram을 비동기 송수신한다. */
public final class SwimUdpTransport implements Transport, AutoCloseable {

    private final NodeId localNode;
    private final Map<NodeId, InetSocketAddress> addresses;
    private final EventLoopGroup ioGroup;
    private final AtomicBoolean bound = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong sentFrames = new AtomicLong();
    private final AtomicLong sentBytes = new AtomicLong();
    private final AtomicLong receivedFrames = new AtomicLong();
    private final AtomicLong receivedBytes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong invalidFrames = new AtomicLong();
    private volatile Channel channel;

    public SwimUdpTransport(NodeId localNode, Map<NodeId, InetSocketAddress> addresses, EventLoopGroup ioGroup) {
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.addresses = Map.copyOf(addresses);
        if (this.addresses.values().stream().anyMatch(InetSocketAddress::isUnresolved)) {
            throw new IllegalArgumentException("unresolved SWIM peer address");
        }
        this.ioGroup = Objects.requireNonNull(ioGroup, "ioGroup");
    }

    /** SWIM UDP listener를 연다. DNS 조회는 생성 시 서버 시작 thread에서 끝낸다. */
    public void bind(String host, int port, ChannelHandler inboundHandler) throws InterruptedException {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(inboundHandler, "inboundHandler");
        if (closed.get()) {
            throw new IllegalStateException("SWIM UDP transport is closed");
        }
        if (!bound.compareAndSet(false, true)) {
            return;
        }

        ChannelFuture bindFuture = new Bootstrap()
                .group(ioGroup)
                .channel(NioDatagramChannel.class)
                .handler(inboundHandler)
                .bind(new InetSocketAddress(host, port));
        try {
            channel = bindFuture.sync().channel();
        } catch (InterruptedException | RuntimeException e) {
            bound.set(false);
            bindFuture.channel().close();
            throw e;
        }
    }

    @Override
    public CompletionStage<Void> send(PeerEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope");
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("SWIM UDP transport is closed"));
        }
        Channel activeChannel = channel;
        if (activeChannel == null || !activeChannel.isActive()) {
            return CompletableFuture.failedFuture(new IllegalStateException("SWIM UDP transport is not bound"));
        }
        if (!localNode.equals(envelope.from())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("envelope sender must be local node " + localNode.value()));
        }
        InetSocketAddress address = addresses.get(envelope.to());
        if (address == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("unknown SWIM peer node " + envelope.to().value()));
        }

        byte[] payload;
        try {
            payload = SwimMessageCodec.encode(envelope);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Void> completion = new CompletableFuture<>();
        DatagramPacket packet = new DatagramPacket(Unpooled.wrappedBuffer(payload), address);
        activeChannel.writeAndFlush(packet).addListener(write -> {
            if (write.isSuccess()) {
                sentFrames.incrementAndGet();
                sentBytes.addAndGet(payload.length);
                completion.complete(null);
            } else {
                failures.incrementAndGet();
                completion.completeExceptionally(write.cause());
            }
        });
        return completion;
    }

    public void recordReceived(int bytes) {
        receivedFrames.incrementAndGet();
        receivedBytes.addAndGet(bytes);
    }

    public void recordInvalid() { invalidFrames.incrementAndGet(); }
    public long sentFrames() { return sentFrames.get(); }
    public long sentBytes() { return sentBytes.get(); }
    public long receivedFrames() { return receivedFrames.get(); }
    public long receivedBytes() { return receivedBytes.get(); }
    public long failures() { return failures.get(); }
    public long invalidFrames() { return invalidFrames.get(); }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Channel activeChannel = channel;
        channel = null;
        if (activeChannel != null) {
            activeChannel.close();
        }
    }

}
