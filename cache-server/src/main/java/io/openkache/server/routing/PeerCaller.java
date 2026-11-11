package io.openkache.server.routing;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.server.netty.Frame;
import io.openkache.server.netty.FrameDecoder;
import io.openkache.server.netty.FrameEncoder;
import io.openkache.server.netty.Protocol;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 응답을 기다리는 peer 요청 client. 이동 전송(TRANSFER)과 commit(CONTROL)처럼 상대의 적용 확인이 필요한 요청에 쓴다.
 * 노드별 연결 하나를 재사용하고 requestId로 응답을 짝짓는다. 어느 thread에서나 호출할 수 있고 EventLoop에서 block하지 않는다.
 */
final class PeerCaller implements AutoCloseable {

    /** §3 응답 body를 푼 결과. status는 §5 상태 코드(0 = OK). */
    record Reply(int status, long routingVersion, String detail, byte[] value) {
        boolean ok() { return status == 0; }
    }

    private final EventLoopGroup group;
    private final Map<NodeId, InetSocketAddress> peers;
    private final long timeoutMs;
    private final ConcurrentMap<NodeId, CompletableFuture<Channel>> channels = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, CompletableFuture<Reply>> pending = new ConcurrentHashMap<>();
    private final AtomicLong nextId = new AtomicLong(1);
    private final AtomicBoolean closed = new AtomicBoolean();

    PeerCaller(EventLoopGroup group, Map<NodeId, InetSocketAddress> peers, long timeoutMs) {
        this.group = group;
        this.peers = Map.copyOf(peers);
        this.timeoutMs = timeoutMs;
    }

    boolean knows(NodeId node) {
        return peers.containsKey(node);
    }

    /** peer class frame을 보내고 응답을 기다린다. 연결 실패·시간 초과는 예외로 완료된다. */
    CompletableFuture<Reply> call(NodeId target, int opcode, byte[] body) {
        if (closed.get() || !peers.containsKey(target)) {
            return CompletableFuture.failedFuture(new IllegalStateException("closed or unknown peer " + target));
        }
        long id = nextId.getAndIncrement();
        CompletableFuture<Reply> reply = new CompletableFuture<>();
        pending.put(id, reply);
        reply.orTimeout(timeoutMs, TimeUnit.MILLISECONDS).whenComplete((r, e) -> pending.remove(id));
        Frame frame = new Frame(Protocol.CLASS_PEER, opcode, 0, 0, id, 0, Bytes.copyOf(body));
        CompletableFuture<Channel> connection = channels.computeIfAbsent(target, this::connect);
        connection.whenComplete((channel, error) -> {
            if (error != null || !channel.isActive()) {
                channels.remove(target, connection);
                reply.completeExceptionally(error != null ? error : new IllegalStateException("peer disconnected"));
                return;
            }
            channel.writeAndFlush(frame).addListener(write -> {
                if (!write.isSuccess()) {
                    channels.remove(target, connection);
                    channel.close();
                    reply.completeExceptionally(write.cause());
                }
            });
        });
        return reply;
    }

    private CompletableFuture<Channel> connect(NodeId target) {
        CompletableFuture<Channel> future = new CompletableFuture<>();
        new Bootstrap().group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new FrameDecoder(), new FrameEncoder(), new SimpleChannelInboundHandler<Frame>() {
                            @Override protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
                                CompletableFuture<Reply> r = pending.remove(frame.requestId());
                                if (r != null) r.complete(decode(frame));
                            }
                            @Override public void channelInactive(ChannelHandlerContext ctx) {
                                channels.remove(target, future);
                            }
                            @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
                                ctx.close();
                            }
                        });
                    }
                })
                .connect(peers.get(target)).addListener(connected -> {
                    if (connected.isSuccess()) future.complete(((ChannelFuture) connected).channel());
                    else future.completeExceptionally(connected.cause());
                });
        return future;
    }

    private static Reply decode(Frame frame) {
        ByteBuffer b = ByteBuffer.wrap(frame.body().toByteArray());
        String detail = "";
        byte[] value = new byte[0];
        if (b.remaining() >= 20) {
            b.getLong();
            b.getLong();
            byte[] d = new byte[b.getInt()];
            b.get(d);
            detail = new String(d, StandardCharsets.UTF_8);
            value = new byte[b.getInt()];
            b.get(value);
        }
        return new Reply(frame.status(), frame.routingVersion(), detail, value);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        channels.values().forEach(f -> f.thenAccept(Channel::close));
        channels.clear();
        pending.values().forEach(r -> r.completeExceptionally(new IllegalStateException("closed")));
    }
}
