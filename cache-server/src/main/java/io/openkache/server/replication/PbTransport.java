package io.openkache.server.replication;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.openkache.core.model.NodeId;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** 노드별 TCP 연결 재사용. EventLoop에서 block하지 않으며 send 성공은 write 완료일 뿐 복제 ACK가 아니다. */
public final class PbTransport implements Transport,AutoCloseable {
    private final EventLoopGroup group;
    private final java.util.function.Function<PeerEnvelope,Frame> encoder;
    private final Map<NodeId,InetSocketAddress> peers;
    private final ConcurrentMap<NodeId,CompletableFuture<Channel>> channels=new ConcurrentHashMap<>();
    private final AtomicBoolean closed=new AtomicBoolean();
    private final AtomicInteger pending=new AtomicInteger();
    private final AtomicLong sentBytes=new AtomicLong(),failed=new AtomicLong();
    private final PeerTraffic traffic=new PeerTraffic();
    /** 모든 주소는 고정 구성이다. 인증/TLS가 없으므로 전용 보안 그룹 안에서만 사용한다. */
    public PbTransport(EventLoopGroup group,Map<NodeId,InetSocketAddress> peers) { this(group,peers,PbWire::encode); }
    public PbTransport(EventLoopGroup group,Map<NodeId,InetSocketAddress> peers,java.util.function.Function<PeerEnvelope,Frame> encoder) { this.group=group; this.peers=Map.copyOf(peers); this.encoder=encoder; }
    @Override public CompletionStage<Void> send(PeerEnvelope envelope) {
        if(closed.get() || !peers.containsKey(envelope.to())) return CompletableFuture.failedFuture(new IllegalStateException("transport closed/unknown peer"));
        if(pending.incrementAndGet()>1024) { pending.decrementAndGet(); return CompletableFuture.failedFuture(new IllegalStateException("peer queue full")); }
        Frame frame;
        try { frame=encoder.apply(envelope); } catch(RuntimeException e) { pending.decrementAndGet(); return CompletableFuture.failedFuture(e); }
        CompletableFuture<Void> result=new CompletableFuture<>();
        result.whenComplete((v,e)->{ pending.decrementAndGet(); if(e!=null) failed.incrementAndGet(); });
        CompletableFuture<Channel> connection=channels.computeIfAbsent(envelope.to(),this::connect);
        connection.whenComplete((channel,error)->{
            if(error!=null) { channels.remove(envelope.to(),connection); result.completeExceptionally(error); return; }
            if(closed.get() || !channel.isActive()) { channels.remove(envelope.to(),connection); result.completeExceptionally(new IllegalStateException("peer disconnected")); return; }
            channel.writeAndFlush(frame).addListener(write->{
                if(write.isSuccess()) { long size=28L+frame.body().size(); traffic.record(frame.opcode(),size); sentBytes.addAndGet(size); result.complete(null); }
                else { channels.remove(envelope.to(),connection); channel.close(); result.completeExceptionally(write.cause()); }
            });
        });
        return result;
    }
    private CompletableFuture<Channel> connect(NodeId target) {
        CompletableFuture<Channel> future=new CompletableFuture<>();
        new Bootstrap().group(group).channel(NioSocketChannel.class).option(ChannelOption.CONNECT_TIMEOUT_MILLIS,500)
            .option(ChannelOption.TCP_NODELAY,true).handler(new ChannelInitializer<SocketChannel>() {
                @Override protected void initChannel(SocketChannel channel) {
                    channel.pipeline().addLast(new FrameDecoder(),new FrameEncoder(),new SimpleChannelInboundHandler<Frame>() {
                        @Override protected void channelRead0(ChannelHandlerContext ctx,Frame frame) {
                            if(frame.messageClass()!=Protocol.CLASS_RESPONSE || frame.status()!=0) ctx.close();
                        }
                        @Override public void channelInactive(ChannelHandlerContext ctx) { channels.remove(target,future); }
                        @Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable error) { ctx.close(); }
                    });
                }
            }).connect(peers.get(target)).addListener(connected->{
                if(connected.isSuccess()) { Channel channel=((ChannelFuture)connected).channel(); if(closed.get()) channel.close(); future.complete(channel); }
                else future.completeExceptionally(connected.cause());
            });
        return future;
    }
    /** 전송 완료한 frame byte 누적. 원격 적용 확인이나 OS/IP overhead는 포함하지 않는다. */
    public long sentBytes() { return sentBytes.get(); }
    /** 비동기 connect/write 실패의 누적 건수. */
    public long failures() { return failed.get(); }
    /** opcode별 송신 완료 frame/byte와 합계의 원자적 JSON 사본. 원격 수신 확인·TCP/IP overhead는 아니다. */
    public String trafficJson() { return traffic.json(); }
    @Override public void close() { closed.set(true); channels.values().forEach(f->f.thenAccept(Channel::close)); channels.clear(); }
}
