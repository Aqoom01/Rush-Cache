package io.openkache.server.lifecycle;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.openkache.core.api.CacheEngine;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.WorkerId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.config.ServerConfig;
import io.openkache.server.metrics.ServerMetrics;
import io.openkache.server.netty.ClientRequestHandler;
import io.openkache.server.netty.FrameDecoder;
import io.openkache.server.netty.FrameEncoder;
import io.openkache.server.netty.PeerMessageHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 서버 구성 요소의 기동과 종료 순서를 관리한다.
 *
 * <p>기동: metrics endpoint → core worker → {@link KvEngine} → Netty EventLoop → peer listener → client listener.
 * 종료는 listener → EventLoop → engine → worker → metrics 순서이며 {@link #close()}는 여러 번 호출해도 안전하다. 기동 중
 * 실패하면 이미 시작한 구성 요소를 닫고 예외를 던진다.
 *
 * <p>core worker는 엔진 요청을 처리할 owner thread로, Netty EventLoop와 별개다. 지금은 단일 노드 인메모리 KV만
 * 연결되어 있다. 복제·Tablet·peer 프로토콜은 아직 구현되지 않았다.
 */
public final class ServerApplication implements AutoCloseable {

    /** worker 하나의 mailbox 크기. 가득 차면 요청은 BUSY로 거절한다. */
    static final int WORKER_MAILBOX_CAPACITY = 1024;

    private static final Logger LOG = LoggerFactory.getLogger(ServerApplication.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final ServerConfig config;
    private final List<OwnerWorker> workers = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch terminated = new CountDownLatch(1);
    private ServerMetrics metrics;
    private CacheEngine engine;
    private EventLoopGroup acceptGroup;
    private EventLoopGroup ioGroup;
    private Channel clientChannel;
    private Channel peerChannel;

    private ServerApplication(ServerConfig config) {
        this.config = config;
    }

    /** 모든 구성 요소를 시작한다. EventLoop가 아닌 thread(main)에서 호출한다. */
    public static ServerApplication start(ServerConfig config) throws InterruptedException {
        ServerApplication app = new ServerApplication(config);
        try {
            app.startComponents();
            return app;
        } catch (InterruptedException | RuntimeException e) {
            app.close();
            throw e;
        }
    }

    private void startComponents() throws InterruptedException {
        metrics = ServerMetrics.start(config.adminHost(), config.adminPort());

        for (int i = 0; i < config.workers(); i++) {
            workers.add(new OwnerWorker(new WorkerId(i), WORKER_MAILBOX_CAPACITY));
        }
        engine = new KvEngine(config.nodeId(), workers, TimeSource.system());

        acceptGroup = new MultiThreadIoEventLoopGroup(
                1, new DefaultThreadFactory("openkache-accept"), NioIoHandler.newFactory());
        ioGroup = new MultiThreadIoEventLoopGroup(
                0, new DefaultThreadFactory("openkache-io"), NioIoHandler.newFactory());

        FrameEncoder encoder = new FrameEncoder();
        peerChannel = bind(config.peerPort(), encoder, new PeerMessageHandler(metrics));
        clientChannel =
                bind(
                        config.clientPort(),
                        encoder,
                        new ClientRequestHandler(metrics, engine, config.requestTimeout()));

        LOG.info(
                "node {} of cluster '{}' started: client={}:{} peer={}:{} (peer protocol not implemented) metrics=http://{}:{}/metrics workers={}",
                config.nodeId().value(),
                config.clusterId(),
                config.bindHost(),
                config.clientPort(),
                config.bindHost(),
                config.peerPort(),
                config.adminHost(),
                config.adminPort(),
                config.workers());
    }

    private Channel bind(int port, FrameEncoder encoder, ChannelHandler handler) throws InterruptedException {
        return new ServerBootstrap()
                .group(acceptGroup, ioGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(new FrameDecoder(), encoder, handler);
                            }
                        })
                .bind(new InetSocketAddress(config.bindHost(), port))
                .sync()
                .channel();
    }

    /** {@link #close()}가 끝날 때까지 기다린다. */
    public void awaitTermination() throws InterruptedException {
        terminated.await();
    }

    /**
     * listener → EventLoop → engine → worker → metrics 순서로 닫는다. 여러 번 호출해도 안전하다.
     *
     * <p>listener를 먼저 닫아 신규 요청을 막고, EventLoop를 닫아 진행 중인 I/O를 끝낸 뒤, engine을 닫아 아직 완료되지 않은
     * 요청을 {@code UNAVAILABLE}로 끝낸다. worker를 닫을 때 mailbox에 남아 있는 작업은 이미 engine이 처리했으므로
     * 버려도 안전하다.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            awaitQuietly();
            return;
        }
        LOG.info("shutting down");
        try {
            closeChannel(clientChannel);
            closeChannel(peerChannel);
            shutdown(acceptGroup);
            shutdown(ioGroup);
            if (engine != null) {
                engine.close();
            }
            workers.forEach(OwnerWorker::close);
            if (metrics != null) {
                metrics.close();
            }
            LOG.info("shutdown complete");
        } finally {
            terminated.countDown();
        }
    }

    private void awaitQuietly() {
        try {
            terminated.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeChannel(Channel channel) {
        if (channel != null) {
            channel.close().awaitUninterruptibly(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static void shutdown(EventLoopGroup group) {
        if (group != null) {
            group.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .awaitUninterruptibly(SHUTDOWN_TIMEOUT_SECONDS + 1, TimeUnit.SECONDS);
        }
    }
}
