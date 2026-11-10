package io.openkache.server.routing;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.model.WorkerId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.routing.Controller;
import io.openkache.core.tablet.routing.LocalRouting;
import io.openkache.core.tablet.routing.NodeAddress;
import io.openkache.core.tablet.routing.NodeDirectory;
import io.openkache.core.tablet.routing.OwnershipCheck;
import io.openkache.core.tablet.routing.SimpleNodeDirectory;
import io.openkache.core.tablet.routing.TableMessageCodec;
import io.openkache.server.netty.Frame;
import io.openkache.server.netty.FrameDecoder;
import io.openkache.server.netty.FrameEncoder;
import io.openkache.server.netty.Protocol;
import io.openkache.server.replication.PbTransport;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * B1 라우팅 실서버 조립부. 실제 TCP로 라우팅 표를 전파하고 자기 Tablet이 아닌 요청을 MOVED로 돌려보낸다.
 *
 * <ul>
 *   <li>client listener: GET/SET/DELETE를 {@link LocalRouting#check}로 판정한다. 내 Tablet이면
 *       {@link KvEngine}, 아니면 MOVED와 §6-1 redirect 주소로 응답한다.
 *   <li>peer listener: opcode 5·6·7(표 메시지), 8(실험 제어, controller 전용), 9(표 진단), PING.
 *   <li>node 0이 Controller를 겸한다. 표 변경은 CONTROL(8)로만 일어나고 변경 즉시 push된다.
 * </ul>
 *
 * <p>초기 표는 모든 노드가 같은 설정으로 {@link HashRangeScheme#initial}을 계산해 일치한다.
 * 인증이 없으므로 전용 보안 그룹 안에서만 사용한다.
 */
public final class B1Server implements AutoCloseable {

    private static final byte[] EMPTY = new byte[0];

    private final B1NodeConfig config;
    private final TimeSource time = TimeSource.system();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong refreshRequestedAt = new AtomicLong(-1);

    private EventLoopGroup io;
    private EventLoopGroup accept;
    private OwnerWorker kvWorker;
    private KvEngine kv;
    private PbTransport transport;
    private LocalRouting routing;
    private Controller controller;
    private NodeDirectory directory;
    private Channel client;
    private Channel peer;

    private B1Server(B1NodeConfig config) {
        this.config = config;
    }

    /** 동기 bind는 호출 thread에서만 수행한다. 실패하면 이미 시작한 구성 요소도 닫는다. */
    public static B1Server start(B1NodeConfig config) throws InterruptedException {
        B1Server server = new B1Server(config);
        try {
            server.open();
            return server;
        } catch (RuntimeException | InterruptedException e) {
            server.close();
            throw e;
        }
    }

    private void open() throws InterruptedException {
        NodeId self = config.self();
        kvWorker = new OwnerWorker(new WorkerId(0), 4096);
        kv = new KvEngine(self, java.util.List.of(kvWorker), time);

        RoutingTable initial = new HashRangeScheme(config.tabletsPerNode()).initial(config.nodeIds());

        io = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        accept = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        transport = new PbTransport(io, config.peerAddresses(), B1Wire::encode);

        routing = new LocalRouting(self, initial, transport);
        if (config.node() == 0) {
            controller = new Controller(self, initial, transport);
        }
        SimpleNodeDirectory.Builder directoryBuilder = SimpleNodeDirectory.builder();
        for (int i = 0; i < config.hosts().size(); i++) {
            directoryBuilder.add(new NodeId(i), config.hosts().get(i), config.clientPort());
        }
        directory = directoryBuilder.build();

        peer = bind(config.peerPort(), new PeerHandler());
        client = bind(config.clientPort(), new ClientHandler());
    }

    private Channel bind(int port, ChannelHandler handler) throws InterruptedException {
        return new ServerBootstrap().group(accept, io).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new FrameDecoder(), new FrameEncoder(), handler);
                    }
                })
                .bind(config.hosts().get(config.node()), port).sync().channel();
    }

    public RoutingTable table() {
        return routing.table();
    }

    public void awaitTermination() throws InterruptedException {
        client.closeFuture().sync();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (client != null) client.close().awaitUninterruptibly();
        if (peer != null) peer.close().awaitUninterruptibly();
        if (transport != null) transport.close();
        if (io != null) io.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
        if (accept != null) accept.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
        if (kv != null) kv.close();
        if (kvWorker != null) kvWorker.close();
    }

    // -----------------------------------------------------------------------
    // 응답 공통: §3 body 뒤에 선택적 §6-1 redirect를 덧붙인다
    // -----------------------------------------------------------------------

    private static Frame reply(Frame request, ResultStatus status, long routingVersion, long epoch,
                               long valueVersion, String detail, byte[] value, byte[] redirect) {
        byte[] d = detail.getBytes(StandardCharsets.UTF_8);
        ByteBuffer body = ByteBuffer.allocate(8 + 8 + 4 + d.length + 4 + value.length + redirect.length);
        body.putLong(epoch).putLong(valueVersion)
                .putInt(d.length).put(d)
                .putInt(value.length).put(value)
                .put(redirect);
        return new Frame(Protocol.CLASS_RESPONSE, request.opcode(), 0, Protocol.statusCode(status),
                request.requestId(), routingVersion, Bytes.copyOf(body.array()));
    }

    // -----------------------------------------------------------------------
    // client listener
    // -----------------------------------------------------------------------

    @ChannelHandler.Sharable
    private final class ClientHandler extends SimpleChannelInboundHandler<Frame> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (frame.messageClass() != Protocol.CLASS_CLIENT_REQUEST || frame.flags() != 0 || frame.status() != 0) {
                respond(ctx, frame, ResultStatus.BAD_REQUEST, "invalid client header");
                return;
            }
            switch (frame.opcode()) {
                case Protocol.OP_PING -> respond(ctx, frame,
                        frame.body().isEmpty() ? ResultStatus.OK : ResultStatus.BAD_REQUEST, "");
                case Protocol.OP_GET, Protocol.OP_SET, Protocol.OP_DELETE -> handleCommand(ctx, frame);
                default -> respond(ctx, frame, ResultStatus.BAD_REQUEST, "unknown opcode " + frame.opcode());
            }
        }

        private void handleCommand(ChannelHandlerContext ctx, Frame frame) {
            DecodedRequest decoded;
            try {
                decoded = decodeRequest(frame.opcode(), frame.body());
            } catch (IllegalArgumentException e) {
                respond(ctx, frame, ResultStatus.BAD_REQUEST, e.getMessage());
                return;
            }

            long keyHash = KeyHash.of(decoded.key());
            RoutingVersion clientVersion = new RoutingVersion(frame.routingVersion());
            OwnershipCheck.Decision decision = routing.check(keyHash, clientVersion, decoded.epoch());
            maybeRequestTable();

            if (!decision.allowed()) {
                byte[] redirect = decision.status() == ResultStatus.MOVED
                        ? TableMessageCodec.encodeRedirect(
                                decision.redirect().flatMap(id -> directory.lookup(id)))
                        : EMPTY;
                ctx.writeAndFlush(reply(frame, decision.status(), decision.serverVersion().value(),
                        decision.tablet().epoch().value(), 0, "", EMPTY, redirect));
                return;
            }

            Duration timeout = decoded.timeoutMs() > 0 ? Duration.ofMillis(decoded.timeoutMs()) : Duration.ofSeconds(1);
            RequestContext context = new RequestContext(decoded.clientId(), decoded.operationId(),
                    clientVersion, decoded.epoch(), Deadline.after(time, timeout));
            kv.execute(decoded.command(), context).whenComplete((result, error) -> {
                if (error != null) {
                    respond(ctx, frame, ResultStatus.INTERNAL_ERROR, "execute failed");
                    return;
                }
                byte[] value = result.value().map(Bytes::toByteArray).orElse(EMPTY);
                ctx.writeAndFlush(reply(frame, result.status(), routing.version().value(),
                        decision.tablet().epoch().value(), result.valueVersion(), "", value, EMPTY));
            });
        }

        /** 내가 낡았다는 힌트가 켜졌으면 버전당 한 번만 Controller에 표를 요청한다. */
        private void maybeRequestTable() {
            if (controller != null || !routing.needsRefresh()) return;
            long current = routing.version().value();
            if (refreshRequestedAt.getAndSet(current) != current) {
                routing.requestTable(config.controller());
            }
        }

        private void respond(ChannelHandlerContext ctx, Frame frame, ResultStatus status, String detail) {
            ctx.writeAndFlush(Frame.response(frame.requestId(), frame.opcode(), status, detail));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
            ctx.close();
        }
    }

    private record DecodedRequest(Command command, Bytes key, ClientId clientId, OperationId operationId,
                                  OwnershipEpoch epoch, long timeoutMs) {}

    /** protocol-v1.md §2 body를 해석한다. */
    private static DecodedRequest decodeRequest(int opcode, Bytes body) {
        try {
            ByteBuffer b = ByteBuffer.wrap(body.toByteArray());
            long clientId = b.getLong();
            long operationId = b.getLong();
            long epoch = b.getLong();
            long timeoutMs = Integer.toUnsignedLong(b.getInt());
            Bytes key = lengthPrefixed(b);
            Command command = switch (opcode) {
                case Protocol.OP_GET -> new Command.Get(key);
                case Protocol.OP_DELETE -> new Command.Delete(key);
                case Protocol.OP_SET -> new Command.Set(key, lengthPrefixed(b));
                default -> throw new IllegalArgumentException("not a kv opcode: " + opcode);
            };
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("trailing bytes: " + b.remaining());
            }
            return new DecodedRequest(command, key, new ClientId(clientId), new OperationId(operationId),
                    new OwnershipEpoch(epoch), timeoutMs);
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("short request body");
        }
    }

    private static Bytes lengthPrefixed(ByteBuffer b) {
        int length = b.getInt();
        if (length < 0 || length > b.remaining()) {
            throw new IllegalArgumentException("invalid field length: " + length);
        }
        byte[] data = new byte[length];
        b.get(data);
        return Bytes.copyOf(data);
    }

    // -----------------------------------------------------------------------
    // peer listener
    // -----------------------------------------------------------------------

    @ChannelHandler.Sharable
    private final class PeerHandler extends SimpleChannelInboundHandler<Frame> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (frame.messageClass() != Protocol.CLASS_PEER || frame.flags() != 0 || frame.status() != 0) {
                respond(ctx, frame, ResultStatus.BAD_REQUEST, "invalid peer header");
                return;
            }
            try {
                if (frame.opcode() >= B1Wire.TABLE_PUSH && frame.opcode() <= B1Wire.TABLE_REPLY) {
                    PeerEnvelope envelope = B1Wire.decode(frame);
                    if (envelope.to().value() != config.node()) {
                        throw new IllegalArgumentException("misdirected B1 peer message");
                    }
                    boolean handled = controller != null
                            ? controller.onPeerMessage(envelope.from(), envelope.message())
                            : routing.onPeerMessage(envelope.from(), envelope.message());
                    respond(ctx, frame, handled ? ResultStatus.OK : ResultStatus.BAD_REQUEST, "");
                } else if (frame.opcode() == B1Wire.CONTROL) {
                    control(ctx, frame);
                } else if (frame.opcode() == B1Wire.TABLE_DUMP && frame.body().isEmpty()) {
                    byte[] table = TableMessageCodec.encodeTable(routing.table());
                    ctx.writeAndFlush(reply(frame, ResultStatus.OK, routing.version().value(),
                            0, 0, "", table, EMPTY));
                } else if (frame.opcode() == Protocol.OP_PING && frame.body().isEmpty()) {
                    respond(ctx, frame, ResultStatus.OK, "");
                } else {
                    respond(ctx, frame, ResultStatus.NOT_IMPLEMENTED, "unknown B1 peer opcode");
                }
            } catch (IllegalArgumentException e) {
                respond(ctx, frame, ResultStatus.BAD_REQUEST, e.getMessage());
            }
        }

        /**
         * 실험 전용 제어(§6-4). body = kind(u8), expectedVersion(u64), kind별 필드.
         * kind 0 = MOVE(tabletId u64, from u64, to u64), 1 = SPLIT(tabletId u64, at u64).
         * controller 노드만 받고, CAS 실패(낡은 expectedVersion)는 UNAVAILABLE이다.
         */
        private void control(ChannelHandlerContext ctx, Frame frame) {
            if (controller == null) {
                respond(ctx, frame, ResultStatus.NOT_IMPLEMENTED, "not the controller node");
                return;
            }
            ByteBuffer b = ByteBuffer.wrap(frame.body().toByteArray());
            Optional<RoutingTable> applied;
            try {
                int kind = Byte.toUnsignedInt(b.get());
                RoutingVersion expected = new RoutingVersion(b.getLong());
                applied = switch (kind) {
                    case 0 -> controller.move(expected,
                            new Change(new TabletId(b.getLong()), new NodeId(b.getLong()), new NodeId(b.getLong())));
                    case 1 -> controller.split(expected, new TabletId(b.getLong()), b.getLong());
                    default -> throw new IllegalArgumentException("unknown control kind: " + kind);
                };
                if (b.hasRemaining()) {
                    throw new IllegalArgumentException("trailing control bytes");
                }
            } catch (BufferUnderflowException e) {
                respond(ctx, frame, ResultStatus.BAD_REQUEST, "short control body");
                return;
            } catch (IllegalArgumentException | IllegalStateException e) {
                respond(ctx, frame, ResultStatus.BAD_REQUEST, e.getMessage());
                return;
            }
            if (applied.isEmpty()) {
                respond(ctx, frame, ResultStatus.UNAVAILABLE, "stale expected version");
                return;
            }
            // Controller는 자기 자신에게 push하지 않으므로 겸임 노드의 표를 직접 갱신한다
            routing.accept(applied.get());
            ctx.writeAndFlush(reply(frame, ResultStatus.OK, applied.get().version().value(),
                    0, 0, "", EMPTY, EMPTY));
        }

        private void respond(ChannelHandlerContext ctx, Frame frame, ResultStatus status, String detail) {
            ctx.writeAndFlush(Frame.response(frame.requestId(), frame.opcode(), status, detail));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
            ctx.close();
        }
    }
}
