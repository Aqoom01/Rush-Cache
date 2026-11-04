package com.openkache.failuredetector.transport;

import com.openkache.failuredetector.aerospike.AerospikeMeshConfig;
import com.openkache.failuredetector.aerospike.AerospikeMeshFailureDetector;
import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.FailureDetector;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.ignite.IgniteTcpRingConfig;
import com.openkache.failuredetector.ignite.IgniteTcpRingFailureDetector;
import com.openkache.failuredetector.phi.GossipHeartbeat;
import com.openkache.failuredetector.phi.PhiAccrualFailureDetector;
import com.openkache.failuredetector.product.ProductAlgorithm;
import com.openkache.failuredetector.redis.RedisClusterConfig;
import com.openkache.failuredetector.redis.RedisClusterFailureDetector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One-process-per-node real-socket adapter for the five product-inspired detector profiles.
 *
 * <p>This is not wire-compatible with the products. Redis and Phi use versioned UDP lab frames,
 * while Ignite and Aerospike heartbeat traffic uses a persistent TCP connection. Aerospike's
 * explicitly simplified membership agreement uses UDP proposal/ack frames.</p>
 */
public final class ProductUdpExperimentNodeMain {
    private static final int OBSERVER_ID = 0;
    private static final int TARGET_ID = 1;
    private static final int TCP_MAGIC = 0x46445443;
    private static final int TCP_APPLICATION_BYTES = Integer.BYTES * 2 + Long.BYTES;
    private static final long STARTUP_GRACE_MILLIS = 3_000;
    private static final long REDIS_INTERVAL_MILLIS = 1_000;
    private static final long PHI_INTERVAL_MILLIS = 1_000;
    private static final long IGNITE_INTERVAL_MILLIS = 1_000;
    private static final long AEROSPIKE_INTERVAL_MILLIS = 150;

    private ProductUdpExperimentNodeMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("control")) {
            sendFaultControl(args);
            return;
        }
        new Node(NodeConfig.fromEnvironment()).run();
    }

    private static void sendFaultControl(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("control requires host, port, and optional start|end");
        }
        UdpWireMessage.Type type = args.length == 3 || args[3].equals("start")
                ? UdpWireMessage.Type.FAULT_START
                : args[3].equals("end") ? UdpWireMessage.Type.FAULT_END : null;
        if (type == null) {
            throw new IllegalArgumentException("control phase must be start or end");
        }
        UdpWireMessage message = new UdpWireMessage(type, 0, -1, OBSERVER_ID, TARGET_ID);
        byte[] payload = message.encode();
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.send(new DatagramPacket(
                    payload,
                    payload.length,
                    new InetSocketAddress(args[1], Integer.parseInt(args[2]))
            ));
        }
    }

    private static final class Node implements AutoCloseable {
        private final NodeConfig config;
        private final DatagramSocket udpSocket;
        private final ScheduledExecutorService state;
        private final Thread udpReceiver;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicLong sentFrames = new AtomicLong();
        private final AtomicLong sentBytes = new AtomicLong();
        private final AtomicLong receivedFrames = new AtomicLong();
        private final AtomicLong receivedBytes = new AtomicLong();
        private final CountDownLatch finished = new CountDownLatch(1);
        private final List<DetectorEvent> detectorEvents = new ArrayList<>();
        private final long startedAtNanos = System.nanoTime();
        private final RedisClusterFailureDetector redisDetector;
        private final FailureDetector observerDetector;
        private final List<String> nodeNames;
        private final Set<String> redisMasters;
        private ServerSocket tcpServer;
        private Thread tcpAcceptor;
        private Socket tcpClient;
        private DataOutputStream tcpOutput;
        private long sequence;
        private long aerospikeEpoch;
        private Long faultAtNanos;
        private Long faultEndAtNanos;

        private Node(NodeConfig config) throws IOException {
            this.config = config;
            this.udpSocket = new DatagramSocket(
                    new InetSocketAddress("0.0.0.0", config.udpPort()));
            this.state = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "product-fd-state-" + config.nodeId());
                thread.setDaemon(false);
                return thread;
            });
            this.udpReceiver = new Thread(
                    this::receiveUdpLoop, "product-fd-udp-" + config.nodeId());
            this.udpReceiver.setDaemon(true);
            this.nodeNames = config.allNodeIds().stream()
                    .sorted()
                    .map(ProductUdpExperimentNodeMain::nodeName)
                    .toList();
            this.redisMasters = config.allNodeIds().stream()
                    .filter(id -> id != TARGET_ID)
                    .map(ProductUdpExperimentNodeMain::nodeName)
                    .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));

            RedisClusterFailureDetector redis = null;
            FailureDetector observer = null;
            switch (config.algorithm()) {
                case REDIS_CLUSTER -> {
                    if (config.nodeId() != TARGET_ID) {
                        redis = new RedisClusterFailureDetector(
                                nodeName(config.nodeId()), redisMasters,
                                RedisClusterConfig.defaults());
                        redis.registerTarget(0, nodeName(TARGET_ID));
                    }
                    if (config.nodeId() == OBSERVER_ID) {
                        observer = redis;
                    }
                }
                case PHI_GOSSIP -> {
                    if (config.nodeId() == OBSERVER_ID) {
                        observer = new PhiAccrualFailureDetector(nodeName(OBSERVER_ID));
                    }
                }
                case IGNITE_TCP_RING -> {
                    if (config.nodeId() == OBSERVER_ID) {
                        observer = new IgniteTcpRingFailureDetector(
                                nodeName(OBSERVER_ID), nodeName(OBSERVER_ID), nodeNames,
                                IgniteTcpRingConfig.defaults());
                    }
                }
                case AEROSPIKE_MESH -> {
                    if (config.nodeId() == OBSERVER_ID) {
                        observer = new AerospikeMeshFailureDetector(
                                nodeName(OBSERVER_ID), Set.copyOf(nodeNames),
                                AerospikeMeshConfig.defaults());
                    }
                }
                case SWIM_LIFEGUARD -> throw new IllegalArgumentException(
                        "SWIM + Lifeguard must use UdpExperimentNodeMain");
            }
            this.redisDetector = redis;
            this.observerDetector = observer;
            if (usesTcpHeartbeat() && config.nodeId() == OBSERVER_ID) {
                startTcpServer();
            }
        }

        private void run() throws InterruptedException {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "product-fd-shutdown"));
            udpReceiver.start();
            state.scheduleAtFixedRate(this::tick, 0, 50, TimeUnit.MILLISECONDS);
            state.scheduleAtFixedRate(
                    this::sendProtocolTraffic,
                    protocolIntervalMillis(config.algorithm()),
                    protocolIntervalMillis(config.algorithm()),
                    TimeUnit.MILLISECONDS
            );
            state.schedule(this::finish, config.durationSeconds(), TimeUnit.SECONDS);
            finished.await();
        }

        private void receiveUdpLoop() {
            byte[] bytes = new byte[UdpWireMessage.WIRE_BYTES];
            while (running.get()) {
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
                try {
                    udpSocket.receive(packet);
                    receivedFrames.incrementAndGet();
                    receivedBytes.addAndGet(packet.getLength());
                    UdpWireMessage message = UdpWireMessage.decode(
                            packet.getData(), packet.getLength());
                    state.execute(() -> handleUdp(message));
                } catch (SocketException exception) {
                    if (running.get()) {
                        System.err.println("UDP receiver failed: " + exception.getMessage());
                    }
                    return;
                } catch (IOException | IllegalArgumentException exception) {
                    System.err.println("UDP frame rejected: " + exception.getMessage());
                }
            }
        }

        private void handleUdp(UdpWireMessage message) {
            switch (message.type()) {
                case PING -> {
                    if (config.algorithm() == ProductAlgorithm.REDIS_CLUSTER
                            && config.nodeId() == TARGET_ID) {
                        sendUdp(message.origin(), new UdpWireMessage(
                                UdpWireMessage.Type.ACK,
                                message.sequence(), config.nodeId(),
                                message.origin(), config.nodeId()));
                    }
                }
                case ACK -> onAck(message);
                case HEARTBEAT -> onUdpHeartbeat(message);
                case SUSPECT -> onRedisFailureReport(message);
                case PING_REQUEST -> onAerospikeProposal(message);
                case FAULT_START -> recordFaultStart();
                case FAULT_END -> recordFaultEnd();
                case MEMBERSHIP_REMOVE, EXPERIMENT_FINISH, PROCESS_CRASH, ALIVE -> {
                    // Reserved by the other real-socket harnesses.
                }
            }
        }

        private void onAck(UdpWireMessage message) {
            long now = elapsedNanos();
            if (config.algorithm() == ProductAlgorithm.REDIS_CLUSTER
                    && redisDetector != null && message.sender() == TARGET_ID) {
                redisDetector.recordPong(now, nodeName(TARGET_ID));
                drainRedisEvents();
            } else if (config.algorithm() == ProductAlgorithm.AEROSPIKE_MESH
                    && config.nodeId() == OBSERVER_ID
                    && observerDetector instanceof AerospikeMeshFailureDetector aerospike
                    && message.target() == TARGET_ID) {
                aerospike.acknowledgeMembershipProposal(
                        now, message.sequence(), nodeName(message.sender()));
                drainObserverEvents();
            }
        }

        private void onUdpHeartbeat(UdpWireMessage message) {
            if (config.algorithm() != ProductAlgorithm.PHI_GOSSIP
                    || config.nodeId() != OBSERVER_ID || message.sender() != TARGET_ID) {
                return;
            }
            PhiAccrualFailureDetector phi = (PhiAccrualFailureDetector) observerDetector;
            phi.recordHeartbeat(new GossipHeartbeat(
                    nodeName(TARGET_ID), 1, message.sequence(), elapsedNanos()), true);
            drainObserverEvents();
        }

        private void onRedisFailureReport(UdpWireMessage message) {
            if (config.algorithm() != ProductAlgorithm.REDIS_CLUSTER
                    || config.nodeId() != OBSERVER_ID || redisDetector == null
                    || message.target() != TARGET_ID) {
                return;
            }
            redisDetector.recordFailureReport(
                    elapsedNanos(), nodeName(TARGET_ID), nodeName(message.sender()));
            drainRedisEvents();
        }

        private void onAerospikeProposal(UdpWireMessage message) {
            if (config.algorithm() != ProductAlgorithm.AEROSPIKE_MESH
                    || config.nodeId() == OBSERVER_ID || config.nodeId() == TARGET_ID
                    || message.target() != TARGET_ID) {
                return;
            }
            sendUdp(OBSERVER_ID, new UdpWireMessage(
                    UdpWireMessage.Type.ACK,
                    message.sequence(), config.nodeId(), OBSERVER_ID, TARGET_ID));
        }

        private void recordFaultStart() {
            if (config.nodeId() == OBSERVER_ID && faultAtNanos == null) {
                faultAtNanos = elapsedNanos();
                System.out.println("EVENT_JSON={\"kind\":\"fault-start\",\"atNanos\":"
                        + faultAtNanos + ",\"target\":\"node-1\"}");
            }
        }

        private void recordFaultEnd() {
            if (config.nodeId() == OBSERVER_ID && faultAtNanos != null
                    && faultEndAtNanos == null) {
                faultEndAtNanos = elapsedNanos();
                System.out.println("EVENT_JSON={\"kind\":\"fault-end\",\"atNanos\":"
                        + faultEndAtNanos + ",\"target\":\"node-1\"}");
            }
        }

        private void sendProtocolTraffic() {
            if (!running.get()) {
                return;
            }
            switch (config.algorithm()) {
                case REDIS_CLUSTER -> sendRedisPing();
                case PHI_GOSSIP -> sendPhiHeartbeat();
                case IGNITE_TCP_RING, AEROSPIKE_MESH -> sendTcpHeartbeat();
                case SWIM_LIFEGUARD -> {
                    // Routed to UdpExperimentNodeMain by the shell runner.
                }
            }
        }

        private void sendRedisPing() {
            if (config.nodeId() == TARGET_ID || redisDetector == null) {
                return;
            }
            long now = elapsedNanos();
            redisDetector.recordPingSent(now, nodeName(TARGET_ID));
            sendUdp(TARGET_ID, new UdpWireMessage(
                    UdpWireMessage.Type.PING, ++sequence, config.nodeId(),
                    config.nodeId(), TARGET_ID));
        }

        private void sendPhiHeartbeat() {
            if (config.nodeId() != TARGET_ID) {
                return;
            }
            sendUdp(OBSERVER_ID, new UdpWireMessage(
                    UdpWireMessage.Type.HEARTBEAT, ++sequence, config.nodeId(),
                    config.nodeId(), OBSERVER_ID));
        }

        private void tick() {
            long now = elapsedNanos();
            if (now < TimeUnit.MILLISECONDS.toNanos(STARTUP_GRACE_MILLIS)) {
                return;
            }
            if (config.algorithm() == ProductAlgorithm.REDIS_CLUSTER) {
                if (redisDetector != null) {
                    redisDetector.tick(now);
                    drainRedisEvents();
                }
                return;
            }
            if (observerDetector == null) {
                return;
            }
            observerDetector.tick(now);
            drainObserverEvents();
            advanceClusterDecision(now);
            drainObserverEvents();
        }

        private void drainRedisEvents() {
            if (redisDetector == null) {
                return;
            }
            for (DetectorEvent event : redisDetector.drainEvents()) {
                if (config.nodeId() == OBSERVER_ID) {
                    recordObserverEvent(event);
                } else if (event.newStatus() == NodeStatus.SUSPECT) {
                    sendUdp(OBSERVER_ID, new UdpWireMessage(
                            UdpWireMessage.Type.SUSPECT, ++sequence, config.nodeId(),
                            config.nodeId(), TARGET_ID));
                }
            }
        }

        private void drainObserverEvents() {
            if (observerDetector == null) {
                return;
            }
            for (DetectorEvent event : observerDetector.drainEvents()) {
                recordObserverEvent(event);
            }
        }

        private void recordObserverEvent(DetectorEvent event) {
            detectorEvents.add(event);
            System.out.println("EVENT_JSON={\"kind\":\"detector\",\"algorithm\":\""
                    + config.algorithm().id() + "\",\"atNanos\":" + event.atNanos()
                    + ",\"oldStatus\":\"" + event.oldStatus()
                    + "\",\"newStatus\":\"" + event.newStatus()
                    + "\",\"reason\":\"" + escape(event.reason()) + "\"}");
        }

        private void advanceClusterDecision(long now) {
            if (observerDetector == null
                    || observerDetector.status(nodeName(TARGET_ID)) != NodeStatus.SUSPECT) {
                return;
            }
            if (observerDetector instanceof IgniteTcpRingFailureDetector ignite) {
                ignite.coordinatorConfirmFailure(
                        now, nodeName(OBSERVER_ID), nodeName(TARGET_ID));
            } else if (observerDetector instanceof AerospikeMeshFailureDetector aerospike
                    && aerospikeEpoch == 0) {
                aerospikeEpoch = Math.max(1, aerospike.viewVersion() + 1);
                if (aerospike.proposeMembershipRemoval(
                        aerospikeEpoch, nodeName(TARGET_ID))) {
                    for (int peer : config.allNodeIds()) {
                        if (peer != OBSERVER_ID && peer != TARGET_ID) {
                            sendUdp(peer, new UdpWireMessage(
                                    UdpWireMessage.Type.PING_REQUEST,
                                    aerospikeEpoch, OBSERVER_ID, OBSERVER_ID, TARGET_ID));
                        }
                    }
                }
            }
        }

        private void sendUdp(int peer, UdpWireMessage message) {
            InetSocketAddress address = config.peers().get(peer);
            if (address == null) {
                return;
            }
            byte[] payload = message.encode();
            try {
                udpSocket.send(new DatagramPacket(payload, payload.length, address));
                sentFrames.incrementAndGet();
                sentBytes.addAndGet(payload.length);
            } catch (IOException exception) {
                System.err.println("UDP send failed: " + exception.getMessage());
            }
        }

        private boolean usesTcpHeartbeat() {
            return config.algorithm() == ProductAlgorithm.IGNITE_TCP_RING
                    || config.algorithm() == ProductAlgorithm.AEROSPIKE_MESH;
        }

        private void startTcpServer() throws IOException {
            tcpServer = new ServerSocket();
            tcpServer.setReuseAddress(true);
            tcpServer.bind(new InetSocketAddress("0.0.0.0", config.tcpPort()));
            tcpAcceptor = new Thread(this::acceptTcpLoop, "product-fd-tcp-accept");
            tcpAcceptor.setDaemon(true);
            tcpAcceptor.start();
        }

        private void acceptTcpLoop() {
            while (running.get()) {
                try {
                    Socket socket = tcpServer.accept();
                    Thread reader = new Thread(
                            () -> readTcpLoop(socket), "product-fd-tcp-reader");
                    reader.setDaemon(true);
                    reader.start();
                } catch (SocketException exception) {
                    if (running.get()) {
                        System.err.println("TCP accept failed: " + exception.getMessage());
                    }
                    return;
                } catch (IOException exception) {
                    System.err.println("TCP accept failed: " + exception.getMessage());
                }
            }
        }

        private void readTcpLoop(Socket socket) {
            try (socket; DataInputStream input = new DataInputStream(socket.getInputStream())) {
                while (running.get()) {
                    int magic = input.readInt();
                    int sender = input.readInt();
                    long heartbeatSequence = input.readLong();
                    if (magic != TCP_MAGIC) {
                        throw new IOException("invalid TCP heartbeat magic");
                    }
                    receivedFrames.incrementAndGet();
                    receivedBytes.addAndGet(TCP_APPLICATION_BYTES);
                    state.execute(() -> onTcpHeartbeat(sender, heartbeatSequence));
                }
            } catch (EOFException ignored) {
                // A closed connection is converted to a detector timeout by the state thread.
            } catch (IOException exception) {
                if (running.get()) {
                    System.err.println("TCP heartbeat read failed: " + exception.getMessage());
                }
            }
        }

        private void onTcpHeartbeat(int sender, long heartbeatSequence) {
            if (config.nodeId() != OBSERVER_ID || observerDetector == null) {
                return;
            }
            long now = elapsedNanos();
            if (observerDetector instanceof IgniteTcpRingFailureDetector ignite) {
                ignite.recordNeighborMessage(now, nodeName(sender));
            } else if (observerDetector instanceof AerospikeMeshFailureDetector aerospike) {
                aerospike.recordHeartbeat(now, nodeName(sender));
            }
            drainObserverEvents();
        }

        private void sendTcpHeartbeat() {
            if (config.nodeId() == OBSERVER_ID) {
                return;
            }
            try {
                if (tcpClient == null || tcpClient.isClosed()) {
                    tcpClient = new Socket();
                    tcpClient.connect(new InetSocketAddress("node0", config.tcpPort()), 200);
                    tcpClient.setTcpNoDelay(true);
                    tcpOutput = new DataOutputStream(tcpClient.getOutputStream());
                }
                tcpOutput.writeInt(TCP_MAGIC);
                tcpOutput.writeInt(config.nodeId());
                tcpOutput.writeLong(++sequence);
                tcpOutput.flush();
                sentFrames.incrementAndGet();
                sentBytes.addAndGet(TCP_APPLICATION_BYTES);
            } catch (IOException exception) {
                closeTcpClient();
            }
        }

        private long elapsedNanos() {
            return System.nanoTime() - startedAtNanos;
        }

        private void finish() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            long runDurationNanos = elapsedNanos();
            Long suspectAt = firstEventAtOrAfter(faultAtNanos, NodeStatus.SUSPECT);
            Long deadAt = firstEventAtOrAfter(faultAtNanos, NodeStatus.DEAD);
            Long recoveredAt = firstEventAtOrAfter(faultEndAtNanos, NodeStatus.ALIVE);
            long preFaultDeadEvents = detectorEvents.stream()
                    .filter(event -> event.newStatus() == NodeStatus.DEAD)
                    .filter(event -> faultAtNanos == null || event.atNanos() < faultAtNanos)
                    .count();
            long preFaultSuspectEvents = detectorEvents.stream()
                    .filter(event -> event.newStatus() == NodeStatus.SUSPECT)
                    .filter(event -> faultAtNanos == null || event.atNanos() < faultAtNanos)
                    .count();
            System.out.println("RESULT_JSON={"
                    + "\"profile\":\"" + config.algorithm().id() + "\","
                    + "\"implementationType\":\"inspired-real-socket-adapter\","
                    + "\"nodeId\":" + config.nodeId() + ","
                    + "\"timeSource\":\"observer-monotonic\","
                    + "\"wireSource\":\"" + wireSource() + "\","
                    + "\"runDurationNanos\":" + runDurationNanos + ","
                    + "\"faultAtNanos\":" + nullable(faultAtNanos) + ","
                    + "\"faultEndAtNanos\":" + nullable(faultEndAtNanos) + ","
                    + "\"preFaultSuspectEvents\":" + preFaultSuspectEvents + ","
                    + "\"preFaultDeadEvents\":" + preFaultDeadEvents + ","
                    + "\"suspectLatencyNanos\":" + latency(faultAtNanos, suspectAt) + ","
                    + "\"deadLatencyNanos\":" + latency(faultAtNanos, deadAt) + ","
                    + "\"recoveredAtNanos\":" + nullable(recoveredAt) + ","
                    + "\"recoveryLatencyNanos\":"
                    + latency(faultEndAtNanos, recoveredAt) + ","
                    + "\"sentFrames\":" + sentFrames.get() + ","
                    + "\"sentBytes\":" + sentBytes.get() + ","
                    + "\"receivedFrames\":" + receivedFrames.get() + ","
                    + "\"receivedBytes\":" + receivedBytes.get()
                    + "}");
            udpSocket.close();
            closeTcpClient();
            if (tcpServer != null) {
                try {
                    tcpServer.close();
                } catch (IOException ignored) {
                    // Best-effort shutdown.
                }
            }
            state.shutdownNow();
            finished.countDown();
        }

        private Long firstEventAtOrAfter(Long threshold, NodeStatus status) {
            if (threshold == null) {
                return null;
            }
            return detectorEvents.stream()
                    .filter(event -> event.newStatus() == status)
                    .map(DetectorEvent::atNanos)
                    .filter(at -> at >= threshold)
                    .findFirst()
                    .orElse(null);
        }

        private String wireSource() {
            return usesTcpHeartbeat()
                    ? "real-tcp-heartbeat-and-udp-control-observed"
                    : "real-udp-observed";
        }

        private static String latency(Long start, Long end) {
            return start == null || end == null || end < start
                    ? "null" : Long.toString(end - start);
        }

        private static String nullable(Long value) {
            return value == null ? "null" : Long.toString(value);
        }

        private void closeTcpClient() {
            if (tcpClient != null) {
                try {
                    tcpClient.close();
                } catch (IOException ignored) {
                    // Best-effort reconnect cleanup.
                }
            }
            tcpClient = null;
            tcpOutput = null;
        }

        @Override
        public void close() {
            finish();
        }
    }

    static long protocolIntervalMillis(ProductAlgorithm algorithm) {
        return switch (algorithm) {
            case REDIS_CLUSTER -> REDIS_INTERVAL_MILLIS;
            case PHI_GOSSIP -> PHI_INTERVAL_MILLIS;
            case IGNITE_TCP_RING -> IGNITE_INTERVAL_MILLIS;
            case AEROSPIKE_MESH -> AEROSPIKE_INTERVAL_MILLIS;
            case SWIM_LIFEGUARD -> throw new IllegalArgumentException(
                    "SWIM uses the dedicated UDP harness");
        };
    }

    private static String nodeName(int nodeId) {
        return "node-" + nodeId;
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private record NodeConfig(
            int nodeId,
            int udpPort,
            int tcpPort,
            int durationSeconds,
            ProductAlgorithm algorithm,
            Map<Integer, InetSocketAddress> peers
    ) {
        private static NodeConfig fromEnvironment() {
            int nodeId = integer("FD_NODE_ID");
            int udpPort = integer("FD_PORT");
            int tcpPort = optionalInteger("FD_TCP_PORT", 19001);
            int duration = integer("FD_DURATION_SECONDS");
            ProductAlgorithm algorithm = ProductAlgorithm.fromId(
                    required("FD_PRODUCT_ALGORITHM"));
            Map<Integer, InetSocketAddress> peers = new LinkedHashMap<>();
            for (String item : required("FD_PEERS").split(",")) {
                String[] idAndAddress = item.split("=", 2);
                String[] hostAndPort = idAndAddress[1].split(":", 2);
                peers.put(
                        Integer.parseInt(idAndAddress[0]),
                        new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]))
                );
            }
            peers.remove(nodeId);
            return new NodeConfig(
                    nodeId, udpPort, tcpPort, duration, algorithm, Map.copyOf(peers));
        }

        private Set<Integer> allNodeIds() {
            Set<Integer> ids = new LinkedHashSet<>(peers.keySet());
            ids.add(nodeId);
            return Set.copyOf(ids);
        }

        private static int integer(String name) {
            return Integer.parseInt(required(name));
        }

        private static int optionalInteger(String name, int defaultValue) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
        }

        private static String required(String name) {
            String value = System.getenv(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("missing environment variable " + name);
            }
            return value;
        }
    }
}
