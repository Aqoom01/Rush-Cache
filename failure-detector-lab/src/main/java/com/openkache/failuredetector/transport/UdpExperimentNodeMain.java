package com.openkache.failuredetector.transport;

import com.openkache.failuredetector.adaptive.AdaptiveDeadlineConfig;
import com.openkache.failuredetector.adaptive.AdaptiveDeadlineFailureDetector;
import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.FailureDetector;
import com.openkache.failuredetector.deadline.FixedDeadlineConfig;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.experiment.DetectorProfile;
import com.openkache.failuredetector.phi.GossipHeartbeat;
import com.openkache.failuredetector.phi.PhiAccrualConfig;
import com.openkache.failuredetector.phi.PhiAccrualFailureDetector;
import com.openkache.failuredetector.swim.SwimLifeguardConfig;
import com.openkache.failuredetector.swim.SwimLifeguardDetector;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Multi-process real-UDP detector smoke node used by the Docker/netem runner. */
public final class UdpExperimentNodeMain {
    private static final int OBSERVER_ID = 0;
    private static final int TARGET_ID = 1;
    private static final int HELPER_ID = 2;

    private UdpExperimentNodeMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("control")) {
            sendFaultControl(args);
            return;
        }
        new Node(NodeConfig.fromEnvironment()).run();
    }

    private static void sendFaultControl(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException("control requires host and port");
        }
        UdpWireMessage message = new UdpWireMessage(
                UdpWireMessage.Type.FAULT_START, 0, -1, OBSERVER_ID, TARGET_ID);
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
        private static final long HEARTBEAT_INTERVAL_MILLIS = 200;
        private static final long PROBE_TIMEOUT_MILLIS = 100;

        private final NodeConfig config;
        private final DatagramSocket socket;
        private final ScheduledExecutorService state;
        private final Thread receiver;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicLong sentFrames = new AtomicLong();
        private final AtomicLong sentBytes = new AtomicLong();
        private final AtomicLong receivedFrames = new AtomicLong();
        private final AtomicLong receivedBytes = new AtomicLong();
        private final CountDownLatch finished = new CountDownLatch(1);
        private final Map<Long, Probe> probes = new HashMap<>();
        private final List<DetectorEvent> detectorEvents = new ArrayList<>();
        private final FailureDetector detector;
        private final long startedAtNanos = System.nanoTime();
        private long sequence;
        private Long faultAtNanos;

        private Node(NodeConfig config) throws SocketException {
            this.config = config;
            this.socket = new DatagramSocket(new InetSocketAddress("0.0.0.0", config.port()));
            this.state = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "udp-fd-state-" + config.nodeId());
                thread.setDaemon(false);
                return thread;
            });
            this.receiver = new Thread(this::receiveLoop, "udp-fd-receiver-" + config.nodeId());
            this.receiver.setDaemon(true);
            this.detector = createDetector(config.profile());
            if (detector instanceof FixedDeadlineFailureDetector fixed) {
                fixed.registerTarget(0, nodeName(TARGET_ID), 1);
            } else if (detector instanceof AdaptiveDeadlineFailureDetector adaptive) {
                adaptive.registerTarget(0, nodeName(TARGET_ID), 1);
            }
        }

        private void run() throws InterruptedException {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "udp-fd-shutdown"));
            receiver.start();
            state.scheduleAtFixedRate(this::tick, 0, 50, TimeUnit.MILLISECONDS);
            state.scheduleAtFixedRate(
                    this::sendProtocolTraffic,
                    0,
                    HEARTBEAT_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS
            );
            state.schedule(this::finish, config.durationSeconds(), TimeUnit.SECONDS);
            finished.await();
        }

        private FailureDetector createDetector(DetectorProfile profile) {
            if (config.nodeId() != OBSERVER_ID) {
                return null;
            }
            return switch (profile) {
                case F0_FIXED_DEADLINE -> new FixedDeadlineFailureDetector(
                        nodeName(OBSERVER_ID),
                        new FixedDeadlineConfig(
                                Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                                Duration.ofMillis(600),
                                Duration.ofMillis(400),
                                Duration.ofMillis(600),
                                FixedDeadlineConfig.PeerSelectionMode.FULL_MESH,
                                UdpWireMessage.WIRE_BYTES
                        )
                );
                case F4_ADAPTIVE_DEADLINE -> new AdaptiveDeadlineFailureDetector(
                        nodeName(OBSERVER_ID),
                        new AdaptiveDeadlineConfig(
                                Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                                5,
                                0.2,
                                Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                                Duration.ofMillis(100),
                                4,
                                Duration.ZERO,
                                Duration.ofMillis(400),
                                Duration.ofSeconds(2),
                                Duration.ofSeconds(2),
                                Duration.ofMillis(400),
                                Duration.ofMillis(600),
                                UdpWireMessage.WIRE_BYTES
                        )
                );
                case F1_PHI_GOSSIP -> new PhiAccrualFailureDetector(
                        nodeName(OBSERVER_ID),
                        new PhiAccrualConfig(
                                Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                                8,
                                100,
                                Duration.ofMillis(400),
                                Duration.ofMillis(400),
                                Duration.ofSeconds(1)
                        ),
                        profile.id()
                );
                case F2_SWIM_BASE -> new SwimLifeguardDetector(
                        nodeName(OBSERVER_ID), config.peers().size() + 1, swimConfig(false));
                case F3_SWIM_LIFEGUARD -> new SwimLifeguardDetector(
                        nodeName(OBSERVER_ID), config.peers().size() + 1, swimConfig(true));
                case F5_FIXED_DEADLINE_RING -> new FixedDeadlineFailureDetector(
                        nodeName(OBSERVER_ID), udpRingConfig(false));
                case F6_FIXED_DEADLINE_RING_VERIFY -> new FixedDeadlineFailureDetector(
                        nodeName(OBSERVER_ID), udpRingConfig(true));
                case F7A_SWIM_LHA_PROBE -> new SwimLifeguardDetector(
                        nodeName(OBSERVER_ID), config.peers().size() + 1,
                        udpSwimConfig(SwimLifeguardConfig.lhaProbeOnly()));
                case F7B_SWIM_LHA_SUSPICION -> new SwimLifeguardDetector(
                        nodeName(OBSERVER_ID), config.peers().size() + 1,
                        udpSwimConfig(SwimLifeguardConfig.lhaSuspicionOnly()));
                case F7C_SWIM_BUDDY -> throw new UnsupportedOperationException(
                        profile.id() + " is currently simulation-only; no UDP adapter is implemented");
            };
        }

        private static FixedDeadlineConfig udpRingConfig(boolean verify) {
            return new FixedDeadlineConfig(
                    Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                    Duration.ofMillis(600),
                    Duration.ofMillis(400),
                    Duration.ofMillis(600),
                    FixedDeadlineConfig.PeerSelectionMode.DETERMINISTIC_RING,
                    UdpWireMessage.WIRE_BYTES,
                    verify,
                    Duration.ofMillis(PROBE_TIMEOUT_MILLIS),
                    2
            );
        }

        private static SwimLifeguardConfig udpSwimConfig(SwimLifeguardConfig source) {
            return new SwimLifeguardConfig(
                    Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                    Duration.ofMillis(PROBE_TIMEOUT_MILLIS),
                    1,
                    source.suspicionMultiplier(),
                    source.suspicionMaxTimeoutMultiplier(),
                    1,
                    source.awarenessMaxMultiplier(),
                    source.lhaProbeEnabled(),
                    source.lhaSuspicionEnabled(),
                    source.suspicionConfirmationsEnabled(),
                    source.buddySystemEnabled()
            );
        }

        private static SwimLifeguardConfig swimConfig(boolean lifeguard) {
            return new SwimLifeguardConfig(
                    Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                    Duration.ofMillis(PROBE_TIMEOUT_MILLIS),
                    1,
                    4,
                    2,
                    1,
                    8,
                    lifeguard,
                    lifeguard
            );
        }

        private void sendProtocolTraffic() {
            if (!running.get()) {
                return;
            }
            if (config.profile() == DetectorProfile.F0_FIXED_DEADLINE
                    || config.profile() == DetectorProfile.F4_ADAPTIVE_DEADLINE
                    || config.profile() == DetectorProfile.F1_PHI_GOSSIP
                    || config.profile() == DetectorProfile.F5_FIXED_DEADLINE_RING
                    || config.profile() == DetectorProfile.F6_FIXED_DEADLINE_RING_VERIFY) {
                Iterable<Integer> heartbeatPeers = isRingProfile()
                        ? List.of(ringHeartbeatRecipient(
                                config.nodeId(), config.peers().size() + 1))
                        : config.peers().keySet();
                for (int peer : heartbeatPeers) {
                    send(peer, new UdpWireMessage(
                            UdpWireMessage.Type.HEARTBEAT,
                            ++sequence,
                            config.nodeId(),
                            config.nodeId(),
                            peer
                    ));
                }
                return;
            }
            int target = (config.nodeId() + 1) % (config.peers().size() + 1);
            long probeId = ++sequence;
            if (config.nodeId() == OBSERVER_ID) {
                probes.put(probeId, new Probe(probeId, ProbeStage.DIRECT));
            }
            send(target, new UdpWireMessage(
                    UdpWireMessage.Type.PING,
                    probeId,
                    config.nodeId(),
                    config.nodeId(),
                    target
            ));
            if (config.nodeId() == OBSERVER_ID) {
                state.schedule(
                        () -> onProbeTimeout(probeId),
                        PROBE_TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS
                );
            }
        }

        private void onProbeTimeout(long probeId) {
            Probe probe = probes.get(probeId);
            if (probe == null) {
                return;
            }
            if (probe.stage() == ProbeStage.VERIFY) {
                probes.remove(probeId);
                FixedDeadlineFailureDetector fixed = (FixedDeadlineFailureDetector) detector;
                fixed.recordVerification(elapsedNanos(), nodeName(TARGET_ID), false);
                drainEvents();
                return;
            }
            if (probe.stage() == ProbeStage.DIRECT) {
                probes.put(probeId, new Probe(probeId, ProbeStage.INDIRECT));
                send(HELPER_ID, new UdpWireMessage(
                        UdpWireMessage.Type.PING_REQUEST,
                        probeId,
                        config.nodeId(),
                        config.nodeId(),
                        TARGET_ID
                ));
                state.schedule(
                        () -> onProbeTimeout(probeId),
                        PROBE_TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS
                );
                return;
            }
            probes.remove(probeId);
            SwimLifeguardDetector swim = (SwimLifeguardDetector) detector;
            long now = elapsedNanos();
            swim.recordProbe(now, nodeName(TARGET_ID), false, 1);
            drainEvents();
        }

        private void receiveLoop() {
            byte[] bytes = new byte[UdpWireMessage.WIRE_BYTES];
            while (running.get()) {
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
                try {
                    socket.receive(packet);
                    receivedFrames.incrementAndGet();
                    receivedBytes.addAndGet(packet.getLength());
                    UdpWireMessage message = UdpWireMessage.decode(packet.getData(), packet.getLength());
                    state.execute(() -> handle(message));
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

        private void handle(UdpWireMessage message) {
            switch (message.type()) {
                case HEARTBEAT -> onHeartbeat(message);
                case PING -> send(message.origin(), new UdpWireMessage(
                        UdpWireMessage.Type.ACK,
                        message.sequence(),
                        config.nodeId(),
                        message.origin(),
                        config.nodeId()
                ));
                case ACK -> onAck(message);
                case PING_REQUEST -> send(message.target(), new UdpWireMessage(
                        UdpWireMessage.Type.PING,
                        message.sequence(),
                        config.nodeId(),
                        message.origin(),
                        message.target()
                ));
                case FAULT_START -> {
                    if (config.nodeId() == OBSERVER_ID && faultAtNanos == null) {
                        faultAtNanos = elapsedNanos();
                        System.out.println("EVENT_JSON={\"kind\":\"fault\",\"atNanos\":"
                                + faultAtNanos + ",\"target\":\"node-1\"}");
                    }
                }
            }
        }

        private void onHeartbeat(UdpWireMessage message) {
            if (config.nodeId() != OBSERVER_ID || message.sender() != TARGET_ID) {
                return;
            }
            long now = elapsedNanos();
            if (detector instanceof FixedDeadlineFailureDetector fixed) {
                fixed.recordHeartbeat(now, nodeName(TARGET_ID), 1);
            } else if (detector instanceof AdaptiveDeadlineFailureDetector adaptive) {
                adaptive.recordHeartbeat(now, nodeName(TARGET_ID), 1);
            } else if (detector instanceof PhiAccrualFailureDetector phi) {
                phi.recordHeartbeat(new GossipHeartbeat(
                        nodeName(TARGET_ID), 1, message.sequence(), now), true);
            }
            drainEvents();
        }

        private void onAck(UdpWireMessage message) {
            if (config.nodeId() != OBSERVER_ID || message.target() != TARGET_ID) {
                return;
            }
            Probe probe = probes.remove(message.sequence());
            if (probe == null) {
                return;
            }
            if (probe.stage() == ProbeStage.VERIFY) {
                FixedDeadlineFailureDetector fixed = (FixedDeadlineFailureDetector) detector;
                fixed.recordVerification(elapsedNanos(), nodeName(TARGET_ID), true);
                drainEvents();
                return;
            }
            SwimLifeguardDetector swim = (SwimLifeguardDetector) detector;
            swim.recordProbe(elapsedNanos(), nodeName(TARGET_ID), true, 1);
            drainEvents();
        }

        private void tick() {
            if (detector == null) {
                return;
            }
            detector.tick(elapsedNanos());
            drainEvents();
            if (detector instanceof FixedDeadlineFailureDetector fixed
                    && fixed.config().verifySuspectEnabled()
                    && fixed.beginVerification(nodeName(TARGET_ID))) {
                long probeId = ++sequence;
                probes.put(probeId, new Probe(probeId, ProbeStage.VERIFY));
                send(HELPER_ID, new UdpWireMessage(
                        UdpWireMessage.Type.PING_REQUEST,
                        probeId,
                        config.nodeId(),
                        config.nodeId(),
                        TARGET_ID
                ));
                state.schedule(
                        () -> onProbeTimeout(probeId),
                        PROBE_TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS
                );
            }
        }

        private boolean isRingProfile() {
            return config.profile() == DetectorProfile.F5_FIXED_DEADLINE_RING
                    || config.profile() == DetectorProfile.F6_FIXED_DEADLINE_RING_VERIFY;
        }

        private void drainEvents() {
            if (detector == null) {
                return;
            }
            for (DetectorEvent event : detector.drainEvents()) {
                detectorEvents.add(event);
                System.out.println("EVENT_JSON={\"kind\":\"detector\",\"atNanos\":"
                        + event.atNanos() + ",\"oldStatus\":\"" + event.oldStatus()
                        + "\",\"newStatus\":\"" + event.newStatus()
                        + "\",\"reason\":\"" + escape(event.reason()) + "\"}");
            }
        }

        private void send(int peer, UdpWireMessage message) {
            InetSocketAddress address = config.peers().get(peer);
            if (address == null) {
                return;
            }
            byte[] payload = message.encode();
            try {
                socket.send(new DatagramPacket(payload, payload.length, address));
                sentFrames.incrementAndGet();
                sentBytes.addAndGet(payload.length);
            } catch (IOException exception) {
                System.err.println("UDP send failed: " + exception.getMessage());
            }
        }

        private long elapsedNanos() {
            return System.nanoTime() - startedAtNanos;
        }

        private void finish() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            Long suspectAt = detectorEvents.stream()
                    .filter(event -> event.newStatus().name().equals("SUSPECT"))
                    .map(DetectorEvent::atNanos)
                    .findFirst()
                    .orElse(null);
            Long deadAt = detectorEvents.stream()
                    .filter(event -> event.newStatus().name().equals("DEAD"))
                    .map(DetectorEvent::atNanos)
                    .findFirst()
                    .orElse(null);
            System.out.println("RESULT_JSON={"
                    + "\"profile\":\"" + config.profile().id() + "\","
                    + "\"nodeId\":" + config.nodeId() + ","
                    + "\"timeSource\":\"observer-monotonic\","
                    + "\"wireSource\":\"real-udp-observed\","
                    + "\"faultAtNanos\":" + nullable(faultAtNanos) + ","
                    + "\"suspectLatencyNanos\":" + latency(suspectAt) + ","
                    + "\"deadLatencyNanos\":" + latency(deadAt) + ","
                    + "\"sentFrames\":" + sentFrames.get() + ","
                    + "\"sentBytes\":" + sentBytes.get() + ","
                    + "\"receivedFrames\":" + receivedFrames.get() + ","
                    + "\"receivedBytes\":" + receivedBytes.get()
                    + "}");
            socket.close();
            state.shutdownNow();
            finished.countDown();
        }

        private String latency(Long eventAt) {
            if (eventAt == null || faultAtNanos == null || eventAt < faultAtNanos) {
                return "null";
            }
            return Long.toString(eventAt - faultAtNanos);
        }

        @Override
        public void close() {
            finish();
        }
    }

    private record Probe(long id, ProbeStage stage) {
    }

    private enum ProbeStage {
        DIRECT,
        INDIRECT,
        VERIFY
    }

    private record NodeConfig(
            int nodeId,
            int port,
            DetectorProfile profile,
            int durationSeconds,
            Map<Integer, InetSocketAddress> peers
    ) {
        private static NodeConfig fromEnvironment() {
            int nodeId = integer("FD_NODE_ID");
            int port = integer("FD_PORT");
            int duration = integer("FD_DURATION_SECONDS");
            DetectorProfile profile = DetectorProfile.fromId(required("FD_PROFILE"));
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
            return new NodeConfig(nodeId, port, profile, duration, Map.copyOf(peers));
        }

        private static int integer(String name) {
            return Integer.parseInt(required(name));
        }

        private static String required(String name) {
            String value = System.getenv(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("missing environment variable " + name);
            }
            return value;
        }
    }

    private static String nodeName(int nodeId) {
        return "node-" + nodeId;
    }

    /** A node sends its liveness heartbeat to the predecessor that monitors it. */
    static int ringHeartbeatRecipient(int nodeId, int nodeCount) {
        if (nodeCount < 2 || nodeId < 0 || nodeId >= nodeCount) {
            throw new IllegalArgumentException("invalid ring node");
        }
        return (nodeId - 1 + nodeCount) % nodeCount;
    }

    private static String nullable(Long value) {
        return value == null ? "null" : value.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
