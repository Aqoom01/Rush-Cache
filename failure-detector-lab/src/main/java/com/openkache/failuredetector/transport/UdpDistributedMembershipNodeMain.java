package com.openkache.failuredetector.transport;

import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.NodeStatus;
import com.openkache.failuredetector.deadline.FixedDeadlineConfig;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.deadline.RingMonitorAssignment;
import com.openkache.failuredetector.experiment.VersionedMembershipView;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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

/** Real-UDP Ring-2 detector with per-node remove-only membership views. */
public final class UdpDistributedMembershipNodeMain {
    private static final long HEARTBEAT_INTERVAL_MILLIS = 200;
    private static final long PROBE_TIMEOUT_MILLIS = 100;

    private UdpDistributedMembershipNodeMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("control")) {
            sendControl(args);
            return;
        }
        if (args.length > 0 && args[0].equals("inject")) {
            injectCrash(args);
            return;
        }
        new Node(NodeConfig.fromEnvironment()).run();
    }

    private static void injectCrash(String[] args) throws IOException {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "inject requires all-hosts, port and crash-hosts");
        }
        try (DatagramSocket socket = new DatagramSocket()) {
            sendToHosts(socket, args[1], Integer.parseInt(args[2]),
                    UdpWireMessage.Type.FAULT_START);
            sendToHosts(socket, args[3], Integer.parseInt(args[2]),
                    UdpWireMessage.Type.PROCESS_CRASH);
        }
    }

    private static void sendControl(String[] args) throws IOException {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "control requires host, port and fault-start|finish");
        }
        UdpWireMessage.Type type = switch (args[3]) {
            case "fault-start" -> UdpWireMessage.Type.FAULT_START;
            case "finish" -> UdpWireMessage.Type.EXPERIMENT_FINISH;
            default -> throw new IllegalArgumentException("unknown control: " + args[3]);
        };
        try (DatagramSocket socket = new DatagramSocket()) {
            sendToHosts(socket, args[1], Integer.parseInt(args[2]), type);
        }
    }

    private static void sendToHosts(
            DatagramSocket socket,
            String hosts,
            int port,
            UdpWireMessage.Type type
    ) throws IOException {
        byte[] payload = new UdpWireMessage(type, 0, -1, -1, -1).encode();
        for (String host : hosts.split(",")) {
            socket.send(new DatagramPacket(
                    payload, payload.length, new InetSocketAddress(host, port)));
        }
    }

    private static final class Node implements AutoCloseable {
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
        private final FixedDeadlineFailureDetector detector;
        private final VersionedMembershipView view;
        private final Set<String> registeredTargets = new LinkedHashSet<>();
        private final Map<Long, Probe> probes = new HashMap<>();
        private final Map<Long, VerificationRound> verificationRounds = new HashMap<>();
        private final List<DetectorEvent> detectorEvents = new ArrayList<>();
        private final Map<Integer, Long> removalLatencies = new LinkedHashMap<>();
        private final Map<Integer, Long> localDeadLatencies = new LinkedHashMap<>();
        private final Map<Integer, Long> firstAssignedAfterFault = new LinkedHashMap<>();
        private final Set<Integer> monitoredAtFault = new LinkedHashSet<>();
        private final long startedAtNanos = System.nanoTime();
        private long sequence;
        private long logicalClock;
        private Long faultAtNanos;
        private long localRemovals;
        private long remoteRemovals;
        private long membershipControlFrames;
        private long verificationRoundSuccesses;
        private long verificationRoundFailures;
        private long verificationProbeSuccesses;
        private long verificationProbeFailures;

        private Node(NodeConfig config) throws SocketException {
            this.config = config;
            this.socket = new DatagramSocket(new InetSocketAddress("0.0.0.0", config.port()));
            this.state = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(
                        runnable, "udp-membership-state-" + config.nodeId());
                thread.setDaemon(false);
                return thread;
            });
            this.receiver = new Thread(
                    this::receiveLoop, "udp-membership-receiver-" + config.nodeId());
            this.receiver.setDaemon(true);
            this.detector = new FixedDeadlineFailureDetector(
                    nodeName(config.nodeId()), ringVerifyConfig(config.failedRoundsToDead()));
            this.view = new VersionedMembershipView(config.initialMembers());
            syncTargets(0, "initial-view");
        }

        private void run() throws InterruptedException {
            Runtime.getRuntime().addShutdownHook(
                    new Thread(this::close, "udp-membership-shutdown"));
            receiver.start();
            state.scheduleAtFixedRate(this::tick, 0, 50, TimeUnit.MILLISECONDS);
            state.scheduleAtFixedRate(
                    this::sendHeartbeats,
                    0,
                    HEARTBEAT_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS
            );
            if (config.durationSeconds() > 0) {
                state.schedule(this::finish, config.durationSeconds(), TimeUnit.SECONDS);
            }
            finished.await();
        }

        private static FixedDeadlineConfig ringVerifyConfig(int failedRoundsToDead) {
            return new FixedDeadlineConfig(
                    Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                    Duration.ofMillis(600),
                    Duration.ofMillis(400),
                    Duration.ofSeconds(3),
                    FixedDeadlineConfig.PeerSelectionMode.DETERMINISTIC_RING,
                    UdpWireMessage.WIRE_BYTES,
                    true,
                    Duration.ofMillis(PROBE_TIMEOUT_MILLIS),
                    failedRoundsToDead
            );
        }

        private void sendHeartbeats() {
            if (!running.get() || !view.contains(nodeName(config.nodeId()))) {
                return;
            }
            for (int recipient : heartbeatRecipients(
                    config.nodeId(), view.members(), config.neighborCount())) {
                send(recipient, new UdpWireMessage(
                        UdpWireMessage.Type.HEARTBEAT,
                        ++sequence,
                        config.nodeId(),
                        config.nodeId(),
                        recipient
                ));
            }
        }

        private void receiveLoop() {
            byte[] bytes = new byte[UdpWireMessage.WIRE_BYTES];
            while (running.get()) {
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
                try {
                    socket.receive(packet);
                    receivedFrames.incrementAndGet();
                    receivedBytes.addAndGet(packet.getLength());
                    UdpWireMessage message = UdpWireMessage.decode(
                            packet.getData(), packet.getLength());
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
                case MEMBERSHIP_REMOVE -> onMembershipRemove(message);
                case FAULT_START -> onFaultStart();
                case EXPERIMENT_FINISH -> finish();
                case PROCESS_CRASH -> Runtime.getRuntime().halt(137);
            }
        }

        private void onFaultStart() {
            if (faultAtNanos != null) {
                return;
            }
            faultAtNanos = elapsedNanos();
            registeredTargets.stream().map(UdpDistributedMembershipNodeMain::nodeId)
                    .forEach(monitoredAtFault::add);
            System.out.println("EVENT_JSON={\"kind\":\"fault\",\"atNanos\":"
                    + faultAtNanos + "}");
        }

        private void onHeartbeat(UdpWireMessage message) {
            String target = nodeName(message.sender());
            if (!registeredTargets.contains(target)) {
                return;
            }
            detector.recordHeartbeat(elapsedNanos(), target, 1);
            processDetectorEvents();
        }

        private void onAck(UdpWireMessage message) {
            Probe probe = probes.remove(message.sequence());
            if (probe == null || probe.targetId() != message.target()) {
                return;
            }
            completeVerificationProbe(probe, true);
        }

        private void onMembershipRemove(UdpWireMessage message) {
            logicalClock = Math.max(logicalClock, message.sequence());
            VersionedMembershipView.VersionStamp stamp =
                    new VersionedMembershipView.VersionStamp(
                            message.sequence(), nodeName(message.origin()));
            applyRemoval(message.target(), stamp, false);
        }

        private void tick() {
            if (!running.get()) {
                return;
            }
            detector.tick(elapsedNanos());
            processDetectorEvents();
            for (String target : List.copyOf(registeredTargets)) {
                List<Integer> helpers = verificationHelpers(
                        config.nodeId(), nodeId(target), view.members());
                if (helpers.size() < config.verificationHelperCount()) {
                    continue;
                }
                if (detector.beginVerification(target)) {
                    startVerificationRound(nodeId(target), helpers.subList(
                            0, config.verificationHelperCount()));
                }
            }
        }

        private void startVerificationRound(int targetId, List<Integer> helpers) {
            long roundId = ++sequence;
            verificationRounds.put(
                    roundId,
                    new VerificationRound(roundId, targetId, helpers.size())
            );
            for (int helper : helpers) {
                long probeId = ++sequence;
                probes.put(probeId, new Probe(probeId, roundId, targetId));
                send(helper, new UdpWireMessage(
                        UdpWireMessage.Type.PING_REQUEST,
                        probeId,
                        config.nodeId(),
                        config.nodeId(),
                        targetId
                ));
                state.schedule(
                        () -> onProbeTimeout(probeId),
                        PROBE_TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS
                );
            }
        }

        private void onProbeTimeout(long probeId) {
            Probe probe = probes.remove(probeId);
            if (probe != null) {
                completeVerificationProbe(probe, false);
            }
        }

        private void completeVerificationProbe(Probe probe, boolean reachable) {
            VerificationRound round = verificationRounds.get(probe.roundId());
            if (round == null) {
                return;
            }
            if (reachable) {
                verificationProbeSuccesses++;
            } else {
                verificationProbeFailures++;
            }
            round.complete(reachable);
            if (!round.complete()) {
                return;
            }
            verificationRounds.remove(round.id());
            boolean succeeded = round.failures() < config.verificationFailureQuorum();
            if (succeeded) {
                verificationRoundSuccesses++;
            } else {
                verificationRoundFailures++;
            }
            long now = elapsedNanos();
            detector.recordVerification(now, nodeName(round.targetId()), succeeded);
            System.out.println("EVENT_JSON={\"kind\":\"verification\",\"atNanos\":"
                    + now + ",\"target\":\"" + nodeName(round.targetId())
                    + "\",\"helperFailures\":" + round.failures()
                    + ",\"helperCount\":" + round.expected()
                    + ",\"succeeded\":" + succeeded + "}");
            processDetectorEvents();
        }

        private void processDetectorEvents() {
            List<DetectorEvent> events = detector.drainEvents();
            for (DetectorEvent event : events) {
                detectorEvents.add(event);
                System.out.println("EVENT_JSON={\"kind\":\"detector\",\"atNanos\":"
                        + event.atNanos() + ",\"observer\":\"" + event.observer()
                        + "\",\"target\":\"" + event.target()
                        + "\",\"oldStatus\":\"" + event.oldStatus()
                        + "\",\"newStatus\":\"" + event.newStatus()
                        + "\",\"reason\":\"" + escape(event.reason()) + "\"}");
                if (event.newStatus() == NodeStatus.DEAD && view.contains(event.target())) {
                    int targetId = nodeId(event.target());
                    if (faultAtNanos != null && event.atNanos() >= faultAtNanos) {
                        localDeadLatencies.putIfAbsent(
                                targetId, event.atNanos() - faultAtNanos);
                    }
                    VersionedMembershipView.VersionStamp stamp =
                            new VersionedMembershipView.VersionStamp(
                                    ++logicalClock, nodeName(config.nodeId()));
                    if (applyRemoval(targetId, stamp, true)) {
                        broadcastRemoval(targetId, stamp);
                    }
                }
            }
        }

        private boolean applyRemoval(
                int targetId,
                VersionedMembershipView.VersionStamp stamp,
                boolean local
        ) {
            String target = nodeName(targetId);
            VersionedMembershipView.ApplyOutcome outcome = view.applyRemoval(target, stamp);
            if (outcome != VersionedMembershipView.ApplyOutcome.MEMBER_REMOVED) {
                return false;
            }
            if (local) {
                localRemovals++;
            } else {
                remoteRemovals++;
            }
            if (faultAtNanos != null) {
                removalLatencies.putIfAbsent(
                        targetId, Math.max(0, elapsedNanos() - faultAtNanos));
            }
            syncTargets(elapsedNanos(), local ? "local-detector-dead" : "remote-dead-event");
            System.out.println("EVENT_JSON={\"kind\":\"membership\",\"atNanos\":"
                    + elapsedNanos() + ",\"removed\":\"" + target
                    + "\",\"source\":\"" + (local ? "local" : "remote")
                    + "\",\"viewVersion\":" + view.viewVersion()
                    + ",\"stampTime\":" + stamp.logicalTime()
                    + ",\"stampOrigin\":\"" + stamp.origin() + "\"}");
            return true;
        }

        private void broadcastRemoval(
                int targetId,
                VersionedMembershipView.VersionStamp stamp
        ) {
            for (int peer : config.peers().keySet()) {
                send(peer, new UdpWireMessage(
                        UdpWireMessage.Type.MEMBERSHIP_REMOVE,
                        stamp.logicalTime(),
                        config.nodeId(),
                        config.nodeId(),
                        targetId
                ));
                membershipControlFrames++;
            }
        }

        private void syncTargets(long nowNanos, String source) {
            Set<String> desired;
            if (view.contains(nodeName(config.nodeId()))) {
                desired = new LinkedHashSet<>(new RingMonitorAssignment(
                        view.members(), config.neighborCount())
                        .monitoredBy(nodeName(config.nodeId())));
            } else {
                desired = Set.of();
            }
            for (String target : List.copyOf(registeredTargets)) {
                if (!desired.contains(target)) {
                    detector.unregisterTarget(target);
                    registeredTargets.remove(target);
                }
            }
            for (String target : desired) {
                if (registeredTargets.add(target)) {
                    detector.registerTarget(nowNanos, target, 1);
                    if (faultAtNanos != null) {
                        firstAssignedAfterFault.putIfAbsent(
                                nodeId(target), Math.max(0, nowNanos - faultAtNanos));
                    }
                    System.out.println("EVENT_JSON={\"kind\":\"assignment\",\"atNanos\":"
                            + nowNanos + ",\"target\":\"" + target
                            + "\",\"source\":\"" + source + "\"}");
                }
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
            long preFaultDeadEvents = detectorEvents.stream()
                    .filter(event -> event.newStatus() == NodeStatus.DEAD)
                    .filter(event -> faultAtNanos == null || event.atNanos() < faultAtNanos)
                    .count();
            System.out.println("RESULT_JSON={"
                    + "\"profile\":\"ring2-distributed-h3-q3-r3\","
                    + "\"nodeId\":" + config.nodeId() + ","
                    + "\"neighborCount\":" + config.neighborCount() + ","
                    + "\"verificationHelperCount\":"
                    + config.verificationHelperCount() + ","
                    + "\"verificationFailureQuorum\":"
                    + config.verificationFailureQuorum() + ","
                    + "\"failedVerificationRoundsToDead\":"
                    + config.failedRoundsToDead() + ","
                    + "\"faultAtNanos\":" + nullable(faultAtNanos) + ","
                    + "\"preFaultDeadEvents\":" + preFaultDeadEvents + ","
                    + "\"viewVersion\":" + view.viewVersion() + ","
                    + "\"finalMembers\":\"" + memberIds(view.members()) + "\","
                    + "\"monitoredAtFault\":\"" + joinIds(monitoredAtFault) + "\","
                    + "\"firstAssignedAfterFault\":\""
                    + joinLatencies(firstAssignedAfterFault) + "\","
                    + "\"removalLatenciesNanos\":\""
                    + joinLatencies(removalLatencies) + "\","
                    + "\"localDeadLatenciesNanos\":\""
                    + joinLatencies(localDeadLatencies) + "\","
                    + "\"localRemovals\":" + localRemovals + ","
                    + "\"remoteRemovals\":" + remoteRemovals + ","
                    + "\"membershipControlFrames\":" + membershipControlFrames + ","
                    + "\"verificationRoundSuccesses\":"
                    + verificationRoundSuccesses + ","
                    + "\"verificationRoundFailures\":"
                    + verificationRoundFailures + ","
                    + "\"verificationProbeSuccesses\":"
                    + verificationProbeSuccesses + ","
                    + "\"verificationProbeFailures\":"
                    + verificationProbeFailures + ","
                    + "\"sentFrames\":" + sentFrames.get() + ","
                    + "\"sentBytes\":" + sentBytes.get() + ","
                    + "\"receivedFrames\":" + receivedFrames.get() + ","
                    + "\"receivedBytes\":" + receivedBytes.get() + ","
                    + "\"timeSource\":\"node-monotonic\","
                    + "\"wireSource\":\"real-udp-observed\""
                    + "}");
            socket.close();
            state.shutdownNow();
            finished.countDown();
        }

        @Override
        public void close() {
            finish();
        }
    }

    private record Probe(long id, long roundId, int targetId) {
    }

    private static final class VerificationRound {
        private final long id;
        private final int targetId;
        private final int expected;
        private int completed;
        private int failures;

        private VerificationRound(long id, int targetId, int expected) {
            this.id = id;
            this.targetId = targetId;
            this.expected = expected;
        }

        private void complete(boolean reachable) {
            completed++;
            if (!reachable) {
                failures++;
            }
        }

        private boolean complete() {
            return completed == expected;
        }

        private long id() {
            return id;
        }

        private int targetId() {
            return targetId;
        }

        private int expected() {
            return expected;
        }

        private int failures() {
            return failures;
        }
    }

    private record NodeConfig(
            int nodeId,
            int port,
            int durationSeconds,
            int neighborCount,
            int verificationHelperCount,
            int verificationFailureQuorum,
            int failedRoundsToDead,
            Map<Integer, InetSocketAddress> peers,
            List<String> initialMembers
    ) {
        private static NodeConfig fromEnvironment() {
            int nodeId = integer("FD_NODE_ID");
            int port = integer("FD_PORT");
            int duration = integer("FD_DURATION_SECONDS");
            int neighborCount = optionalInteger("FD_RING_NEIGHBOR_COUNT", 2);
            int helperCount = optionalInteger("FD_VERIFY_HELPER_COUNT", 3);
            int failureQuorum = optionalInteger("FD_VERIFY_FAILURE_QUORUM", 3);
            int failedRounds = optionalInteger("FD_VERIFY_FAILED_ROUNDS", 3);
            if (neighborCount < 1 || helperCount < 1 || failureQuorum < 1
                    || failureQuorum > helperCount || failedRounds < 1) {
                throw new IllegalArgumentException("invalid distributed UDP settings");
            }
            Map<Integer, InetSocketAddress> peers = new LinkedHashMap<>();
            List<String> members = new ArrayList<>();
            for (String item : required("FD_PEERS").split(",")) {
                String[] idAndAddress = item.split("=", 2);
                int id = Integer.parseInt(idAndAddress[0]);
                String[] hostAndPort = idAndAddress[1].split(":", 2);
                members.add(nodeName(id));
                if (id != nodeId) {
                    peers.put(id, new InetSocketAddress(
                            hostAndPort[0], Integer.parseInt(hostAndPort[1])));
                }
            }
            if (!members.contains(nodeName(nodeId)) || members.size() < 5
                    || neighborCount >= members.size()) {
                throw new IllegalArgumentException("invalid distributed UDP membership");
            }
            return new NodeConfig(
                    nodeId, port, duration, neighborCount, helperCount, failureQuorum,
                    failedRounds, Map.copyOf(peers), List.copyOf(members));
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

    static List<Integer> heartbeatRecipients(
            int nodeId,
            List<String> members,
            int neighborCount
    ) {
        String node = nodeName(nodeId);
        RingMonitorAssignment assignment = new RingMonitorAssignment(members, neighborCount);
        if (!assignment.members().contains(node)) {
            return List.of();
        }
        List<Integer> recipients = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : assignment.assignments().entrySet()) {
            if (entry.getValue().contains(node)) {
                recipients.add(nodeId(entry.getKey()));
            }
        }
        recipients.sort(Comparator.naturalOrder());
        return List.copyOf(recipients);
    }

    static List<Integer> verificationHelpers(
            int observerId,
            int targetId,
            List<String> members
    ) {
        return members.stream()
                .map(UdpDistributedMembershipNodeMain::nodeId)
                .filter(id -> id != observerId && id != targetId)
                .sorted()
                .toList();
    }

    private static String memberIds(List<String> members) {
        return joinIds(members.stream().map(UdpDistributedMembershipNodeMain::nodeId).toList());
    }

    private static String joinIds(Iterable<Integer> ids) {
        List<String> values = new ArrayList<>();
        ids.forEach(id -> values.add(Integer.toString(id)));
        return String.join(";", values);
    }

    private static String joinLatencies(Map<Integer, Long> values) {
        List<String> entries = values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .toList();
        return String.join(";", entries);
    }

    private static String nodeName(int nodeId) {
        return "node-" + nodeId;
    }

    private static int nodeId(String nodeName) {
        if (nodeName == null || !nodeName.startsWith("node-")) {
            throw new IllegalArgumentException("invalid node name: " + nodeName);
        }
        return Integer.parseInt(nodeName.substring("node-".length()));
    }

    private static String nullable(Long value) {
        return value == null ? "null" : value.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
