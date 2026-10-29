package com.openkache.failuredetector.transport;

import com.openkache.failuredetector.adaptive.AdaptiveDeadlineConfig;
import com.openkache.failuredetector.adaptive.AdaptiveDeadlineFailureDetector;
import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.FailureDetector;
import com.openkache.failuredetector.core.NodeStatus;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final long SWIM_PROTOCOL_WARMUP_MILLIS = 3_000;

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
        private final Map<Long, ConfirmationProbe> confirmationProbes = new HashMap<>();
        private final Map<Long, VerificationRound> verificationRounds = new HashMap<>();
        private final Map<Integer, Long> knownIncarnations = new HashMap<>();
        private final Set<SuspicionRound> seenSuspicionRounds = new HashSet<>();
        private final Set<SuspicionRound> reportedSuspicionRounds = new HashSet<>();
        private final List<DetectorEvent> detectorEvents = new ArrayList<>();
        private final FailureDetector detector;
        private final long startedAtNanos = System.nanoTime();
        private long sequence;
        private Long faultAtNanos;
        private long nextVerificationAtNanos;
        private long verificationRoundSuccesses;
        private long verificationRoundFailures;
        private long verificationProbeSuccesses;
        private long verificationProbeFailures;
        private long suspicionGossipFrames;
        private long aliveGossipFrames;
        private long independentConfirmations;
        private long confirmationProbeSuccesses;
        private long confirmationProbeFailures;
        private long localIncarnation = 1;

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
            if (config.nodeId() == OBSERVER_ID
                    && config.profile() == DetectorProfile.F6_FIXED_DEADLINE_RING_VERIFY) {
                verificationHelpers(
                        OBSERVER_ID,
                        TARGET_ID,
                        config.peers().keySet(),
                        config.verificationHelperCount());
            }
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
                    protocolInitialDelayMillis(config.profile()),
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
                        nodeName(OBSERVER_ID), config.peers().size() + 1,
                        swimConfig(false, config.peers().size() + 1));
                case F3_SWIM_LIFEGUARD -> new SwimLifeguardDetector(
                        nodeName(OBSERVER_ID), config.peers().size() + 1,
                        swimConfig(true, config.peers().size() + 1));
                case F5_FIXED_DEADLINE_RING -> new FixedDeadlineFailureDetector(
                        nodeName(OBSERVER_ID), udpRingConfig(false, 2));
                case F6_FIXED_DEADLINE_RING_VERIFY -> new FixedDeadlineFailureDetector(
                        nodeName(OBSERVER_ID),
                        udpRingConfig(true, config.failedVerificationRoundsToDead()));
                case F7A_SWIM_LHA_PROBE -> new SwimLifeguardDetector(
                        nodeName(OBSERVER_ID), config.peers().size() + 1,
                        udpSwimConfig(
                                SwimLifeguardConfig.lhaProbeOnly(),
                                config.peers().size() + 1));
                case F7B_SWIM_LHA_SUSPICION -> new SwimLifeguardDetector(
                        nodeName(OBSERVER_ID), config.peers().size() + 1,
                        udpSwimConfig(
                                SwimLifeguardConfig.lhaSuspicionOnly(),
                                config.peers().size() + 1));
                case F7C_SWIM_BUDDY -> throw new UnsupportedOperationException(
                        profile.id() + " is currently simulation-only; no UDP adapter is implemented");
            };
        }

        private static FixedDeadlineConfig udpRingConfig(
                boolean verify,
                int failedVerificationRoundsToDead
        ) {
            return new FixedDeadlineConfig(
                    Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS),
                    Duration.ofMillis(600),
                    Duration.ofMillis(400),
                    Duration.ofSeconds(3),
                    FixedDeadlineConfig.PeerSelectionMode.DETERMINISTIC_RING,
                    UdpWireMessage.WIRE_BYTES,
                    verify,
                    Duration.ofMillis(PROBE_TIMEOUT_MILLIS),
                    failedVerificationRoundsToDead
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
                        ? ringHeartbeatRecipients(
                                config.nodeId(), config.peers().size() + 1, 2)
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
            if (config.nodeId() == OBSERVER_ID
                    && detector instanceof SwimLifeguardDetector swim
                    && (swim.status(nodeName(target)) != NodeStatus.ALIVE
                    || probes.values().stream().anyMatch(probe ->
                            probe.stage() == ProbeStage.DIRECT
                                    || probe.stage() == ProbeStage.INDIRECT))) {
                return;
            }
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
                completeVerificationProbe(probe, false);
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
            long incarnation = knownIncarnation(TARGET_ID);
            swim.recordProbe(now, nodeName(TARGET_ID), false, incarnation);
            drainEvents();
            if (confirmationGossipEnabled()) {
                broadcastSuspect(TARGET_ID, incarnation);
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
                case SUSPECT -> onSuspect(message);
                case ALIVE -> onAlive(message);
                case MEMBERSHIP_REMOVE, EXPERIMENT_FINISH, PROCESS_CRASH -> {
                    // Reserved for the distributed-membership UDP harness.
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
            ConfirmationProbe confirmation = confirmationProbes.remove(message.sequence());
            if (confirmation != null && message.target() == confirmation.targetId()) {
                confirmationProbeSuccesses++;
                sendAliveEvidence(
                        confirmation.reporterId(),
                        confirmation.targetId(),
                        confirmation.incarnation());
                return;
            }
            if (config.nodeId() != OBSERVER_ID || message.target() != TARGET_ID) {
                return;
            }
            Probe probe = probes.remove(message.sequence());
            if (probe == null) {
                return;
            }
            if (probe.stage() == ProbeStage.VERIFY) {
                completeVerificationProbe(probe, true);
                return;
            }
            SwimLifeguardDetector swim = (SwimLifeguardDetector) detector;
            swim.recordProbe(
                    elapsedNanos(),
                    nodeName(TARGET_ID),
                    true,
                    knownIncarnation(TARGET_ID));
            drainEvents();
        }

        private void onSuspect(UdpWireMessage message) {
            if (!confirmationGossipEnabled() || message.sequence() < 1
                    || message.target() < 0 || message.origin() < 0
                    || message.origin() == message.target()) {
                return;
            }
            int targetId = message.target();
            long incarnation = message.sequence();
            knownIncarnations.merge(targetId, incarnation, Math::max);

            if (targetId == config.nodeId()) {
                localIncarnation = refutationIncarnation(localIncarnation, incarnation);
                knownIncarnations.put(targetId, localIncarnation);
                broadcastAlive(targetId, localIncarnation);
                return;
            }

            if (detector instanceof SwimLifeguardDetector swim) {
                long previousDeadline = swim.suspicionDeadlineNanos(nodeName(targetId));
                swim.suspect(
                        elapsedNanos(),
                        nodeName(targetId),
                        nodeName(message.origin()),
                        incarnation);
                long updatedDeadline = swim.suspicionDeadlineNanos(nodeName(targetId));
                if (previousDeadline > 0 && updatedDeadline > 0
                        && updatedDeadline < previousDeadline) {
                    independentConfirmations++;
                    System.out.println("EVENT_JSON={\"kind\":\"swim-confirmation\","
                            + "\"atNanos\":" + elapsedNanos()
                            + ",\"target\":\"" + nodeName(targetId)
                            + "\",\"reporter\":\"" + nodeName(message.origin())
                            + "\",\"oldDeadlineNanos\":" + previousDeadline
                            + ",\"newDeadlineNanos\":" + updatedDeadline + "}");
                }
                drainEvents();
            }

            SuspicionRound round = new SuspicionRound(targetId, incarnation);
            if (seenSuspicionRounds.add(round)) {
                startConfirmationProbe(message.origin(), targetId, incarnation);
            }
        }

        private void onAlive(UdpWireMessage message) {
            if (!confirmationGossipEnabled() || message.sequence() < 1
                    || message.target() < 0 || message.origin() != message.target()) {
                return;
            }
            int targetId = message.target();
            long incarnation = message.sequence();
            knownIncarnations.merge(targetId, incarnation, Math::max);
            seenSuspicionRounds.removeIf(round -> round.targetId() == targetId
                    && round.incarnation() <= incarnation);
            reportedSuspicionRounds.removeIf(round -> round.targetId() == targetId
                    && round.incarnation() <= incarnation);
            if (detector instanceof SwimLifeguardDetector swim) {
                swim.recordProbe(elapsedNanos(), nodeName(targetId), true, incarnation);
                drainEvents();
            }
        }

        private void startConfirmationProbe(
                int reporterId,
                int targetId,
                long incarnation
        ) {
            long probeId = ++sequence;
            confirmationProbes.put(probeId, new ConfirmationProbe(
                    probeId,
                    reporterId,
                    targetId,
                    incarnation,
                    ConfirmationProbeStage.DIRECT));
            send(targetId, new UdpWireMessage(
                    UdpWireMessage.Type.PING,
                    probeId,
                    config.nodeId(),
                    config.nodeId(),
                    targetId));
            state.schedule(
                    () -> onConfirmationProbeTimeout(probeId),
                    PROBE_TIMEOUT_MILLIS,
                    TimeUnit.MILLISECONDS);
        }

        private void onConfirmationProbeTimeout(long probeId) {
            ConfirmationProbe probe = confirmationProbes.get(probeId);
            if (probe == null) {
                return;
            }
            if (probe.stage() == ConfirmationProbeStage.DIRECT) {
                int helper = indirectHelper(
                        config.nodeId(), probe.targetId(), config.peers().keySet());
                if (helper >= 0) {
                    confirmationProbes.put(probeId, probe.withStage(
                            ConfirmationProbeStage.INDIRECT));
                    send(helper, new UdpWireMessage(
                            UdpWireMessage.Type.PING_REQUEST,
                            probeId,
                            config.nodeId(),
                            config.nodeId(),
                            probe.targetId()));
                    state.schedule(
                            () -> onConfirmationProbeTimeout(probeId),
                            PROBE_TIMEOUT_MILLIS,
                            TimeUnit.MILLISECONDS);
                    return;
                }
            }
            confirmationProbes.remove(probeId);
            confirmationProbeFailures++;
            broadcastSuspect(probe.targetId(), probe.incarnation());
        }

        private void broadcastSuspect(int targetId, long incarnation) {
            SuspicionRound round = new SuspicionRound(targetId, incarnation);
            if (!reportedSuspicionRounds.add(round)) {
                return;
            }
            seenSuspicionRounds.add(round);
            for (int peer : config.peers().keySet()) {
                suspicionGossipFrames++;
                send(peer, new UdpWireMessage(
                        UdpWireMessage.Type.SUSPECT,
                        incarnation,
                        config.nodeId(),
                        config.nodeId(),
                        targetId));
            }
        }

        private void broadcastAlive(int targetId, long incarnation) {
            for (int peer : config.peers().keySet()) {
                aliveGossipFrames++;
                send(peer, new UdpWireMessage(
                        UdpWireMessage.Type.ALIVE,
                        incarnation,
                        config.nodeId(),
                        targetId,
                        targetId));
            }
        }

        private void sendAliveEvidence(int reporterId, int targetId, long incarnation) {
            aliveGossipFrames++;
            send(reporterId, new UdpWireMessage(
                    UdpWireMessage.Type.ALIVE,
                    incarnation,
                    config.nodeId(),
                    targetId,
                    targetId));
        }

        private long knownIncarnation(int targetId) {
            return knownIncarnations.getOrDefault(targetId, 1L);
        }

        private void tick() {
            if (detector == null) {
                return;
            }
            detector.tick(elapsedNanos());
            drainEvents();
            long now = elapsedNanos();
            if (detector instanceof FixedDeadlineFailureDetector fixed
                    && fixed.config().verifySuspectEnabled()
                    && now >= nextVerificationAtNanos
                    && fixed.beginVerification(nodeName(TARGET_ID))) {
                startVerificationRound();
            }
        }

        private void startVerificationRound() {
            List<Integer> helpers = verificationHelpers(
                    config.nodeId(), TARGET_ID, config.peers().keySet(),
                    config.verificationHelperCount());
            long roundId = ++sequence;
            verificationRounds.put(
                    roundId,
                    new VerificationRound(roundId, helpers.size()));
            for (int helper : helpers) {
                long probeId = ++sequence;
                probes.put(probeId, new Probe(probeId, ProbeStage.VERIFY, roundId));
                send(helper, new UdpWireMessage(
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

        private void completeVerificationProbe(Probe probe, boolean reachable) {
            VerificationRound round = verificationRounds.get(probe.verificationRoundId());
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
            boolean verificationSucceeded = round.failures()
                    < config.verificationFailureQuorum();
            if (verificationSucceeded) {
                verificationRoundSuccesses++;
            } else {
                verificationRoundFailures++;
            }
            FixedDeadlineFailureDetector fixed = (FixedDeadlineFailureDetector) detector;
            long now = elapsedNanos();
            fixed.recordVerification(now, nodeName(TARGET_ID), verificationSucceeded);
            nextVerificationAtNanos = now + fixed.config().verificationInterval().toNanos();
            System.out.println("EVENT_JSON={\"kind\":\"verification\",\"atNanos\":"
                    + now + ",\"helperFailures\":" + round.failures()
                    + ",\"helperCount\":" + config.verificationHelperCount()
                    + ",\"succeeded\":" + verificationSucceeded + "}");
            drainEvents();
        }

        private boolean isRingProfile() {
            return config.profile() == DetectorProfile.F5_FIXED_DEADLINE_RING
                    || config.profile() == DetectorProfile.F6_FIXED_DEADLINE_RING_VERIFY;
        }

        private boolean confirmationGossipEnabled() {
            return config.profile() == DetectorProfile.F3_SWIM_LIFEGUARD
                    || config.profile() == DetectorProfile.F7B_SWIM_LHA_SUSPICION;
        }

        private int configuredSuspicionMaxConfirmations() {
            return detector instanceof SwimLifeguardDetector swim
                    ? swim.config().maxConfirmations() : 0;
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
            Long suspectAt = firstEventAtOrAfterFault("SUSPECT");
            Long deadAt = firstEventAtOrAfterFault("DEAD");
            long preFaultDeadEvents = detectorEvents.stream()
                    .filter(event -> event.newStatus().name().equals("DEAD"))
                    .filter(event -> faultAtNanos == null || event.atNanos() < faultAtNanos)
                    .count();
            System.out.println("RESULT_JSON={"
                    + "\"profile\":\"" + config.profile().id() + "\","
                    + "\"nodeId\":" + config.nodeId() + ","
                    + "\"verificationHelperCount\":"
                    + config.verificationHelperCount() + ","
                    + "\"verificationFailureQuorum\":"
                    + config.verificationFailureQuorum() + ","
                    + "\"failedVerificationRoundsToDead\":"
                    + config.failedVerificationRoundsToDead() + ","
                    + "\"verificationRoundSuccesses\":"
                    + verificationRoundSuccesses + ","
                    + "\"verificationRoundFailures\":"
                    + verificationRoundFailures + ","
                    + "\"verificationProbeSuccesses\":"
                    + verificationProbeSuccesses + ","
                    + "\"verificationProbeFailures\":"
                    + verificationProbeFailures + ","
                    + "\"suspicionGossipFrames\":" + suspicionGossipFrames + ","
                    + "\"aliveGossipFrames\":" + aliveGossipFrames + ","
                    + "\"independentConfirmations\":" + independentConfirmations + ","
                    + "\"confirmationProbeSuccesses\":"
                    + confirmationProbeSuccesses + ","
                    + "\"confirmationProbeFailures\":"
                    + confirmationProbeFailures + ","
                    + "\"protocolWarmupMillis\":"
                    + protocolInitialDelayMillis(config.profile()) + ","
                    + "\"suspicionMaxConfirmations\":"
                    + configuredSuspicionMaxConfirmations() + ","
                    + "\"timeSource\":\"observer-monotonic\","
                    + "\"wireSource\":\"real-udp-observed\","
                    + "\"faultAtNanos\":" + nullable(faultAtNanos) + ","
                    + "\"preFaultDeadEvents\":" + preFaultDeadEvents + ","
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

        private Long firstEventAtOrAfterFault(String status) {
            if (faultAtNanos == null) {
                return null;
            }
            return detectorEvents.stream()
                    .filter(event -> event.newStatus().name().equals(status))
                    .map(DetectorEvent::atNanos)
                    .filter(atNanos -> atNanos >= faultAtNanos)
                    .findFirst()
                    .orElse(null);
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

    private record Probe(long id, ProbeStage stage, Long verificationRoundId) {
        private Probe(long id, ProbeStage stage) {
            this(id, stage, null);
        }
    }

    private record ConfirmationProbe(
            long id,
            int reporterId,
            int targetId,
            long incarnation,
            ConfirmationProbeStage stage
    ) {
        private ConfirmationProbe withStage(ConfirmationProbeStage nextStage) {
            return new ConfirmationProbe(
                    id, reporterId, targetId, incarnation, nextStage);
        }
    }

    private record SuspicionRound(int targetId, long incarnation) {
    }

    private static final class VerificationRound {
        private final long id;
        private final int expected;
        private int completed;
        private int failures;

        private VerificationRound(long id, int expected) {
            this.id = id;
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

        private int failures() {
            return failures;
        }
    }

    private enum ProbeStage {
        DIRECT,
        INDIRECT,
        VERIFY
    }

    private enum ConfirmationProbeStage {
        DIRECT,
        INDIRECT
    }

    private record NodeConfig(
            int nodeId,
            int port,
            DetectorProfile profile,
            int durationSeconds,
            int verificationHelperCount,
            int verificationFailureQuorum,
            int failedVerificationRoundsToDead,
            Map<Integer, InetSocketAddress> peers
    ) {
        private static NodeConfig fromEnvironment() {
            int nodeId = integer("FD_NODE_ID");
            int port = integer("FD_PORT");
            int duration = integer("FD_DURATION_SECONDS");
            int helperCount = optionalInteger("FD_VERIFY_HELPER_COUNT", 1);
            int failureQuorum = optionalInteger("FD_VERIFY_FAILURE_QUORUM", 1);
            int failedRoundsToDead = optionalInteger("FD_VERIFY_FAILED_ROUNDS", 2);
            if (helperCount < 1 || failureQuorum < 1 || failureQuorum > helperCount
                    || failedRoundsToDead < 1) {
                throw new IllegalArgumentException("invalid UDP verification settings");
            }
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
            return new NodeConfig(
                    nodeId, port, profile, duration, helperCount, failureQuorum,
                    failedRoundsToDead, Map.copyOf(peers));
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

    static SwimLifeguardConfig udpSwimConfig(
            SwimLifeguardConfig source,
            int clusterSize
    ) {
        return new SwimLifeguardConfig(
                Duration.ofMillis(Node.HEARTBEAT_INTERVAL_MILLIS),
                Duration.ofMillis(Node.PROBE_TIMEOUT_MILLIS),
                1,
                source.suspicionMultiplier(),
                source.suspicionMaxTimeoutMultiplier(),
                boundedConfirmationCount(clusterSize, source.maxConfirmations()),
                source.awarenessMaxMultiplier(),
                source.lhaProbeEnabled(),
                source.lhaSuspicionEnabled(),
                source.suspicionConfirmationsEnabled(),
                source.buddySystemEnabled()
        );
    }

    static SwimLifeguardConfig swimConfig(boolean lifeguard, int clusterSize) {
        return new SwimLifeguardConfig(
                Duration.ofMillis(Node.HEARTBEAT_INTERVAL_MILLIS),
                Duration.ofMillis(Node.PROBE_TIMEOUT_MILLIS),
                1,
                4,
                2,
                lifeguard ? boundedConfirmationCount(clusterSize, 3) : 0,
                8,
                lifeguard,
                lifeguard
        );
    }

    static int boundedConfirmationCount(int clusterSize, int requested) {
        return Math.min(requested, Math.max(0, clusterSize - 2));
    }

    private static String nodeName(int nodeId) {
        return "node-" + nodeId;
    }

    /** A node sends its liveness heartbeat to the predecessor that monitors it. */
    static int ringHeartbeatRecipient(int nodeId, int nodeCount) {
        return ringHeartbeatRecipients(nodeId, nodeCount, 1).get(0);
    }

    static List<Integer> ringHeartbeatRecipients(
            int nodeId,
            int nodeCount,
            int neighborCount
    ) {
        if (nodeCount < 2 || nodeId < 0 || nodeId >= nodeCount) {
            throw new IllegalArgumentException("invalid ring node");
        }
        if (neighborCount < 1 || neighborCount >= nodeCount) {
            throw new IllegalArgumentException("invalid Ring neighbor count");
        }
        List<Integer> recipients = new ArrayList<>();
        for (int offset = 1; offset <= neighborCount; offset++) {
            recipients.add((nodeId - offset + nodeCount) % nodeCount);
        }
        return List.copyOf(recipients);
    }

    static List<Integer> verificationHelpers(
            int observerId,
            int targetId,
            Iterable<Integer> peerIds,
            int helperCount
    ) {
        if (helperCount < 1) {
            throw new IllegalArgumentException("verification helper count must be positive");
        }
        List<Integer> helpers = new ArrayList<>();
        for (int peerId : peerIds) {
            if (peerId != observerId && peerId != targetId) {
                helpers.add(peerId);
            }
        }
        helpers.sort(Comparator.naturalOrder());
        if (helpers.size() < helperCount) {
            throw new IllegalArgumentException("not enough distinct verification helpers");
        }
        return List.copyOf(helpers.subList(0, helperCount));
    }

    static int indirectHelper(
            int observerId,
            int targetId,
            Iterable<Integer> peerIds
    ) {
        List<Integer> candidates = new ArrayList<>();
        for (int peerId : peerIds) {
            if (peerId != observerId && peerId != targetId) {
                candidates.add(peerId);
            }
        }
        candidates.sort(Comparator.naturalOrder());
        return candidates.isEmpty() ? -1 : candidates.get(0);
    }

    static long refutationIncarnation(long current, long suspected) {
        if (current < 1 || suspected < 1) {
            throw new IllegalArgumentException("incarnations must be positive");
        }
        return suspected >= current ? suspected + 1 : current;
    }

    static long protocolInitialDelayMillis(DetectorProfile profile) {
        return switch (profile) {
            case F2_SWIM_BASE, F3_SWIM_LIFEGUARD,
                    F7A_SWIM_LHA_PROBE, F7B_SWIM_LHA_SUSPICION ->
                    SWIM_PROTOCOL_WARMUP_MILLIS;
            default -> 0;
        };
    }

    private static String nullable(Long value) {
        return value == null ? "null" : value.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
