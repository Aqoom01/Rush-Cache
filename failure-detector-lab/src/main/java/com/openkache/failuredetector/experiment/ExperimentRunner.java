package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.adaptive.AdaptiveDeadlineConfig;
import com.openkache.failuredetector.adaptive.AdaptiveDeadlineFailureDetector;
import com.openkache.failuredetector.core.DetectorEvent;
import com.openkache.failuredetector.core.FailureDetector;
import com.openkache.failuredetector.core.GroundTruthEvent;
import com.openkache.failuredetector.core.ProtocolCost;
import com.openkache.failuredetector.deadline.FixedDeadlineConfig;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.phi.GossipHeartbeat;
import com.openkache.failuredetector.phi.PhiAccrualConfig;
import com.openkache.failuredetector.phi.PhiAccrualFailureDetector;
import com.openkache.failuredetector.swim.SwimLifeguardConfig;
import com.openkache.failuredetector.swim.SwimLifeguardDetector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ExperimentRunner {
    public static final String OBSERVER = "node-0";
    public static final String TARGET = "node-1";
    public static final String REALISTIC_DEFAULT = "realistic-default";
    public static final String MATCHED_ACTIVITY = "matched-activity";
    private static final int STREAM_SWIM = 1;
    private static final int STREAM_ALL_TO_ALL_HEARTBEAT = 2;
    private static final int STREAM_GOSSIP_HEARTBEAT = 3;
    private static final int STREAM_RING_HEARTBEAT = 4;

    private static final long[] FAULT_OFFSETS_NANOS = {
            0L, 137_000_000L, 491_000_000L, 809_000_000L
    };

    /** Preserves the original two-candidate API. */
    public List<ExperimentResult> runBoth(ScenarioConfig scenario, long seed) {
        return List.of(runSwimLifeguard(scenario, seed), runPhiGossip(scenario, seed));
    }

    public List<ExperimentResult> runAllProfiles(ScenarioConfig scenario, long seed) {
        return runProfiles(scenario, seed, List.of(DetectorProfile.values()), REALISTIC_DEFAULT);
    }

    public List<ExperimentResult> runProfiles(
            ScenarioConfig scenario,
            long seed,
            List<DetectorProfile> profiles,
            String comparisonMode
    ) {
        List<ExperimentResult> results = new ArrayList<>();
        for (DetectorProfile profile : profiles) {
            results.add(switch (profile) {
                case F0_FIXED_DEADLINE -> runFixedDeadline(
                        scenario, seed, FixedDeadlineConfig.defaults(), profile.id(), comparisonMode);
                case F1_PHI_GOSSIP -> runPhi(
                        scenario, seed, PhiAccrualConfig.defaults(), profile.id(), comparisonMode, true);
                case F2_SWIM_BASE -> runSwim(
                        scenario, seed, SwimLifeguardConfig.swimBase(), profile.id(), comparisonMode);
                case F3_SWIM_LIFEGUARD -> runSwim(
                        scenario, seed, SwimLifeguardConfig.defaults(), profile.id(), comparisonMode);
                case F4_ADAPTIVE_DEADLINE -> runAdaptiveDeadline(
                        scenario, seed, AdaptiveDeadlineConfig.defaults(), profile.id(), comparisonMode);
                case F5_FIXED_DEADLINE_RING -> runFixedDeadline(
                        scenario, seed, FixedDeadlineConfig.ring(), profile.id(), comparisonMode);
                case F6_FIXED_DEADLINE_RING_VERIFY -> runFixedDeadline(
                        scenario, seed, FixedDeadlineConfig.ringVerified(), profile.id(), comparisonMode);
                case F7A_SWIM_LHA_PROBE -> runSwim(
                        scenario, seed, SwimLifeguardConfig.lhaProbeOnly(), profile.id(), comparisonMode);
                case F7B_SWIM_LHA_SUSPICION -> runSwim(
                        scenario, seed, SwimLifeguardConfig.lhaSuspicionOnly(), profile.id(), comparisonMode);
                case F7C_SWIM_BUDDY -> runSwim(
                        scenario, seed, SwimLifeguardConfig.buddyOnly(), profile.id(), comparisonMode);
            });
        }
        return List.copyOf(results);
    }

    public ExperimentResult runSwimLifeguard(ScenarioConfig scenario, long seed) {
        return runSwimLifeguard(
                scenario, seed, SwimLifeguardConfig.defaults(), SwimLifeguardDetector.ALGORITHM);
    }

    public ExperimentResult runSwimLifeguard(
            ScenarioConfig scenario,
            long seed,
            SwimLifeguardConfig detectorConfig,
            String resultLabel
    ) {
        return runSwim(scenario, seed, detectorConfig, resultLabel, REALISTIC_DEFAULT);
    }

    public ExperimentResult runSwimBase(ScenarioConfig scenario, long seed) {
        return runSwim(
                scenario,
                seed,
                SwimLifeguardConfig.swimBase(),
                DetectorProfile.F2_SWIM_BASE.id(),
                REALISTIC_DEFAULT
        );
    }

    public ExperimentResult runFixedDeadline(ScenarioConfig scenario, long seed) {
        return runFixedDeadline(
                scenario,
                seed,
                FixedDeadlineConfig.defaults(),
                FixedDeadlineFailureDetector.ALGORITHM,
                REALISTIC_DEFAULT
        );
    }

    public ExperimentResult runPhiGossip(ScenarioConfig scenario, long seed) {
        return runPhiGossip(
                scenario, seed, PhiAccrualConfig.defaults(), PhiAccrualFailureDetector.ALGORITHM);
    }

    public ExperimentResult runPhiGossip(
            ScenarioConfig scenario,
            long seed,
            PhiAccrualConfig detectorConfig,
            String resultLabel
    ) {
        return runPhi(scenario, seed, detectorConfig, resultLabel, REALISTIC_DEFAULT, true);
    }

    private ExperimentResult runSwim(
            ScenarioConfig scenario,
            long seed,
            SwimLifeguardConfig detectorConfig,
            String resultLabel,
            String comparisonMode
    ) {
        Timeline timeline = Timeline.of(scenario, seed);
        NetworkTrace network = new NetworkTrace(scenario, timeline, seed, STREAM_SWIM);
        DeterministicEventScheduler scheduler = new DeterministicEventScheduler();
        ProtocolCost cost = new ProtocolCost();
        SwimLifeguardDetector detector = new SwimLifeguardDetector(
                OBSERVER, scenario.nodeCount(), detectorConfig);
        List<DetectorEvent> detectorEvents = new ArrayList<>();
        List<SimulationEvent> simulationEvents = new ArrayList<>();
        scheduleTruthEvents(scheduler, scenario, timeline, simulationEvents);
        scheduleTicks(scheduler, scenario, timeline, detector, detectorEvents);
        scheduleBackgroundSwim(
                scheduler, scenario, detectorConfig.probeInterval().toNanos(), cost, 0);
        scheduleSwimProbe(
                scheduler,
                scenario,
                timeline,
                network,
                detector,
                cost,
                detectorEvents,
                simulationEvents,
                0
        );
        scheduler.runUntil(scenario.duration().toNanos());
        simulationEvents.add(event(
                scenario.duration().toNanos(),
                SimulationEvent.Type.SIMULATION_ENDED,
                "simulation horizon reached",
                Map.of("durationNanos", scenario.duration().toNanos())
        ));
        drain(detector, detectorEvents);
        return result(
                scenario,
                seed,
                timeline,
                resultLabel,
                comparisonMode,
                detectorEvents,
                groundTruth(scenario, timeline),
                simulationEvents,
                cost
        );
    }

    private void scheduleSwimProbe(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            Timeline timeline,
            NetworkTrace network,
            SwimLifeguardDetector detector,
            ProtocolCost cost,
            List<DetectorEvent> detectorEvents,
            List<SimulationEvent> simulationEvents,
            long atNanos
    ) {
        if (atNanos > scenario.duration().toNanos()) {
            return;
        }
        scheduler.schedule(atNanos, DeterministicEventScheduler.Phase.SEND, () -> {
            if (network.observerPaused(atNanos)) {
                long resumeAt = timeline.faultEndNanos();
                detector.observeLocalDelay(resumeAt - atNanos);
                scheduleSwimProbe(
                        scheduler,
                        scenario,
                        timeline,
                        network,
                        detector,
                        cost,
                        detectorEvents,
                        simulationEvents,
                        resumeAt
                );
                return;
            }
            simulationEvents.add(event(
                    atNanos,
                    SimulationEvent.Type.PROBE_SENT,
                    "direct SWIM probe",
                    Map.of("probeTimeoutNanos", detector.effectiveProbeTimeoutNanos())
            ));
            ProbeOutcome direct = network.directProbe(atNanos);
            cost.add(direct.generatedMessages(), direct.generatedBytes());
            if (direct.success() && direct.elapsedNanos() <= detector.effectiveProbeTimeoutNanos()) {
                long deliverAt = atNanos + direct.elapsedNanos();
                scheduler.schedule(deliverAt, DeterministicEventScheduler.Phase.DELIVERY, () -> {
                    simulationEvents.add(event(
                            deliverAt,
                            SimulationEvent.Type.PACKET_DELIVERED,
                            "direct ACK delivered",
                            Map.of()
                    ));
                    detector.recordProbe(deliverAt, TARGET, true, 1);
                    drain(detector, detectorEvents);
                });
            } else {
                long timeoutAt = atNanos + detector.effectiveProbeTimeoutNanos();
                scheduler.schedule(timeoutAt, DeterministicEventScheduler.Phase.TIMEOUT, () -> {
                    simulationEvents.add(event(
                            timeoutAt,
                            SimulationEvent.Type.DIRECT_TIMEOUT,
                            "direct ACK timeout expired",
                            Map.of()
                    ));
                    scheduleIndirectProbe(
                            scheduler,
                            scenario,
                            network,
                            detector,
                            cost,
                            detectorEvents,
                            simulationEvents,
                            timeoutAt
                    );
                });
            }
            scheduleSwimProbe(
                    scheduler,
                    scenario,
                    timeline,
                    network,
                    detector,
                    cost,
                    detectorEvents,
                    simulationEvents,
                    atNanos + detector.effectiveProbeIntervalNanos()
            );
        });
    }

    private void scheduleIndirectProbe(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            NetworkTrace network,
            SwimLifeguardDetector detector,
            ProtocolCost cost,
            List<DetectorEvent> detectorEvents,
            List<SimulationEvent> simulationEvents,
            long atNanos
    ) {
        simulationEvents.add(event(
                atNanos,
                SimulationEvent.Type.INDIRECT_STARTED,
                "PING-REQ stage started",
                Map.of("checks", detector.config().indirectChecks())
        ));
        ProbeOutcome indirect = network.indirectProbe(atNanos, detector.config().indirectChecks());
        cost.add(indirect.generatedMessages(), indirect.generatedBytes());
        long timeout = detector.effectiveProbeTimeoutNanos();
        if (indirect.success() && indirect.elapsedNanos() <= timeout) {
            long deliverAt = atNanos + indirect.elapsedNanos();
            scheduler.schedule(deliverAt, DeterministicEventScheduler.Phase.DELIVERY, () -> {
                simulationEvents.add(event(
                        deliverAt,
                        SimulationEvent.Type.PACKET_DELIVERED,
                        "indirect ACK delivered",
                        Map.of()
                ));
                detector.recordProbe(deliverAt, TARGET, true, 1);
                drain(detector, detectorEvents);
            });
            return;
        }
        long timeoutAt = atNanos + timeout;
        scheduler.schedule(timeoutAt, DeterministicEventScheduler.Phase.TIMEOUT, () -> {
            simulationEvents.add(event(
                    timeoutAt,
                    SimulationEvent.Type.INDIRECT_TIMEOUT,
                    "indirect ACK timeout expired",
                    Map.of()
            ));
            long previousDeadline = detector.suspicionDeadlineNanos(TARGET);
            detector.recordProbe(timeoutAt, TARGET, false, 1);
            long initialDeadline = detector.suspicionDeadlineNanos(TARGET);
            if (!network.targetResponsive(timeoutAt)
                    || scenario.type() == ScenarioType.PARTITION) {
                for (int index = 0; index < detector.config().maxConfirmations(); index++) {
                    detector.confirmSuspicion(timeoutAt, TARGET, "node-" + (index + 2));
                }
            }
            drain(detector, detectorEvents);
            if (detector.config().buddySystemEnabled()) {
                scheduleBuddyProbe(
                        scheduler,
                        scenario,
                        network,
                        detector,
                        cost,
                        detectorEvents,
                        simulationEvents,
                        timeoutAt + 1_000_000L
                );
            }
            long deadline = detector.suspicionDeadlineNanos(TARGET);
            if (previousDeadline == 0 && deadline > 0) {
                simulationEvents.add(event(
                        timeoutAt,
                        SimulationEvent.Type.SUSPICION_DEADLINE_SCHEDULED,
                        "SWIM suspicion deadline scheduled",
                        Map.of(
                                "initialDeadlineNanos", initialDeadline,
                                "finalDeadlineNanos", deadline,
                                "timeoutFromSuspectNanos", deadline - timeoutAt,
                                "simulationEndNanos", scenario.duration().toNanos(),
                                "withinSimulationHorizon",
                                deadline <= scenario.duration().toNanos(),
                                "confirmationsEnabled",
                                detector.config().suspicionConfirmationsEnabled(),
                                "lhaSuspicionEnabled",
                                detector.config().lhaSuspicionEnabled()
                        )
                ));
            }
            if (deadline > timeoutAt) {
                scheduler.schedule(deadline, DeterministicEventScheduler.Phase.TIMEOUT, () -> {
                    detector.tick(deadline);
                    drain(detector, detectorEvents);
                });
            }
        });
    }

    private void scheduleBuddyProbe(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            NetworkTrace network,
            SwimLifeguardDetector detector,
            ProtocolCost cost,
            List<DetectorEvent> detectorEvents,
            List<SimulationEvent> simulationEvents,
            long atNanos
    ) {
        if (atNanos > scenario.duration().toNanos()) {
            return;
        }
        scheduler.schedule(atNanos, DeterministicEventScheduler.Phase.SEND, () -> {
            simulationEvents.add(event(
                    atNanos,
                    SimulationEvent.Type.VERIFICATION_STARTED,
                    "Lifeguard buddy probe",
                    Map.of("independentEvidence", false)
            ));
            ProbeOutcome buddy = network.directProbe(atNanos);
            cost.add(buddy.generatedMessages(), buddy.generatedBytes());
            if (buddy.success() && buddy.elapsedNanos() <= detector.effectiveProbeTimeoutNanos()) {
                long deliverAt = atNanos + buddy.elapsedNanos();
                scheduler.schedule(deliverAt, DeterministicEventScheduler.Phase.DELIVERY, () -> {
                    detector.recordProbe(deliverAt, TARGET, true, 1);
                    drain(detector, detectorEvents);
                    simulationEvents.add(event(
                            deliverAt,
                            SimulationEvent.Type.PACKET_DELIVERED,
                            "Lifeguard buddy probe succeeded",
                            Map.of("independentEvidence", false)
                    ));
                });
            }
        });
    }

    private void scheduleBackgroundSwim(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            long intervalNanos,
            ProtocolCost cost,
            long atNanos
    ) {
        if (atNanos > scenario.duration().toNanos()) {
            return;
        }
        scheduler.schedule(atNanos, DeterministicEventScheduler.Phase.SEND, () -> {
            long backgroundNodes = scenario.nodeCount() - 1L;
            cost.add(backgroundNodes * 2L, backgroundNodes * 128L);
            scheduleBackgroundSwim(scheduler, scenario, intervalNanos, cost, atNanos + intervalNanos);
        });
    }

    private ExperimentResult runFixedDeadline(
            ScenarioConfig scenario,
            long seed,
            FixedDeadlineConfig detectorConfig,
            String resultLabel,
            String comparisonMode
    ) {
        Timeline timeline = Timeline.of(scenario, seed);
        int streamNamespace = detectorConfig.peerSelectionMode()
                == FixedDeadlineConfig.PeerSelectionMode.DETERMINISTIC_RING
                ? STREAM_RING_HEARTBEAT
                : STREAM_ALL_TO_ALL_HEARTBEAT;
        NetworkTrace network = new NetworkTrace(scenario, timeline, seed, streamNamespace);
        DeterministicEventScheduler scheduler = new DeterministicEventScheduler();
        ProtocolCost cost = new ProtocolCost();
        FixedDeadlineFailureDetector detector = new FixedDeadlineFailureDetector(OBSERVER, detectorConfig);
        detector.registerTarget(0, TARGET, 1);
        List<DetectorEvent> detectorEvents = new ArrayList<>();
        List<SimulationEvent> simulationEvents = new ArrayList<>();
        scheduleTruthEvents(scheduler, scenario, timeline, simulationEvents);
        scheduleTicks(scheduler, scenario, timeline, detector, detectorEvents);
        scheduleFixedHeartbeat(
                scheduler,
                scenario,
                network,
                detector,
                detectorConfig,
                cost,
                detectorEvents,
                simulationEvents,
                0,
                1
        );
        scheduleFixedVerification(
                scheduler,
                scenario,
                network,
                detector,
                detectorConfig,
                cost,
                detectorEvents,
                simulationEvents,
                0
        );
        scheduler.runUntil(scenario.duration().toNanos());
        drain(detector, detectorEvents);
        return result(
                scenario,
                seed,
                timeline,
                resultLabel,
                comparisonMode,
                detectorEvents,
                groundTruth(scenario, timeline),
                simulationEvents,
                cost
        );
    }

    private void scheduleFixedHeartbeat(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            NetworkTrace network,
            FixedDeadlineFailureDetector detector,
            FixedDeadlineConfig config,
            ProtocolCost cost,
            List<DetectorEvent> detectorEvents,
            List<SimulationEvent> simulationEvents,
            long atNanos,
            long version
    ) {
        if (atNanos > scenario.duration().toNanos()) {
            return;
        }
        scheduler.schedule(atNanos, DeterministicEventScheduler.Phase.SEND, () -> {
            long messages = config.peerSelectionMode() == FixedDeadlineConfig.PeerSelectionMode.FULL_MESH
                    ? (long) scenario.nodeCount() * (scenario.nodeCount() - 1L)
                    : scenario.nodeCount();
            cost.add(messages, messages * config.payloadBytes());
            simulationEvents.add(event(
                    atNanos,
                    SimulationEvent.Type.HEARTBEAT_SENT,
                    "fixed deadline heartbeat",
                    Map.of(
                            "version", version,
                            "topology", config.peerSelectionMode().name()
                    )
            ));
            PacketOutcome packet = network.heartbeat(atNanos, 101);
            if (packet.delivered()) {
                long deliverAt = atNanos + packet.delayNanos();
                scheduler.schedule(deliverAt, DeterministicEventScheduler.Phase.DELIVERY, () -> {
                    detector.recordHeartbeat(deliverAt, TARGET, 1);
                    drain(detector, detectorEvents);
                    simulationEvents.add(event(
                            deliverAt,
                            SimulationEvent.Type.PACKET_DELIVERED,
                            "fixed heartbeat delivered",
                            Map.of("version", version)
                    ));
                });
            } else {
                simulationEvents.add(event(
                        atNanos,
                        SimulationEvent.Type.PACKET_DROPPED,
                        "fixed heartbeat dropped",
                        Map.of("version", version)
                ));
            }
            scheduleFixedHeartbeat(
                    scheduler,
                    scenario,
                    network,
                    detector,
                    config,
                    cost,
                    detectorEvents,
                    simulationEvents,
                    atNanos + config.heartbeatInterval().toNanos(),
                    version + 1
            );
        });
    }

    private void scheduleFixedVerification(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            NetworkTrace network,
            FixedDeadlineFailureDetector detector,
            FixedDeadlineConfig config,
            ProtocolCost cost,
            List<DetectorEvent> detectorEvents,
            List<SimulationEvent> simulationEvents,
            long atNanos
    ) {
        if (!config.verifySuspectEnabled() || atNanos > scenario.duration().toNanos()) {
            return;
        }
        scheduler.schedule(atNanos, DeterministicEventScheduler.Phase.SEND, () -> {
            if (!network.observerPaused(atNanos) && detector.beginVerification(TARGET)) {
                cost.add(3, 3L * config.payloadBytes());
                simulationEvents.add(event(
                        atNanos,
                        SimulationEvent.Type.VERIFICATION_STARTED,
                        "independent helper verification requested",
                        Map.of("witness", "node-2")
                ));
                ProbeOutcome verification = network.witnessProbe(atNanos, 0);
                boolean reachable = verification.success()
                        && verification.elapsedNanos() <= config.verificationInterval().toNanos();
                long completeAt = atNanos + (reachable
                        ? verification.elapsedNanos()
                        : config.verificationInterval().toNanos());
                DeterministicEventScheduler.Phase phase = reachable
                        ? DeterministicEventScheduler.Phase.DELIVERY
                        : DeterministicEventScheduler.Phase.TIMEOUT;
                scheduler.schedule(completeAt, phase, () -> {
                    detector.recordVerification(completeAt, TARGET, reachable);
                    drain(detector, detectorEvents);
                    simulationEvents.add(event(
                            completeAt,
                            reachable
                                    ? SimulationEvent.Type.PACKET_DELIVERED
                                    : SimulationEvent.Type.VERIFICATION_TIMEOUT,
                            reachable
                                    ? "independent verification succeeded"
                                    : "independent verification timed out",
                            Map.of("witness", "node-2")
                    ));
                });
            }
            scheduleFixedVerification(
                    scheduler,
                    scenario,
                    network,
                    detector,
                    config,
                    cost,
                    detectorEvents,
                    simulationEvents,
                    atNanos + config.verificationInterval().toNanos()
            );
        });
    }

    private ExperimentResult runAdaptiveDeadline(
            ScenarioConfig scenario,
            long seed,
            AdaptiveDeadlineConfig detectorConfig,
            String resultLabel,
            String comparisonMode
    ) {
        Timeline timeline = Timeline.of(scenario, seed);
        NetworkTrace network = new NetworkTrace(
                scenario, timeline, seed, STREAM_ALL_TO_ALL_HEARTBEAT);
        DeterministicEventScheduler scheduler = new DeterministicEventScheduler();
        ProtocolCost cost = new ProtocolCost();
        AdaptiveDeadlineFailureDetector detector = new AdaptiveDeadlineFailureDetector(
                OBSERVER, detectorConfig);
        detector.registerTarget(0, TARGET, 1);
        List<DetectorEvent> detectorEvents = new ArrayList<>();
        List<SimulationEvent> simulationEvents = new ArrayList<>();
        scheduleTruthEvents(scheduler, scenario, timeline, simulationEvents);
        scheduleTicks(scheduler, scenario, timeline, detector, detectorEvents);
        scheduleAdaptiveHeartbeat(
                scheduler,
                scenario,
                network,
                detector,
                detectorConfig,
                cost,
                detectorEvents,
                simulationEvents,
                0,
                1
        );
        scheduler.runUntil(scenario.duration().toNanos());
        drain(detector, detectorEvents);
        return result(
                scenario,
                seed,
                timeline,
                resultLabel,
                comparisonMode,
                detectorEvents,
                groundTruth(scenario, timeline),
                simulationEvents,
                cost
        );
    }

    private void scheduleAdaptiveHeartbeat(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            NetworkTrace network,
            AdaptiveDeadlineFailureDetector detector,
            AdaptiveDeadlineConfig config,
            ProtocolCost cost,
            List<DetectorEvent> detectorEvents,
            List<SimulationEvent> simulationEvents,
            long atNanos,
            long version
    ) {
        if (atNanos > scenario.duration().toNanos()) {
            return;
        }
        scheduler.schedule(atNanos, DeterministicEventScheduler.Phase.SEND, () -> {
            long messages = (long) scenario.nodeCount() * (scenario.nodeCount() - 1L);
            cost.add(messages, messages * config.payloadBytes());
            simulationEvents.add(event(
                    atNanos,
                    SimulationEvent.Type.HEARTBEAT_SENT,
                    "adaptive deadline heartbeat",
                    Map.of("version", version, "deadlineNanos", detector.deadlineNanos(TARGET))
            ));
            PacketOutcome packet = network.heartbeat(atNanos, 101);
            if (packet.delivered()) {
                long deliverAt = atNanos + packet.delayNanos();
                scheduler.schedule(deliverAt, DeterministicEventScheduler.Phase.DELIVERY, () -> {
                    detector.recordHeartbeat(deliverAt, TARGET, 1);
                    drain(detector, detectorEvents);
                    simulationEvents.add(event(
                            deliverAt,
                            SimulationEvent.Type.PACKET_DELIVERED,
                            "adaptive heartbeat delivered",
                            Map.of("version", version)
                    ));
                });
            } else {
                simulationEvents.add(event(
                        atNanos,
                        SimulationEvent.Type.PACKET_DROPPED,
                        "adaptive heartbeat dropped",
                        Map.of("version", version)
                ));
            }
            scheduleAdaptiveHeartbeat(
                    scheduler,
                    scenario,
                    network,
                    detector,
                    config,
                    cost,
                    detectorEvents,
                    simulationEvents,
                    atNanos + config.heartbeatInterval().toNanos(),
                    version + 1
            );
        });
    }

    private ExperimentResult runPhi(
            ScenarioConfig scenario,
            long seed,
            PhiAccrualConfig detectorConfig,
            String resultLabel,
            String comparisonMode,
            boolean gossip
    ) {
        Timeline timeline = Timeline.of(scenario, seed);
        NetworkTrace network = new NetworkTrace(
                scenario,
                timeline,
                seed,
                gossip ? STREAM_GOSSIP_HEARTBEAT : STREAM_ALL_TO_ALL_HEARTBEAT
        );
        DeterministicEventScheduler scheduler = new DeterministicEventScheduler();
        ProtocolCost cost = new ProtocolCost();
        PhiAccrualFailureDetector detector = new PhiAccrualFailureDetector(
                OBSERVER, detectorConfig, resultLabel);
        List<DetectorEvent> detectorEvents = new ArrayList<>();
        List<SimulationEvent> simulationEvents = new ArrayList<>();
        scheduleTruthEvents(scheduler, scenario, timeline, simulationEvents);
        scheduleTicks(scheduler, scenario, timeline, detector, detectorEvents);
        schedulePhiHeartbeat(
                scheduler,
                scenario,
                network,
                detector,
                detectorConfig,
                cost,
                detectorEvents,
                simulationEvents,
                0,
                1,
                gossip
        );
        scheduler.runUntil(scenario.duration().toNanos());
        drain(detector, detectorEvents);
        return result(
                scenario,
                seed,
                timeline,
                resultLabel,
                comparisonMode,
                detectorEvents,
                groundTruth(scenario, timeline),
                simulationEvents,
                cost
        );
    }

    private void schedulePhiHeartbeat(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            NetworkTrace network,
            PhiAccrualFailureDetector detector,
            PhiAccrualConfig config,
            ProtocolCost cost,
            List<DetectorEvent> detectorEvents,
            List<SimulationEvent> simulationEvents,
            long atNanos,
            long version,
            boolean gossip
    ) {
        if (atNanos > scenario.duration().toNanos()) {
            return;
        }
        scheduler.schedule(atNanos, DeterministicEventScheduler.Phase.SEND, () -> {
            long senders = network.targetResponsive(atNanos)
                    ? scenario.nodeCount()
                    : scenario.nodeCount() - 1L;
            long messages = gossip
                    ? senders * scenario.gossipFanout()
                    : senders * (scenario.nodeCount() - 1L);
            cost.add(messages, messages * 96L);
            simulationEvents.add(event(
                    atNanos,
                    SimulationEvent.Type.HEARTBEAT_SENT,
                    gossip ? "gossip heartbeat" : "direct all-to-all heartbeat",
                    Map.of("version", version, "transport", gossip ? "gossip" : "direct")
            ));
            PacketOutcome packet = network.heartbeat(atNanos, 101);
            if (packet.delivered()) {
                long deliverAt = atNanos + packet.delayNanos();
                scheduler.schedule(deliverAt, DeterministicEventScheduler.Phase.DELIVERY, () -> {
                    detector.recordHeartbeat(
                            new GossipHeartbeat(TARGET, 1, version, deliverAt), true);
                    drain(detector, detectorEvents);
                    simulationEvents.add(event(
                            deliverAt,
                            SimulationEvent.Type.PACKET_DELIVERED,
                            "gossip heartbeat delivered",
                            Map.of("version", version)
                    ));
                });
            } else {
                simulationEvents.add(event(
                        atNanos,
                        SimulationEvent.Type.PACKET_DROPPED,
                        "gossip heartbeat dropped",
                        Map.of("version", version)
                ));
            }
            schedulePhiHeartbeat(
                    scheduler,
                    scenario,
                    network,
                    detector,
                    config,
                    cost,
                    detectorEvents,
                    simulationEvents,
                    atNanos + config.heartbeatInterval().toNanos(),
                    version + 1,
                    gossip
            );
        });
    }

    private static void scheduleTicks(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            Timeline timeline,
            FailureDetector detector,
            List<DetectorEvent> detectorEvents
    ) {
        long interval = scenario.tick().toNanos();
        for (long at = 0; at <= scenario.duration().toNanos(); at += interval) {
            long tickAt = at;
            scheduler.schedule(tickAt, DeterministicEventScheduler.Phase.TICK, () -> {
                if (scenario.type() == ScenarioType.OBSERVER_PAUSE
                        && timeline.inFaultWindow(tickAt)) {
                    return;
                }
                detector.tick(tickAt);
                drain(detector, detectorEvents);
            });
        }
    }

    private static void scheduleTruthEvents(
            DeterministicEventScheduler scheduler,
            ScenarioConfig scenario,
            Timeline timeline,
            List<SimulationEvent> simulationEvents
    ) {
        if (scenario.type() == ScenarioType.NORMAL) {
            return;
        }
        scheduler.schedule(
                timeline.faultStartNanos(),
                DeterministicEventScheduler.Phase.FAULT,
                () -> simulationEvents.add(event(
                        timeline.faultStartNanos(),
                        SimulationEvent.Type.FAULT_INJECTED,
                        scenario.type().id(),
                        Map.of("seedAdjusted", true)
                ))
        );
        if (!timeline.permanentFailure() && timeline.faultEndNanos() <= scenario.duration().toNanos()) {
            scheduler.schedule(
                    timeline.faultEndNanos(),
                    DeterministicEventScheduler.Phase.FAULT,
                    () -> simulationEvents.add(event(
                            timeline.faultEndNanos(),
                            SimulationEvent.Type.FAULT_CLEARED,
                            scenario.type().id(),
                            Map.of()
                    ))
            );
        }
    }

    private static ExperimentResult result(
            ScenarioConfig scenario,
            long seed,
            Timeline timeline,
            String resultLabel,
            String comparisonMode,
            List<DetectorEvent> detectorEvents,
            List<GroundTruthEvent> truth,
            List<SimulationEvent> simulationEvents,
            ProtocolCost cost
    ) {
        RunMetrics metrics = MetricsEvaluator.evaluate(
                scenario.duration().toNanos(), TARGET, detectorEvents, truth, cost);
        return new ExperimentResult(
                runId(resultLabel, scenario.type(), scenario.nodeCount(), seed, comparisonMode),
                resultLabel,
                scenario.type(),
                seed,
                scenario.nodeCount(),
                scenario.duration().toNanos(),
                timeline.faultStartNanos(),
                TARGET,
                detectorEvents,
                truth,
                simulationEvents,
                comparisonMode,
                "simulation-observed",
                "model-estimated",
                true,
                metrics
        );
    }

    private static List<GroundTruthEvent> groundTruth(
            ScenarioConfig scenario,
            Timeline timeline
    ) {
        List<GroundTruthEvent> events = new ArrayList<>();
        events.add(new GroundTruthEvent(0, TARGET, true, true, true, "none", "normal"));
        if (scenario.type() == ScenarioType.NORMAL) {
            return events;
        }
        if (scenario.type() == ScenarioType.CRASH_UNDER_JITTER) {
            events.add(new GroundTruthEvent(
                    timeline.faultStartNanos(), TARGET, true, true, true,
                    "fault-1", "jitter"));
            events.add(new GroundTruthEvent(
                    timeline.failureAtNanos(), TARGET, false, false, false,
                    "fault-2", "crash-stop"));
            return events;
        }
        if (scenario.type() == ScenarioType.CRASH) {
            events.add(new GroundTruthEvent(
                    timeline.failureAtNanos(), TARGET, false, false, false,
                    "fault-1", "crash-stop"));
            return events;
        }
        boolean unreachable = scenario.type() == ScenarioType.PARTITION
                || scenario.type() == ScenarioType.TARGET_PAUSE
                || scenario.type() == ScenarioType.OBSERVER_PAUSE;
        boolean serviceHealthy = scenario.type() != ScenarioType.TARGET_PAUSE;
        events.add(new GroundTruthEvent(
                timeline.faultStartNanos(),
                TARGET,
                true,
                !unreachable,
                serviceHealthy,
                "fault-1",
                scenario.type().id()
        ));
        if (timeline.faultEndNanos() <= scenario.duration().toNanos()) {
            events.add(new GroundTruthEvent(
                    timeline.faultEndNanos(), TARGET, true, true, true,
                    "fault-1", "recovered"));
        }
        return events;
    }

    private static SimulationEvent event(
            long atNanos,
            SimulationEvent.Type type,
            String reason,
            Map<String, Object> details
    ) {
        return new SimulationEvent(atNanos, type, OBSERVER, TARGET, reason, details);
    }

    private static void drain(FailureDetector detector, List<DetectorEvent> events) {
        events.addAll(detector.drainEvents());
    }

    private static String runId(
            String algorithm,
            ScenarioType scenario,
            int nodeCount,
            long seed,
            String comparisonMode
    ) {
        return algorithm + "-" + scenario.id() + "-n" + nodeCount + "-s" + seed
                + "-" + comparisonMode;
    }

    private record Timeline(
            long faultStartNanos,
            long faultEndNanos,
            long failureAtNanos,
            boolean permanentFailure
    ) {
        private static Timeline of(ScenarioConfig scenario, long seed) {
            int index = (int) Math.floorMod(seed - 1, FAULT_OFFSETS_NANOS.length);
            long offset = FAULT_OFFSETS_NANOS[index];
            long start = scenario.faultStart().toNanos() + offset;
            long end = scenario.faultEnd().toNanos() + offset;
            boolean crash = scenario.type() == ScenarioType.CRASH
                    || scenario.type() == ScenarioType.CRASH_UNDER_JITTER;
            long failure = scenario.type() == ScenarioType.CRASH_UNDER_JITTER
                    ? start + (end - start) * 2L / 3L
                    : start;
            return new Timeline(start, end, failure, crash);
        }

        private boolean inFaultWindow(long atNanos) {
            return atNanos >= faultStartNanos && atNanos < faultEndNanos;
        }
    }

    private record PacketOutcome(boolean delivered, long delayNanos) {
    }

    private record ProbeOutcome(
            boolean success,
            long elapsedNanos,
            long generatedMessages,
            long generatedBytes
    ) {
    }

    /** Stateless draws keep the environment trace independent of protocol call order. */
    private static final class NetworkTrace {
        private final ScenarioConfig scenario;
        private final Timeline timeline;
        private final long seed;
        private final int streamNamespace;

        private NetworkTrace(
                ScenarioConfig scenario,
                Timeline timeline,
                long seed,
                int streamNamespace
        ) {
            this.scenario = scenario;
            this.timeline = timeline;
            this.seed = seed;
            this.streamNamespace = streamNamespace;
        }

        private ProbeOutcome directProbe(long atNanos) {
            if (!targetResponsive(atNanos)) {
                return new ProbeOutcome(false, 0, 1, 64);
            }
            long firstDelay = oneWayDelay(atNanos, 1);
            if (!delivered(atNanos, 1)) {
                return new ProbeOutcome(false, firstDelay, 1, 64);
            }
            long responseAt = atNanos + firstDelay;
            long secondDelay = oneWayDelay(responseAt, 2);
            boolean success = targetResponsive(responseAt) && delivered(responseAt, 2);
            return new ProbeOutcome(success, firstDelay + secondDelay, 2, 128);
        }

        private ProbeOutcome indirectProbe(long atNanos, int checks) {
            long generatedMessages = checks * 4L;
            long generatedBytes = checks * 256L;
            long best = Long.MAX_VALUE;
            for (int check = 0; check < checks; check++) {
                long elapsed = 0;
                boolean success = true;
                for (int leg = 0; leg < 4; leg++) {
                    int stream = 10 + check * 4 + leg;
                    long legAt = atNanos + elapsed;
                    if (!targetResponsive(legAt) || !delivered(legAt, stream)) {
                        success = false;
                        break;
                    }
                    elapsed += oneWayDelay(legAt, stream);
                }
                if (success) {
                    best = Math.min(best, elapsed);
                }
            }
            return new ProbeOutcome(best != Long.MAX_VALUE, best, generatedMessages, generatedBytes);
        }

        private ProbeOutcome witnessProbe(long atNanos, int witnessIndex) {
            if (!targetResponsive(atNanos)) {
                return new ProbeOutcome(false, 0, 1, 64);
            }
            int requestStream = 300 + witnessIndex * 3;
            int responseStream = requestStream + 1;
            long requestDelay = oneWayDelay(atNanos, requestStream);
            if (!deliveredForWitness(atNanos, requestStream)) {
                return new ProbeOutcome(false, requestDelay, 1, 64);
            }
            long responseAt = atNanos + requestDelay;
            long responseDelay = oneWayDelay(responseAt, responseStream);
            boolean success = targetResponsive(responseAt)
                    && deliveredForWitness(responseAt, responseStream);
            return new ProbeOutcome(success, requestDelay + responseDelay, 2, 128);
        }

        private PacketOutcome heartbeat(long atNanos, int stream) {
            if (observerPaused(atNanos)
                    || !targetResponsive(atNanos)
                    || !delivered(atNanos, stream)) {
                return new PacketOutcome(false, 0);
            }
            return new PacketOutcome(true, oneWayDelay(atNanos, stream));
        }

        private boolean targetResponsive(long atNanos) {
            boolean alive = (scenario.type() != ScenarioType.CRASH
                    && scenario.type() != ScenarioType.CRASH_UNDER_JITTER)
                    || atNanos < timeline.failureAtNanos();
            return alive && (scenario.type() != ScenarioType.TARGET_PAUSE
                    || !timeline.inFaultWindow(atNanos));
        }

        private boolean observerPaused(long atNanos) {
            return scenario.type() == ScenarioType.OBSERVER_PAUSE
                    && timeline.inFaultWindow(atNanos);
        }

        private boolean delivered(long atNanos, int stream) {
            if (!timeline.inFaultWindow(atNanos)) {
                return true;
            }
            if (scenario.type() == ScenarioType.PARTITION) {
                return false;
            }
            if (scenario.type() == ScenarioType.PACKET_LOSS) {
                return draw(atNanos, stream) >= scenario.packetLoss();
            }
            if (scenario.type() == ScenarioType.PACKET_LOSS_BURST
                    && burstActive(atNanos, 5_000_000_000L, 1_500_000_000L)) {
                return draw(atNanos, stream) >= scenario.packetLoss();
            }
            return true;
        }

        private boolean deliveredForWitness(long atNanos, int stream) {
            if (scenario.type() == ScenarioType.PARTITION && timeline.inFaultWindow(atNanos)) {
                return true;
            }
            return delivered(atNanos, stream);
        }

        private long oneWayDelay(long atNanos, int stream) {
            long base = 20_000_000L;
            long jitterLimit = 5_000_000L;
            if (timeline.inFaultWindow(atNanos)) {
                switch (scenario.type()) {
                    case LATENCY -> {
                        base = scenario.fixedLatency().toNanos();
                        jitterLimit = scenario.jitter().toNanos();
                    }
                    case LATENCY_GRADUAL -> {
                        long span = timeline.faultEndNanos() - timeline.faultStartNanos();
                        double progress = (double) (atNanos - timeline.faultStartNanos()) / span;
                        double triangle = 1.0 - Math.abs(progress * 2.0 - 1.0);
                        base = 20_000_000L + (long) ((scenario.fixedLatency().toNanos()
                                - 20_000_000L) * triangle);
                        jitterLimit = scenario.jitter().toNanos();
                    }
                    case LATENCY_BURSTY -> {
                        if (burstActive(atNanos, 5_000_000_000L, 1_000_000_000L)) {
                            base = scenario.fixedLatency().toNanos();
                            jitterLimit = scenario.jitter().toNanos();
                        }
                    }
                    case LATENCY_REGIME -> {
                        long offset = atNanos - timeline.faultStartNanos();
                        boolean high = (offset / 3_000_000_000L) % 2 == 1;
                        base = high ? scenario.fixedLatency().toNanos() : 80_000_000L;
                        jitterLimit = high ? scenario.jitter().toNanos() : 15_000_000L;
                    }
                    case CRASH_UNDER_JITTER -> {
                        if (burstActive(atNanos, 4_000_000_000L, 2_000_000_000L)) {
                            base = scenario.fixedLatency().toNanos();
                            jitterLimit = scenario.jitter().toNanos();
                        } else {
                            base = 150_000_000L;
                            jitterLimit = 50_000_000L;
                        }
                    }
                    default -> {
                        // This scenario changes reachability rather than latency.
                    }
                }
            }
            double signed = draw(atNanos, stream + 10_000) * 2.0 - 1.0;
            return Math.max(0L, base + (long) (signed * jitterLimit));
        }

        private boolean burstActive(long atNanos, long periodNanos, long activeNanos) {
            long offset = atNanos - timeline.faultStartNanos();
            return offset >= 0 && offset % periodNanos < activeNanos;
        }

        private double draw(long atNanos, int stream) {
            long value = seed * 0x9E3779B97F4A7C15L
                    ^ atNanos
                    ^ ((long) stream * 0xBF58476D1CE4E5B9L)
                    ^ ((long) streamNamespace * 0x94D049BB133111EBL);
            value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
            value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
            value ^= value >>> 31;
            return (value >>> 11) * 0x1.0p-53;
        }
    }
}
