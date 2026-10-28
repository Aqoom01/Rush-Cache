package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 실제 protocol을 호출하고 성공 요청 oracle로 중단 경계별 계약을 검사하는 S6 모델. */
final class InterruptionExperiment {
    static final int OUTAGE_TICKS = 3;
    static final int MAX_TICKS = 10_000;
    static final String GENERATOR = "b3-s6-boundary-1";

    enum Boundary {
        COPYING_ABORT, BEFORE_COMMIT_ABORT, FINAL_ACK_WAIT_ABORT, AFTER_COMMIT_ABORT,
        PARTIAL_CLEANUP_ABORT, CATCHING_UP_ABORT, CATCHING_UP_PARTIAL_ABORT,
        SOURCE_DOWN_RESUME, PULL_TIMEOUT_RESUME, SOURCE_DOWN_UNRECOVERED;

        boolean supported(String variant) {
            return switch (this) {
                case COPYING_ABORT, FINAL_ACK_WAIT_ABORT -> !variant.equals("post-copy");
                case CATCHING_UP_ABORT, CATCHING_UP_PARTIAL_ABORT -> variant.equals("pre-copy");
                case SOURCE_DOWN_RESUME, PULL_TIMEOUT_RESUME, SOURCE_DOWN_UNRECOVERED -> variant.equals("post-copy");
                default -> true;
            };
        }

        boolean cancelsBeforeCommit() {
            return this == COPYING_ABORT || this == BEFORE_COMMIT_ABORT
                    || this == FINAL_ACK_WAIT_ABORT || this == CATCHING_UP_ABORT || this == CATCHING_UP_PARTIAL_ABORT;
        }

        boolean sourceFailure() {
            return this == SOURCE_DOWN_RESUME || this == PULL_TIMEOUT_RESUME || this == SOURCE_DOWN_UNRECOVERED;
        }

        String reason() {
            return switch (this) {
                case COPYING_ABORT -> "post-copy는 복사 전에 소유권을 넘겨 source-owned COPYING 취소 경계가 없다";
                case FINAL_ACK_WAIT_ABORT -> "post-copy는 소유권 전환 전 전체 복사 final ACK를 기다리지 않는다";
                case CATCHING_UP_ABORT, CATCHING_UP_PARTIAL_ABORT -> "CATCHING_UP은 pre-copy 전용 상태다";
                case SOURCE_DOWN_RESUME, PULL_TIMEOUT_RESUME, SOURCE_DOWN_UNRECOVERED -> "소유권 선전환 뒤 source 의존성은 post-copy 별도 계약이다";
                default -> throw new IllegalStateException("공통 경계는 모든 방식이 지원한다");
            };
        }

        String terminal() {
            return cancelsBeforeCommit() ? "ABORTED" : this == SOURCE_DOWN_UNRECOVERED ? "FAILED" : "DONE";
        }
    }

    record Run(String terminal, String injectionState, String workloadHash, Map<String, Object> metrics,
            List<Map<String, Object>> events) {}

    static ComparisonExperiment.Case condition(String profile) {
        if (!List.of("full", "quick", "extended-full", "extended-quick").contains(profile)) throw new IllegalArgumentException("invalid S6 profile");
        boolean quick = profile.endsWith("quick");
        return new ComparisonExperiment.Case("S6", "S6", quick ? 1_000 : 10_000,
                quick ? 4_096 : 65_536, .5, 30, 0, quick ? 1_024 : 262_144, 10);
    }

    static String inputHash(ComparisonExperiment.Case c, Boundary boundary, long seed) {
        String extra = boundary == Boundary.CATCHING_UP_PARTIAL_ABORT ? ":batchBytes=" + batchBytes(c, boundary) : "";
        return ComparisonExperiment.sha256(GENERATOR + ":" + boundary + ":" + OUTAGE_TICKS + ":" + MAX_TICKS
                + ":" + workloadHash(c, seed) + extra);
    }

    /** 기존 full/quick의 135행은 그대로 재생성하고 새 profile에서 부분 전송 경계를 추가한다. */
    static List<Boundary> boundaries(String profile) {
        condition(profile);
        return java.util.Arrays.stream(Boundary.values())
                .filter(b -> profile.startsWith("extended-") || b != Boundary.CATCHING_UP_PARTIAL_ABORT).toList();
    }

    static int batchBytes(ComparisonExperiment.Case c, Boundary boundary) {
        return boundary == Boundary.CATCHING_UP_PARTIAL_ABORT ? Math.toIntExact(c.budget() / 2) : 65_536;
    }

    static Map<String, Object> config(ComparisonExperiment.Case c, Boundary boundary) {
        var result = new java.util.TreeMap<>(config(c));
        if (boundary == Boundary.CATCHING_UP_PARTIAL_ABORT) result.put("batchTargetBytes", batchBytes(c, boundary));
        return result;
    }

    static Map<String, Object> config(ComparisonExperiment.Case c) {
        var config = new java.util.TreeMap<>(c.config());
        config.put("maxTicks", MAX_TICKS);
        return config;
    }

    static String workloadHash(ComparisonExperiment.Case c, long seed) {
        var config = new java.util.TreeMap<>(config(c));
        config.remove("stopThresholdBytes");
        config.remove("maxRounds");
        return ComparisonExperiment.sha256(ComparisonExperiment.GENERATOR + ":seed=" + seed + ":" + BaselineJson.encode(config));
    }

    static Run run(ComparisonExperiment.Case c, Boundary boundary, long seed, String variant) {
        ComparisonExperiment.require(ComparisonExperiment.VARIANTS.contains(variant) && boundary.supported(variant), "unsupported S6 run");
        var trace = new ComparisonExperiment.Trace(c, seed);
        var oracle = trace.initial();
        var d = ComparisonExperiment.driver(variant, c, oracle, batchBytes(c, boundary));
        var observation = new Observation(d, oracle);
        if (boundary == Boundary.FINAL_ACK_WAIT_ABORT) d.holdFinalAck();
        if (boundary == Boundary.PARTIAL_CLEANUP_ABORT) d.cleanupFailures(2);
        d.begin();
        observation.event(0, "begin");
        int injectionTick = -1, failedTick = -1, terminalTick = -1;
        String injectionState = null;
        long snapshotBytes = -1, deltaBytesBeforeAbort = 0, deltaAcknowledgedBeforeAbort = 0;
        Map<Bytes, Bytes> retainedSource = null;
        for (int tick = 0; tick < MAX_TICKS; tick++) {
            String before = d.state();
            if (snapshotBytes < 0 && d.state().equals("CATCHING_UP")) {
                snapshotBytes = d.bytes();
                observation.event(tick, "live-delta-start");
            }
            if (injectionTick < 0 && atBoundary(d, boundary)) {
                injectionTick = tick;
                injectionState = d.state();
                observation.event(tick, "inject:" + boundary);
                if (boundary == Boundary.CATCHING_UP_PARTIAL_ABORT) {
                    deltaBytesBeforeAbort = d.bytes() - snapshotBytes;
                    deltaAcknowledgedBeforeAbort = d.acknowledgedSequence();
                    ComparisonExperiment.require(snapshotBytes >= 0 && deltaBytesBeforeAbort > 0
                            && deltaAcknowledgedBeforeAbort > 0 && d.partialLiveDelta(), "delta not partially applied before abort");
                }
                if (boundary.sourceFailure()) {
                    retainedSource = new LinkedHashMap<>(d.source());
                    if (boundary == Boundary.PULL_TIMEOUT_RESUME) d.dropPull(true);
                    else {
                        d.sourceAvailable(false);
                        d.step(c.budget());
                        ComparisonExperiment.require(d.state().equals("FAILED"), "source failure not detected");
                        failedTick = tick;
                    }
                } else {
                    d.abort();
                    observation.abortAttempts++;
                    if (!boundary.cancelsBeforeCommit()) {
                        ComparisonExperiment.require(before.equals(d.state()), "post-commit abort changed state");
                        observation.abortRefused++;
                    }
                    if (boundary == Boundary.FINAL_ACK_WAIT_ABORT) {
                        ComparisonExperiment.require(d.commits() == 0, "commit without final ACK");
                    }
                }
            }
            if (boundary.sourceFailure() && injectionTick >= 0 && !observation.resumed) {
                if (boundary == Boundary.PULL_TIMEOUT_RESUME && failedTick < 0) {
                    if (tick - injectionTick >= OUTAGE_TICKS) {
                        d.fail();
                        failedTick = tick;
                        observation.event(tick, "controller-timeout");
                    } else observation.timeoutWaitTicks++;
                }
                if (failedTick >= 0) {
                    ComparisonExperiment.require(d.state().equals("FAILED") && d.committed && d.frozen()
                            && d.cleanupCalls() == 0 && retainedSource.equals(d.source()), "failure lost source or ownership");
                    if (!observation.failureProbed) {
                        observation.request(new Command.Set(Bytes.utf8("s6-write-during-outage"), MigrationFixture.value(-2)));
                        observation.request(new Command.Delete(MigrationFixture.key(seed, 1)));
                        observation.request(new Command.Get(Bytes.utf8("s6-write-during-outage")));
                        observation.request(new Command.Get(MigrationFixture.key(seed, 1)));
                        Bytes unresolved = d.source().keySet().stream().filter(key -> !d.destination().containsKey(key)
                                && oracle.containsKey(key)).findFirst().orElseThrow();
                        Result read = observation.request(new Command.Get(unresolved));
                        ComparisonExperiment.require(read.status() == ResultStatus.UNAVAILABLE, "unresolved failure read is not UNAVAILABLE");
                        d.abort();
                        observation.abortAttempts++;
                        ComparisonExperiment.require(d.state().equals("FAILED"), "failed destination rolled back");
                        observation.abortRefused++;
                        observation.failureProbed = true;
                    }
                    if (tick - failedTick >= OUTAGE_TICKS && boundary != Boundary.SOURCE_DOWN_UNRECOVERED) {
                        d.sourceAvailable(true);
                        d.dropPull(false);
                        d.resume();
                        observation.resumed = true;
                        observation.event(tick, "source-and-fence-restored");
                    } else observation.failedTicks++;
                }
            }
            Result probe = d.request(new Command.Delete(ComparisonExperiment.PROBE));
            if (probe.status() == ResultStatus.BUSY) observation.blockedTicks++;
            else ComparisonExperiment.require(probe.status() == ResultStatus.MISS, "control request changed data");
            for (Command command : trace.at(tick)) observation.request(command);
            checkOwnership(d);
            ComparisonExperiment.require(injectionTick >= 0 || !d.done(), "migration finished before requested S6 boundary");
            if (d.state().equals("FAILED")) {
                for (var entry : d.destination().entrySet()) ComparisonExperiment.require(entry.getValue().equals(oracle.get(entry.getKey())), "failed destination lost successful write");
            }
            if ((injectionTick >= 0 && (d.done() || d.state().equals("ABORTED")))
                    || (boundary == Boundary.SOURCE_DOWN_UNRECOVERED && failedTick >= 0 && tick - failedTick >= OUTAGE_TICKS)) {
                terminalTick = tick;
                break;
            }
            long bytes = d.bytes();
            if (d.ready()) {
                d.commit();
                observation.event(tick + 1, "ownership-commit");
            } else d.step(c.budget());
            ComparisonExperiment.require(d.bytes() >= bytes && d.bytes() - bytes <= c.budget(), "byte budget exceeded");
            if (!before.equals(d.state())) observation.event(tick + 1, "state-change");
        }
        ComparisonExperiment.require(injectionTick >= 0 && terminalTick >= 0 && boundary.terminal().equals(d.state()), "unreached S6 boundary or timeout: " + boundary + "/" + variant);
        checkOwnership(d);
        int sourceExact = 0, destinationExact = 0, retainedExact = 0;
        if (boundary.cancelsBeforeCommit()) {
            ComparisonExperiment.require(!d.committed && d.commits() == 0 && !d.frozen()
                    && oracle.equals(d.source()) && d.staging().isEmpty(), "pre-commit cancellation lost data or staging");
            sourceExact = 1;
            ComparisonExperiment.require(d.request(LiveMigrationFixture.DESTINATION, new Command.Get(MigrationFixture.key(seed, 0)))
                    .redirect().orElseThrow().target().equals(LiveMigrationFixture.SOURCE), "abort routing");
        } else {
            ComparisonExperiment.require(d.commits() == 1 && d.request(LiveMigrationFixture.SOURCE,
                    new Command.Get(MigrationFixture.key(seed, 0))).status() == ResultStatus.MOVED, "source fencing");
            if (boundary == Boundary.SOURCE_DOWN_UNRECOVERED) {
                ComparisonExperiment.require(d.frozen() && retainedSource.equals(d.source()) && d.cleanupCalls() == 0, "unrecovered source was discarded");
                var expectedDestination = new LinkedHashMap<Bytes, Bytes>();
                for (var entry : oracle.entrySet()) {
                    Result read = d.request(new Command.Get(entry.getKey()));
                    if (read.status() == ResultStatus.OK) expectedDestination.put(entry.getKey(), entry.getValue());
                    else ComparisonExperiment.require(read.status() == ResultStatus.UNAVAILABLE, "missing key silently recovered");
                }
                ComparisonExperiment.require(expectedDestination.equals(d.destination()), "retained destination exact comparison");
                retainedExact = 1;
            } else {
                ComparisonExperiment.require(d.source().isEmpty() && !d.frozen() && oracle.equals(d.destination()), "completed recovery data mismatch");
                destinationExact = 1;
            }
        }
        if (boundary == Boundary.PARTIAL_CLEANUP_ABORT) ComparisonExperiment.require(d.cleanupCalls() == 3 && observation.abortRefused == 1, "cleanup retry not exercised");
        if (boundary.sourceFailure()) ComparisonExperiment.require(observation.readUnavailable > 0 && observation.failureProbed, "failure contract not exercised");
        observation.event(terminalTick, "expected-terminal");
        Map<String, Object> metrics = new LinkedHashMap<>();
        metric(metrics, "total_ticks", terminalTick + 1, "tick");
        metric(metrics, "injection_tick", injectionTick, "tick");
        metric(metrics, "action_to_terminal_ticks", terminalTick - injectionTick, "tick");
        metric(metrics, "writes_blocked_ticks", observation.blockedTicks, "tick");
        metric(metrics, "sent_bytes", d.bytes(), "bytes");
        metric(metrics, "writes_accepted", observation.accepted, "count");
        metric(metrics, "writes_busy", observation.writeBusy, "count");
        metric(metrics, "reads_busy", observation.readBusy, "count");
        metric(metrics, "reads_unavailable", observation.readUnavailable, "count");
        metric(metrics, "failed_observation_ticks", observation.failedTicks, "tick");
        metric(metrics, "timeout_wait_ticks", observation.timeoutWaitTicks, "tick");
        metric(metrics, "abort_attempts", observation.abortAttempts, "count");
        metric(metrics, "abort_refused", observation.abortRefused, "count");
        metric(metrics, "cleanup_calls", d.cleanupCalls(), "count");
        metric(metrics, "ownership_commits", d.commits(), "count");
        metric(metrics, "source_exact_comparisons", sourceExact, "count");
        metric(metrics, "destination_exact_comparisons", destinationExact, "count");
        metric(metrics, "retained_state_exact_comparisons", retainedExact, "count");
        metric(metrics, "invariant_violations", 0, "count");
        if (boundary == Boundary.CATCHING_UP_PARTIAL_ABORT) {
            metric(metrics, "snapshot_bytes_before_delta", snapshotBytes, "bytes");
            metric(metrics, "delta_bytes_before_abort", deltaBytesBeforeAbort, "bytes");
            metric(metrics, "delta_records_acknowledged_before_abort", deltaAcknowledgedBeforeAbort, "count");
        }
        return new Run(d.state(), injectionState, workloadHash(c, seed), Map.copyOf(metrics), List.copyOf(observation.events));
    }

    private static boolean atBoundary(ComparisonExperiment.Driver d, Boundary boundary) {
        return switch (boundary) {
            case COPYING_ABORT -> d.state().equals("COPYING") && d.bytes() > 0;
            case BEFORE_COMMIT_ABORT -> d.ready();
            case FINAL_ACK_WAIT_ABORT -> d.finalAckWaiting();
            case AFTER_COMMIT_ABORT -> d.committed;
            case PARTIAL_CLEANUP_ABORT -> d.state().equals("CLEANING_UP") && d.cleanupCalls() > 0;
            case CATCHING_UP_ABORT -> d.state().equals("CATCHING_UP");
            case CATCHING_UP_PARTIAL_ABORT -> d.partialLiveDelta();
            case SOURCE_DOWN_RESUME, PULL_TIMEOUT_RESUME, SOURCE_DOWN_UNRECOVERED -> d.state().equals("PULLING") && d.bytes() > 0;
        };
    }

    private static void checkOwnership(ComparisonExperiment.Driver d) {
        var tablet = d.table().get(LiveMigrationFixture.MOVING);
        ComparisonExperiment.require(d.commits() == (d.committed ? 1 : 0)
                && d.table().version().value() == (d.committed ? 1 : 0)
                && tablet.epoch().value() == (d.committed ? 1 : 0)
                && tablet.owner().equals(d.committed ? LiveMigrationFixture.DESTINATION : LiveMigrationFixture.SOURCE), "owner/epoch invariant");
    }

    private static void metric(Map<String, Object> metrics, String name, Number value, String unit) {
        metrics.put(name, Map.of("value", value, "unit", unit));
    }

    private static final class Observation {
        final ComparisonExperiment.Driver d;
        final Map<Bytes, Bytes> oracle;
        final List<Map<String, Object>> events = new ArrayList<>();
        long blockedTicks, accepted, writeBusy, readBusy, readUnavailable;
        int failedTicks, timeoutWaitTicks, abortAttempts, abortRefused;
        boolean resumed, failureProbed;

        Observation(ComparisonExperiment.Driver d, Map<Bytes, Bytes> oracle) { this.d = d; this.oracle = oracle; }

        void event(int tick, String action) {
            var event = new LinkedHashMap<String, Object>();
            event.putAll(Map.of("tick", tick, "action", action, "state", d.state(), "committed", d.committed,
                    "sentBytes", d.bytes(), "round", d.rounds(), "owner", d.committed ? "destination" : "source"));
            event.put("acknowledgedSequence", d.acknowledgedSequence());
            event.put("partialLiveDelta", d.partialLiveDelta());
            events.add(Map.copyOf(event));
        }

        Result request(Command command) {
            Result result = d.request(command);
            switch (command) {
                case Command.Get get -> {
                    if (result.status() == ResultStatus.BUSY || result.status() == ResultStatus.UNAVAILABLE) {
                        ComparisonExperiment.require(d.committed && !d.done() && oracle.containsKey(get.key()), "unexpected unavailable read");
                        if (result.status() == ResultStatus.BUSY) readBusy++; else readUnavailable++;
                    } else if (oracle.containsKey(get.key())) ComparisonExperiment.require(result.status() == ResultStatus.OK
                            && oracle.get(get.key()).equals(result.value().orElseThrow()), "stale/false-miss read");
                    else ComparisonExperiment.require(result.status() == ResultStatus.MISS, "deleted key resurrected");
                }
                case Command.Set set -> {
                    if (result.status() == ResultStatus.BUSY) writeBusy++;
                    else {
                        ComparisonExperiment.require(result.status() == ResultStatus.OK, "SET outcome");
                        oracle.put(set.key(), set.value()); accepted++;
                    }
                }
                case Command.Delete delete -> {
                    if (result.status() == ResultStatus.BUSY) writeBusy++;
                    else {
                        boolean existed = oracle.remove(delete.key()) != null;
                        ComparisonExperiment.require(result.status() == (existed ? ResultStatus.OK : ResultStatus.MISS), "DELETE outcome");
                        accepted++;
                    }
                }
                default -> throw new IllegalArgumentException("CRUD only");
            }
            return result;
        }
    }

    private InterruptionExperiment() {}
}
