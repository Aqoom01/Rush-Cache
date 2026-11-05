package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.migration.postcopy.PostCopyState;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;

/** 실제 세 protocol을 실행하는 독립 drain·operator·결정적 장애 주입 연구 fixture. */
final class ExecutionSafetyExperiment {
    static final int JOBS = 4;
    static final int MAX_TICKS = 5000;
    static final List<String> SCYLLA = List.of("normal", "no-capacity", "replica-floor", "peer-down",
            "before-source", "after-commit", "cleanup-retry");
    static final List<String> MONGO = List.of("normal", "no-capacity", "balancer-paused", "jumbo",
            "db-primary", "before-source", "after-commit", "cleanup-retry", "controller-reload");
    static final List<String> PD = List.of("before-source", "before-destination", "after-commit", "timeout",
            "cleanup-retry", "duplicate-cancel", "shared-source", "shared-destination", "unrecovered-source");
    record Case(String method, String scenario, String variant, int keys, long seed) {
        Map<String, Object> config() {
            return Map.of("method", method, "scenario", scenario, "variant", variant, "keysPerTablet", keys,
                    "seed", seed, "jobs", JOBS, "clusterBytesPerTick", 4096, "nodeBytesPerTick", 1024,
                    "maxJobTicks", 200, "maxTicks", MAX_TICKS);
        }
    }

    static String resolveFault(Case c) {
        return c.scenario().equals("seeded-fault")
                ? List.of("before-source", "before-destination", "after-commit", "timeout", "cleanup-retry", "duplicate-cancel")
                    .get(new SplittableRandom(c.seed()).nextInt(6)) : c.scenario();
    }
    static List<Change> planFor(Case c) {
        boolean drain = c.method().endsWith("-drain");
        String fault = resolveFault(c);
        List<Change> result = new ArrayList<>();
        for (int i = 0; i < JOBS; i++) {
            long source = drain ? (i == 0 ? 1 : 2) : fault.equals("shared-source") && i == 2 ? 2 : i + 1;
            long destination = fault.equals("shared-destination") && i == 2 ? 102 : 101 + i;
            result.add(new Change(new TabletId(i), new NodeId(source), new NodeId(destination)));
        }
        return result;
    }

    static List<Case> cases(String profile) {
        if (!List.of("full", "quick").contains(profile)) throw new IllegalArgumentException("unknown profile");
        int seeds = profile.equals("full") ? 5 : 2, keys = profile.equals("full") ? 128 : 48;
        List<Case> result = new ArrayList<>();
        for (String method : List.of("scylla-drain", "mongodb-drain", "pd-operator")) {
            var scenarios = method.equals("scylla-drain") ? SCYLLA : method.equals("mongodb-drain") ? MONGO : PD;
            for (String scenario : scenarios) for (String variant : MovementExperiment.VARIANTS)
                for (int seed = 1; seed <= seeds; seed++) result.add(new Case(method, scenario, variant, keys, seed));
        }
        for (String variant : MovementExperiment.VARIANTS)
            for (int seed = 1; seed <= (profile.equals("full") ? 20 : 4); seed++)
                result.add(new Case("foundationdb-fault", "seeded-fault", variant, keys, seed));
        return List.copyOf(result);
    }

    static Map<String, Object> run(Case condition) { return new Trial(condition).run(); }

    private static final class Trial {
        final Case c;
        final boolean drain;
        final String fault;
        final MultiMigrationFixture f;
        final List<Driver> drivers = new ArrayList<>();
        final Set<NodeId> unavailable = new HashSet<>();
        final List<Map<String, Object>> events = new ArrayList<>();
        final Map<Bytes, Bytes> guard;
        final Map<NodeId, Long> freeBytes = new LinkedHashMap<>();
        final Set<TabletId> jumbo = new HashSet<>();
        final boolean balancerEnabled;
        final int replicaMinimum;
        final int nodesBefore, unavailableBefore;
        final long incomingBytes;
        String drainState = "draining";
        int tick, injectionTick = -1, recoveryTick = -1, reloads, adminWait;
        long busy, unready, accepted, reads;
        final long[] consumed = new long[JOBS];
        final Map<Integer, Map<Bytes, java.util.Optional<Bytes>>> protectedWrites = new LinkedHashMap<>();
        boolean injected, removed, databasePrimary;

        Trial(Case condition) {
            c = condition; drain = c.method().endsWith("-drain");
            fault = resolveFault(c);
            f = new MultiMigrationFixture(planFor(c), java.util.Collections.nCopies(JOBS, c.keys()), c.seed(),
                    c.variant(), 1024, 256);
            guard = Map.copyOf(f.guardData);
            for (int i = 0; i < JOBS; i++) {
                drivers.add(new Driver(i)); protectedWrites.put(i, new LinkedHashMap<>());
            }
            incomingBytes = f.handles.stream().skip(1).mapToLong(h -> h.sourceData.size() * 152L).sum();
            for (var h : f.handles) freeBytes.put(h.change.to(), (long) h.sourceData.size() * 152);
            if (fault.equals("no-capacity")) freeBytes.put(f.changes.getLast().to(), 0L);
            if (fault.equals("jumbo")) jumbo.add(f.changes.getLast().unit());
            if (fault.equals("peer-down")) unavailable.add(f.changes.getFirst().to());
            balancerEnabled = !fault.equals("balancer-paused");
            replicaMinimum = fault.equals("replica-floor") ? f.nodes.size() : 1;
            nodesBefore = f.nodes.size(); unavailableBefore = unavailable.size();
            databasePrimary = c.scenario().equals("db-primary");
        }

        void event(String kind, int job, String detail) {
            events.add(Map.of("order", events.size(), "tick", tick, "kind", kind, "job", job, "detail", detail,
                    "routingVersion", f.routing.get().version().value()));
        }
        boolean published(int id) {
            var h = f.handles.get(id);
            return f.routing.get().get(h.change.unit()).owner().equals(h.change.to());
        }
        void requests() {
            for (int id = 0; id < JOBS; id++) {
                var h = f.handles.get(id);
                var rng = new SplittableRandom(c.seed() * 1000003 + tick * 7919L + id * 97L);
                Bytes key = h.keys.get(rng.nextInt(h.keys.size()));
                Command write = rng.nextInt(100) < 30 ? new Command.Delete(key)
                        : new Command.Set(key, MigrationFixture.value(((long) tick << 16) ^ id));
                for (Command command : List.of(write, new Command.Get(h.keys.get(rng.nextInt(h.keys.size()))))) {
                    var owner = f.routing.get().get(h.change.unit()).owner();
                    Result r = unavailable.contains(owner) ? h.result(ResultStatus.UNAVAILABLE) : h.request(command);
                    if (r.status() == ResultStatus.BUSY) { busy++; continue; }
                    if (r.status() == ResultStatus.UNAVAILABLE) { unready++; continue; }
                    switch (command) {
                        case Command.Set set -> {
                            require(r.status() == ResultStatus.OK, "SET failure"); h.oracle.put(key, set.value()); accepted++;
                            if (published(id)) protectedWrites.get(id).put(key, java.util.Optional.of(set.value()));
                        }
                        case Command.Delete delete -> {
                            boolean existed = h.oracle.remove(key) != null;
                            require(r.status() == (existed ? ResultStatus.OK : ResultStatus.MISS), "DELETE outcome"); accepted++;
                            if (published(id)) protectedWrites.get(id).put(key, java.util.Optional.empty());
                        }
                        case Command.Get get -> {
                            reads++;
                            Bytes value = h.oracle.get(get.key());
                            require(value == null ? r.status() == ResultStatus.MISS
                                    : r.status() == ResultStatus.OK && r.value().orElseThrow().equals(value), "stale/false-miss read");
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            }
        }

        final class Driver implements MigrationScheduler.Driver {
            final int id;
            boolean failed, stall, recovering;
            Driver(int id) { this.id = id; }
            MultiMigrationFixture.Handle h() { return f.handles.get(id); }
            @Override public void begin() {
                if (recovering && published(id)) {
                    if (h().post != null && h().post.state() == PostCopyState.FAILED) h().post.resume();
                } else h().begin();
                if (id == 1 && fault.equals("cleanup-retry") && !injected) {
                    h().cleanupFailures = 3; h().partialCleanup = true; mark("cleanup-retry-configured");
                }
            }
            void mark(String detail) {
                injected = true; injectionTick = tick;
                event("fault", id, detail + ":" + h().state() + ":bytes=" + h().sentBytes());
            }
            void interrupt() {
                if (h().post != null) { if (published(id)) h().post.fail(); else h().post.abort(); }
                else if (h().pre != null) h().pre.abort();
                else h().stop.abort();
                failed = true;
            }
            @Override public void step(long budget) {
                if (!recovering && !injected && id == 1) {
                    boolean before = !published(id) && (h().post != null || h().sentBytes() > 0);
                    boolean after = published(id) && !h().done();
                    boolean isBefore = List.of("before-source", "before-destination", "duplicate-cancel",
                            "shared-source", "shared-destination").contains(fault);
                    if (fault.equals("unrecovered-source")) isBefore = h().post == null;
                    if (isBefore && before || fault.equals("after-commit") && after
                            || fault.equals("unrecovered-source") && h().post != null && after) {
                        mark(fault);
                        boolean destination = fault.contains("destination");
                        unavailable.add(destination ? h().change.to() : h().change.from());
                        interrupt();
                        if (fault.equals("duplicate-cancel")) { interrupt(); event("duplicate-cancel", id, "idempotent"); }
                        return;
                    }
                    if (fault.equals("timeout")) { mark("stalled-before-cutover"); stall = true; }
                }
                if (!recovering && (unavailable.contains(h().change.from()) || unavailable.contains(h().change.to()))) {
                    interrupt(); return;
                }
                if (!stall) h().step(budget);
            }
            @Override public long sentBytes() { return h().sentBytes(); }
            @Override public boolean done() { return h().done(); }
            @Override public boolean failed() { return failed || h().failed(); }
            @Override public String state() { return h().state(); }
            @Override public void stop() { interrupt(); event("timeout", id, "protocol retained/aborted by ownership phase"); }
        }

        List<MigrationScheduler.JobView> execute(List<Change> plan, String phase) {
            if (plan.isEmpty()) return List.of();
            int endpoint = c.method().equals("mongodb-drain") ? 1 : 2;
            var scheduler = new MigrationScheduler(plan, new MigrationScheduler.Settings(4, endpoint, 4096, 1024, 200),
                    change -> drivers.get((int) change.unit().value()));
            var previous = new java.util.HashMap<Integer, String>();
            for (; tick < MAX_TICKS && !scheduler.finished(); tick++) {
                requests();
                var observed = scheduler.step();
                var budgets = new MovementExperiment.Case("safety", "safety", JOBS, 4, endpoint, "independent",
                        c.keys(), 4096, 1024, 1024, 1024, 256, .1, c.variant());
                MovementExperiment.checkBudgets(observed, budgets);
                for (var job : observed.jobs()) {
                    int id = (int) job.change().unit().value();
                    String state = job.status() + ":" + job.state();
                    if (!state.equals(previous.put(id, state))) event(phase, id, state);
                    consumed[id] += observed.consumed().getOrDefault(job.index(), 0L);
                }
                if (c.scenario().equals("controller-reload") && reloads == 0 && f.commits > 0) {
                    var journal = BaselineJson.object(BaselineJson.decode(BaselineJson.encode(
                            Map.of("state", drainState, "remaining", f.routing.get().ownedBy(new NodeId(2)).size(),
                                    "databasePrimary", databasePrimary))));
                    require("draining".equals(journal.get("state")), "drain journal lost");
                    drainState = (String) journal.get("state");
                    databasePrimary = (Boolean) journal.get("databasePrimary");
                    reloads++; event("controller-reloaded", -1, "state=" + drainState
                            + ";remaining=" + journal.get("remaining") + ";databasePrimary=" + databasePrimary);
                }
            }
            require(scheduler.finished(), "safety simulation deadline");
            return scheduler.jobs();
        }

        Map<String, Object> run() {
            long free = f.changes.stream().skip(1).map(Change::to).distinct().mapToLong(freeBytes::get).sum();
            boolean capacityFits = free >= incomingBytes;
            boolean replicasFit = f.nodes.size() - 1 >= replicaMinimum;
            if (drain && c.method().equals("scylla-drain") && (!capacityFits || !replicasFit || !unavailable.isEmpty())) {
                event("preflight-rejected", -1, !capacityFits ? "insufficient-space" : !replicasFit ? "replica-floor" : "peer-unavailable");
                drainState = "rejected";
                for (var h : f.handles) require(h.oracle.equals(h.sourceData) && h.session == null, "rejected plan changed data");
                return result("REJECTED", 0, 0, 0);
            }
            var selected = f.changes.stream().filter(change -> {
                if (!c.method().equals("mongodb-drain") || change.unit().value() == 0) return true;
                return balancerEnabled && !jumbo.contains(change.unit())
                        && f.handle(change).sourceData.size() * 152L <= freeBytes.get(change.to());
            }).toList();
            var initial = execute(selected, "initial");
            var failed = initial.stream().filter(j -> j.status() != MigrationScheduler.Status.DONE).toList();
            long healthyCompleted = initial.stream().filter(j -> j.change().unit().value() != 1
                    && j.status() == MigrationScheduler.Status.DONE).count();
            if (fault.equals("unrecovered-source")) {
                require(injected && !failed.isEmpty() && healthyCompleted == JOBS - 1, "permanent failure not isolated");
                event("held-unrecovered", 1, "no ownership rollback/no node removal");
                return result("HELD", initial.size(), healthyCompleted, failed.size());
            }
            if (!failed.isEmpty()) {
                recoveryTick = tick; unavailable.clear();
                for (var j : failed) {
                    var d = drivers.get((int) j.change().unit().value());
                    d.failed = false; d.stall = false; d.recovering = true;
                    if (!published(d.id)) { d.h().stop = null; d.h().pre = null; d.h().post = null; d.h().session = null; }
                }
                event("recovery", -1, "same data and fence restored");
                var retried = execute(failed.stream().map(MigrationScheduler.JobView::change).toList(), "recovery");
                require(retried.stream().allMatch(j -> j.status() == MigrationScheduler.Status.DONE), "recovery did not finish");
            }
            if (List.of("before-source", "before-destination", "after-commit", "timeout", "duplicate-cancel",
                    "shared-source", "shared-destination", "cleanup-retry").contains(fault)) require(injected, "requested boundary not exercised");
            if (fault.equals("cleanup-retry")) require(f.handles.get(1).cleanupCalls == 4, "cleanup retries not exercised");
            if (drain) {
                if (databasePrimary) {
                    event("drain-wait", -1, "database primary metadata still present");
                    for (int i = 0; i < 5; i++) { requests(); tick++; adminWait++; }
                    databasePrimary = false; event("admin-ack", -1, "database primary moved");
                }
                var victim = new NodeId(2);
                boolean empty = f.routing.get().ownedBy(victim).isEmpty() && f.nodes.get(victim).tablets.isEmpty();
                if (empty && !databasePrimary) {
                    require(drainState.equals("draining"), "drain journal phase");
                    f.nodes.remove(victim); removed = true; drainState = "removed"; event("node-removed", -1, "drained");
                }
                else { require(!removed, "removed nonempty node"); event("drain-blocked", -1, fault); }
            }
            return result(drain ? removed ? "REMOVED" : "BLOCKED" : "COMPLETED", initial.size(), healthyCompleted, failed.size());
        }

        Map<String, Object> result(String outcome, long started, long healthyCompleted, long failures) {
            require(f.guardData.equals(guard) && f.routing.get().get(f.guard).epoch().value() == 0, "unrelated tablet changed");
            require(f.routing.get().version().value() == f.commits, "routing commit lost");
            List<Map<String, Object>> jobs = new ArrayList<>();
            for (int id = 0; id < JOBS; id++) {
                var h = f.handles.get(id);
                boolean committed = published(id), complete = h.done();
                require(f.routing.get().get(h.change.unit()).epoch().value() == (committed ? 1 : 0), "epoch changed twice");
                if (complete) {
                    require(h.oracle.equals(h.finalData()) && h.sourceData.isEmpty(), "completed data mismatch");
                    require(h.request(h.change.from(), new Command.Get(h.keys.getFirst())).status() == ResultStatus.MOVED, "old owner not fenced");
                } else if (!committed) require(h.oracle.equals(h.sourceData), "uncommitted source data lost");
                else {
                    require(outcome.equals("HELD") && h.post != null && h.post.state() == PostCopyState.FAILED, "unexpected incomplete publication");
                    for (var e : h.oracle.entrySet()) {
                        var r = h.request(new Command.Get(e.getKey()));
                        require(r.status() == ResultStatus.UNAVAILABLE || r.status() == ResultStatus.OK
                                && r.value().orElseThrow().equals(e.getValue()), "failed destination false MISS/stale read");
                    }
                    for (var e : h.post.data().entrySet()) require(e.getValue().equals(h.oracle.get(e.getKey())), "successful destination write lost");
                    for (var e : protectedWrites.get(id).entrySet()) {
                        var r = h.request(new Command.Get(e.getKey()));
                        require(e.getValue().isEmpty() ? r.status() == ResultStatus.MISS
                                : r.status() == ResultStatus.OK && r.value().orElseThrow().equals(e.getValue().orElseThrow()),
                                "acknowledged post-cutover write/delete lost");
                    }
                    require(!h.sourceData.isEmpty(), "unrecoverable source cleaned");
                }
                jobs.add(Map.of("job", id, "state", h.state(), "published", committed, "complete", complete,
                        "sourceRecords", h.sourceData.size(), "sentBytes", consumed[id], "cleanupCalls", h.cleanupCalls,
                        "oracleHash", MovementExperiment.dataHash(h.oracle),
                        "observedHash", MovementExperiment.dataHash(complete ? h.finalData() : h.sourceData)));
            }
            long remaining = f.routing.get().ownedBy(new NodeId(2)).size();
            require(!removed || remaining == 0 && !databasePrimary, "unsafe removal");
            Map<String, Object> metrics = new LinkedHashMap<>();
            metrics.put("total_ticks", tick); metrics.put("sent_bytes", java.util.Arrays.stream(consumed).sum());
            metrics.put("initial_jobs", started); metrics.put("initial_failed_jobs", failures); metrics.put("healthy_jobs_completed_before_recovery", healthyCompleted);
            metrics.put("ownership_commits", f.commits); metrics.put("remaining_tablets", remaining); metrics.put("node_removed", removed ? 1 : 0);
            metrics.put("writes_accepted", accepted); metrics.put("busy", busy); metrics.put("unavailable", unready);
            metrics.put("reads_checked", reads); metrics.put("controller_reloads", reloads); metrics.put("admin_wait_ticks", adminWait);
            metrics.put("injection_tick", injectionTick < 0 ? null : injectionTick);
            metrics.put("recovery_start_tick", recoveryTick < 0 ? null : recoveryTick);
            metrics.put("invariant_violations", 0);
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("condition", c.config()); root.put("resolvedFault", fault); root.put("outcome", outcome);
            root.put("plan", SchedulerComparisonExperiment.changes(f.changes));
            root.put("preflight", Map.of("incomingBytes", incomingBytes,
                    "freeDestinationBytes", freeBytes.entrySet().stream().filter(e -> !e.getKey().equals(f.changes.getFirst().to()))
                            .mapToLong(Map.Entry::getValue).sum(),
                    "replicaMinimum", replicaMinimum, "balancerEnabled", balancerEnabled,
                    "jumboChunks", jumbo.size(), "drainState", drainState, "nodesBefore", nodesBefore,
                    "unavailableBefore", unavailableBefore));
            root.put("inputHash", MovementExperiment.hash(c.config()));
            root.put("inputHashKind", "seeded-request-generator-and-fault-specification");
            root.put("metrics", metrics); root.put("jobs", jobs); root.put("events", events);
            root.put("evidenceHash", MovementExperiment.hash(root));
            return root;
        }
    }

    private static void require(boolean condition, String message) { MovementExperiment.require(condition, message); }
    private ExecutionSafetyExperiment() {}
}
