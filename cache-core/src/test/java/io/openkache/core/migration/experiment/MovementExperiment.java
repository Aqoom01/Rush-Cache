package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;

/** 같은 고정 Change·외부 요청 생성 규격으로 실제 protocol을 호출하는 M1~M5 suite. */
final class MovementExperiment {
    static final String GENERATOR = "b3-multi-job-trace-1";
    static final int MAX_TICKS = 20_000;
    static final List<String> VARIANTS = List.of("stop-and-copy", "pre-copy", "post-copy");

    record Case(String id, String scenario, int jobs, int concurrency, int endpointLimit, String endpoints,
                int keys, long clusterBudget, long nodeBudget, long referenceBudget, int batchBytes, long threshold,
                double rho, String variant) {
        Case {
            if (jobs < 1 || concurrency < 1 || endpointLimit < 1 || keys < 1 || clusterBudget < 1 || nodeBudget < 1
                    || referenceBudget < 1 || batchBytes <= 37 || threshold < 0 || !Double.isFinite(rho) || rho < 0
                    || !VARIANTS.contains(variant) || !List.of("independent", "shared-source", "shared-destination").contains(endpoints)) {
                throw new IllegalArgumentException("invalid movement case");
            }
        }
        Map<String, Object> config() {
            var values = new TreeMap<String, Object>();
            values.put("jobs", jobs); values.put("maxConcurrentJobs", concurrency); values.put("maxJobsPerNode", endpointLimit);
            values.put("endpoints", endpoints); values.put("keysPerTablet", keys); values.put("clusterBytesPerTick", clusterBudget);
            values.put("nodeBytesPerTick", nodeBudget); values.put("referenceBytesPerTick", referenceBudget);
            values.put("batchBytes", batchBytes); values.put("stopThresholdBytes", threshold); values.put("maxRounds", 10);
            values.put("rho", rho); values.put("variant", variant); values.put("maxTicks", MAX_TICKS);
            values.put("deletePercent", 30); values.put("readsPerJobPerTick", 2); values.put("recordBytes", 152);
            return values;
        }
    }

    record Run(Map<String, Object> metrics, List<Map<String, Object>> jobs,
               List<Map<String, Object>> budgets, List<Map<String, Object>> events) {}

    static List<Case> cases(String profile) {
        if (!List.of("full", "quick").contains(profile)) throw new IllegalArgumentException("invalid profile");
        boolean quick = profile.equals("quick");
        int keys = quick ? 128 : 2048;
        long node = quick ? 1024 : 16384, cluster = node * 4;
        List<Case> result = new ArrayList<>();
        for (int jobs : new int[] {1, 4, 16, 64}) result.add(condition("M1-j" + jobs, "M1", jobs, 4, 1, "independent", keys, cluster, node, node, .1, "stop-and-copy"));
        for (int concurrency : new int[] {1, 2, 4, 8}) result.add(condition("M2-c" + concurrency, "M2", 16, concurrency, 1, "independent", keys, cluster, cluster, node, .1, "stop-and-copy"));
        for (String endpoints : List.of("independent", "shared-source", "shared-destination")) {
            for (int limit : new int[] {1, 4}) result.add(condition("M3-" + endpoints + "-e" + limit, "M3", 16, 4, limit, endpoints, keys, cluster, node, node, .1, "stop-and-copy"));
        }
        for (double rho : new double[] {0, .5, .9, 1.2}) result.add(condition("M4-r" + rho, "M4", 16, 4, 1, "independent", keys, cluster, node, node, rho, "pre-copy"));
        for (String variant : VARIANTS) {
            for (int concurrency : new int[] {1, 2, 4, 8}) result.add(condition("M5-M2-c" + concurrency + "-" + variant, "M5", 16, concurrency, 1, "independent", keys, cluster, cluster, node, .1, variant));
            for (double rho : new double[] {0, .5, .9, 1.2}) result.add(condition("M5-M4-r" + rho + "-" + variant, "M5", 16, 4, 1, "independent", keys, cluster, node, node, rho, variant));
        }
        return List.copyOf(result);
    }

    private static Case condition(String id, String scenario, int jobs, int concurrency, int endpointLimit, String endpoints,
                                  int keys, long cluster, long node, long reference, double rho, String variant) {
        return new Case(id, scenario, jobs, concurrency, endpointLimit, endpoints, keys, cluster, node, reference,
                Math.toIntExact(reference), reference / 4, rho, variant);
    }

    static String workloadHash(Case c, long seed) {
        // Scheduler/variant는 요청 수·선택 seed를 바꾸지 않는다. 이는 실제 요청열 전체가 아닌 생성 규격 hash다.
        return hash(Map.of("generator", GENERATOR, "seed", seed, "jobs", c.jobs(), "keys", c.keys(),
                "rho", c.rho(), "referenceBudget", c.referenceBudget(), "maxTicks", MAX_TICKS));
    }

    static Run run(Case c, long seed) {
        var f = new MultiMigrationFixture(c.jobs(), c.keys(), seed, c.endpoints(), c.variant(), c.batchBytes(), c.threshold());
        var scheduler = new MigrationScheduler(f.changes, new MigrationScheduler.Settings(c.concurrency(), c.endpointLimit(),
                c.clusterBudget(), c.nodeBudget(), 5000), f::handle);
        List<Map<String, Object>> budgets = new ArrayList<>(), events = new ArrayList<>();
        String[] states = new String[c.jobs()];
        int[] rounds = new int[c.jobs()];
        for (int i = 0; i < c.jobs(); i++) states[i] = "PENDING";
        int tick = 0;
        for (; tick < MAX_TICKS && !scheduler.finished(); tick++) {
            for (int index = 0; index < c.jobs(); index++) {
                var h = f.handles.get(index);
                if (f.routing.get().get(h.change.unit()).owner().equals(h.change.from()) && !h.sourceWritable()) h.blockedTicks++;
                requests(h, c, seed, index, tick);
            }
            var observation = scheduler.step();
            checkBudgets(observation, c);
            List<Map<String, Object>> grants = new ArrayList<>();
            Map<String, Object> nodes = new TreeMap<>();
            for (var job : observation.jobs()) {
                if (!observation.allocated().containsKey(job.index())) continue;
                long grant = observation.allocated().get(job.index()), used = observation.consumed().get(job.index());
                grants.add(Map.of("job", job.index(), "allocatedBytes", grant, "consumedBytes", used));
                for (NodeId node : List.of(job.change().from(), job.change().to())) {
                    @SuppressWarnings("unchecked")
                    var row = (Map<String, Long>) nodes.computeIfAbsent(Long.toString(node.value()), ignored -> new LinkedHashMap<>(Map.of("allocatedBytes", 0L, "consumedBytes", 0L)));
                    row.merge("allocatedBytes", grant, Math::addExact); row.merge("consumedBytes", used, Math::addExact);
                }
            }
            budgets.add(Map.of("tick", tick, "grants", grants, "nodes", nodes));
            for (var job : observation.jobs()) {
                var h = f.handles.get(job.index());
                h.recordDirty(tick + 1L);
                if (job.startTick() >= 0) h.startTick = job.startTick();
                if (h.commitTick < 0 && f.routing.get().get(h.change.unit()).owner().equals(h.change.to())) h.commitTick = tick + 1L;
                if (!states[job.index()].equals(h.state()) || rounds[job.index()] != h.rounds()) {
                    events.add(Map.of("tick", tick + 1, "job", job.index(), "state", h.state(), "round", h.rounds(), "sentBytes", h.sentBytes()));
                    states[job.index()] = h.state(); rounds[job.index()] = h.rounds();
                }
            }
        }
        require(scheduler.finished(), "suite model timeout");
        scheduler.jobs().forEach(job -> require(job.status() == MigrationScheduler.Status.DONE, "job did not complete: " + job));
        require(f.commits == c.jobs() && f.routing.get().version().value() == c.jobs(), "lost routing update");
        require(f.routing.get().get(f.guard).epoch().value() == 0 && f.guardData.size() == 8, "guard tablet changed");
        require(f.guardData.values().stream().allMatch(v -> v.equals(MigrationFixture.value(-1))), "guard data changed");
        require(f.nodes.get(f.routing.get().get(f.guard).owner()).tablets.get(f.guard).get().equals(f.guardData), "colocated guard removed");
        List<Map<String, Object>> jobs = new ArrayList<>();
        long totalBytes = 0, finalBytes = 0, blocked = 0, writeBusy = 0, writes = 0, readBusy = 0, reads = 0;
        List<Long> waits = new ArrayList<>();
        for (var job : scheduler.jobs()) {
            var h = f.handles.get(job.index());
            require(job.status() == MigrationScheduler.Status.DONE, "job did not complete: " + job);
            require(h.sourceData.isEmpty() && h.oracle.equals(h.finalData()), "job exact data mismatch");
            require(f.nodes.get(h.change.to()).tablets.get(h.change.unit()).get().equals(h.oracle), "destination node storage");
            require(f.routing.get().get(h.change.unit()).epoch().value() == 1, "epoch mismatch");
            require(h.request(h.change.from(), new Command.Get(h.keys.getFirst())).status() == ResultStatus.MOVED, "old source not fenced");
            require(!f.nodes.get(h.change.from()).frozen.containsKey(h.change.unit())
                    && !f.nodes.get(h.change.from()).hooks.containsKey(h.change.unit()), "source resources not released");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("job", job.index()); row.put("tablet", h.change.unit().value()); row.put("source", h.change.from().value()); row.put("destination", h.change.to().value());
            row.put("startTick", job.startTick()); row.put("commitTick", h.commitTick); row.put("endTick", job.endTick()); row.put("status", job.status().name());
            row.put("sentBytes", h.sentBytes()); row.put("finalBytes", h.oracle.size() * 152L); row.put("blockedTicks", h.blockedTicks);
            row.put("writes", h.writes); row.put("acceptedWrites", h.accepted); row.put("writeBusy", h.writeBusy); row.put("reads", h.reads); row.put("readBusy", h.readBusy);
            row.put("rounds", h.rounds()); row.put("stopReason", h.reason()); row.put("exactMatch", true); row.put("sourceRecords", h.sourceData.size());
            row.put("oracleHash", dataHash(h.oracle)); row.put("destinationHash", dataHash(h.finalData()));
            row.put("dirtyRounds", h.dirtyTrace.events());
            jobs.add(row); waits.add(job.startTick()); totalBytes += h.sentBytes(); finalBytes += h.oracle.size() * 152L;
            blocked += h.blockedTicks; writes += h.writes; writeBusy += h.writeBusy; reads += h.reads; readBusy += h.readBusy;
        }
        Map<String, Object> metrics = new TreeMap<>();
        metric(metrics, "makespan_ticks", tick, "tick"); metric(metrics, "sent_bytes", totalBytes, "bytes");
        metric(metrics, "final_data_bytes", finalBytes, "bytes"); metric(metrics, "initial_data_bytes", (long) c.jobs() * c.keys() * 152, "bytes");
        metric(metrics, "aggregate_bytes_per_tick", totalBytes / (double) tick, "bytes/tick");
        metric(metrics, "useful_bytes_per_tick", finalBytes / (double) tick, "bytes/tick");
        metric(metrics, "initial_transfer_ratio", totalBytes / (double) ((long) c.jobs() * c.keys() * 152), "ratio");
        metric(metrics, "final_transfer_ratio", totalBytes / (double) finalBytes, "ratio");
        metric(metrics, "cluster_budget_utilization", totalBytes / ((double) tick * c.clusterBudget()), "ratio");
        for (double q : new double[] {.50, .95, .99, 1.}) metric(metrics, "wait_p" + (int) (q * 100) + "_ticks", percentile(waits, q), "tick");
        metric(metrics, "writes_attempted", writes, "count"); metric(metrics, "writes_busy", writeBusy, "count");
        metric(metrics, "reads_attempted", reads, "count"); metric(metrics, "reads_busy", readBusy, "count");
        metric(metrics, "read_busy_ratio", reads == 0 ? 0. : readBusy / (double) reads, "ratio");
        metric(metrics, "blocked_job_ticks", blocked, "tick"); metric(metrics, "completion_ratio", 1., "ratio");
        metric(metrics, "exact_comparisons", c.jobs(), "count"); metric(metrics, "invariant_violations", 0, "count");
        metric(metrics, "starvation_jobs", scheduler.jobs().stream().filter(j -> j.startTick() < 0).count(), "count");
        metric(metrics, "min_job_sent_bytes", scheduler.jobs().stream().mapToLong(MigrationScheduler.JobView::sentBytes).min().orElseThrow(), "bytes");
        metric(metrics, "max_job_sent_bytes", scheduler.jobs().stream().mapToLong(MigrationScheduler.JobView::sentBytes).max().orElseThrow(), "bytes");
        return new Run(metrics, List.copyOf(jobs), List.copyOf(budgets), List.copyOf(events));
    }

    static void requests(MultiMigrationFixture.Handle h, Case c, long seed, int index, int tick) {
        long mixed = seed ^ (0x9E3779B97F4A7C15L * (tick + 1)) ^ (index * 0xBF58476D1CE4E5B9L);
        SplittableRandom random = new SplittableRandom(mixed ^ (mixed >>> 31));
        double rate = c.rho() * c.referenceBudget() / 152.;
        int writes = (int) (Math.floor((tick + 1) * rate) - Math.floor(tick * rate));
        for (int i = 0; i < writes + 2; i++) {
            Bytes key = h.keys.get(random.nextInt(h.keys.size()));
            Command command = i >= writes ? new Command.Get(key) : random.nextInt(100) < 30 ? new Command.Delete(key)
                    : new Command.Set(key, MigrationFixture.value(((long) tick << 32) ^ ((long) index << 16) ^ i));
            Result result = h.request(command);
            switch (command) {
                case Command.Get get -> {
                    h.reads++;
                    if (result.status() == ResultStatus.BUSY) h.readBusy++;
                    else if (h.oracle.containsKey(key)) require(result.status() == ResultStatus.OK && result.value().orElseThrow().equals(h.oracle.get(key)), "stale/false-miss GET");
                    else require(result.status() == ResultStatus.MISS, "deleted key resurrected");
                }
                case Command.Set set -> {
                    h.writes++;
                    if (result.status() == ResultStatus.BUSY) h.writeBusy++;
                    else { require(result.status() == ResultStatus.OK, "SET rejected"); h.oracle.put(key, set.value()); h.accepted++; }
                }
                case Command.Delete delete -> {
                    h.writes++;
                    if (result.status() == ResultStatus.BUSY) h.writeBusy++;
                    else { boolean existed = h.oracle.remove(key) != null; require(result.status() == (existed ? ResultStatus.OK : ResultStatus.MISS), "DELETE mismatch"); h.accepted++; }
                }
                default -> throw new IllegalArgumentException("CRUD only");
            }
        }
    }

    static void checkBudgets(MigrationScheduler.Tick tick, Case c) {
        require(tick.allocated().size() <= c.concurrency(), "concurrency exceeded");
        require(tick.allocated().values().stream().mapToLong(Long::longValue).sum() <= c.clusterBudget(), "cluster allocation exceeded");
        require(tick.consumed().values().stream().mapToLong(Long::longValue).sum() <= c.clusterBudget(), "cluster consumption exceeded");
        Map<NodeId, Long> allocated = new LinkedHashMap<>();
        Map<NodeId, Integer> occupied = new LinkedHashMap<>();
        for (var job : tick.jobs()) if (tick.allocated().containsKey(job.index())) {
            for (NodeId node : List.of(job.change().from(), job.change().to())) {
                allocated.merge(node, tick.allocated().get(job.index()), Long::sum); occupied.merge(node, 1, Integer::sum);
            }
        }
        require(allocated.values().stream().allMatch(n -> n <= c.nodeBudget()) && tick.nodeConsumed().values().stream().allMatch(n -> n <= c.nodeBudget()), "node budget exceeded");
        require(occupied.values().stream().allMatch(n -> n <= c.endpointLimit()), "endpoint concurrency exceeded");
    }

    static long percentile(List<Long> values, double q) {
        List<Long> sorted = values.stream().sorted().toList();
        return sorted.get(Math.max(0, (int) Math.ceil(q * sorted.size()) - 1));
    }
    private static void metric(Map<String, Object> values, String name, Number value, String unit) { values.put(name, Map.of("value", value, "unit", unit)); }
    static void require(boolean condition, String message) { if (!condition) throw new IllegalStateException(message); }
    static Object canonical(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new TreeMap<>(); map.forEach((k, v) -> result.put(k.toString(), canonical(v))); return result;
        }
        if (value instanceof List<?> list) return list.stream().map(MovementExperiment::canonical).toList();
        if (value instanceof Number number) return new BigDecimal(number.toString()).stripTrailingZeros();
        return value;
    }
    static String hash(Object value) { return ComparisonExperiment.sha256(BaselineJson.encode(canonical(value))); }
    static String dataHash(Map<Bytes, Bytes> data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            data.entrySet().stream().sorted(Comparator.comparing(e -> HexFormat.of().formatHex(e.getKey().toByteArray())))
                    .forEach(e -> { digest.update(e.getKey().toByteArray()); digest.update(e.getValue().toByteArray()); });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException failure) { throw new AssertionError(failure); }
    }
    private MovementExperiment() {}
}
