package io.openkache.core.migration;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * 고정 Change 목록을 FIFO(실행 가능한 가장 오래된 항목 우선)로 실행한다.
 * 모든 호출과 driver는 단일 owner/controller thread 전용이며 실제 시간·I/O를 사용하지 않는다.
 * 배치·노드 수를 바꾸지 않고 동시 job·endpoint 점유·byte 예산만 제어한다.
 */
public final class MigrationScheduler {
    /** 모든 예산은 tick당 bytes. node 예산은 송신·수신이 공유하며 전송 1 byte는 양 endpoint에 각각 1씩 부과한다. */
    public record Settings(int maxConcurrentJobs, int maxJobsPerNode, long clusterBytesPerTick,
                           long nodeBytesPerTick, long maxJobTicks) {
        public Settings {
            if (maxConcurrentJobs < 1 || maxJobsPerNode < 1 || clusterBytesPerTick < 1
                    || nodeBytesPerTick < 1 || maxJobTicks < 1) throw new IllegalArgumentException("invalid scheduler settings");
        }
    }

    /** protocol별 제어 adapter. step은 budget 이하만 전송하고 누적 sentBytes를 노출해야 한다. */
    public interface Driver {
        void begin();
        void step(long budgetBytes);
        long sentBytes();
        boolean done();
        boolean failed();
        String state();
        /** timeout 시 protocol의 commit 전 취소·commit 후 보존 계약에 따라 중단한다. */
        void stop();
    }

    /** 연구별 정책 경계. 시작 후보·할당을 제안하며 실제 자원 상한은 Scheduler가 다시 검증한다. */
    public interface Policy {
        default List<Integer> order(long tick, List<JobView> jobs, Settings settings) {
            return jobs.stream().filter(j -> j.status() == Status.QUEUED).map(JobView::index).toList();
        }
        /** null은 기존 회전 균등 할당. 반환하는 경우 모든 active job의 예산을 포함해야 한다. */
        default Map<Integer, Long> allocate(long tick, List<JobView> active, Settings settings) { return null; }
        default void started(long tick, JobView job) {}
    }

    public enum Status { QUEUED, RUNNING, DONE, FAILED, TIMED_OUT }

    /** 시각은 논리 tick. 아직 시작/완료하지 않은 시각은 -1이다. */
    public record JobView(int index, Change change, Status status, long startTick, long endTick, long sentBytes, String state) {}
    /** 해당 tick의 할당·실소비. 양 endpoint 소비 합계는 cluster 소비의 2배다. */
    public record Tick(long tick, List<Integer> admitted, Map<Integer, Long> allocated, Map<Integer, Long> consumed,
                       Map<NodeId, Long> nodeConsumed, List<JobView> jobs) {}

    private static final class Entry {
        final int index;
        final Change change;
        Driver driver;
        Status status = Status.QUEUED;
        long start = -1, end = -1;
        Entry(int index, Change change) { this.index = index; this.change = change; }
        JobView view() { return new JobView(index, change, status, start, end,
                driver == null ? 0 : driver.sentBytes(), driver == null ? "PENDING" : driver.state()); }
    }

    private final Settings settings;
    private final Function<Change, Driver> factory;
    private final Policy policy;
    private final List<Entry> entries = new ArrayList<>();
    private long nextTick;

    /** changes는 수정하지 않으며 같은 Tablet을 두 번 계획한 입력은 시작 전에 거절한다. */
    public MigrationScheduler(List<Change> changes, Settings settings, Function<Change, Driver> factory) {
        this(changes, settings, factory, new Policy() {});
    }

    public MigrationScheduler(List<Change> changes, Settings settings, Function<Change, Driver> factory, Policy policy) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.policy = Objects.requireNonNull(policy, "policy");
        var tablets = new HashSet<TabletId>();
        for (Change change : List.copyOf(changes)) {
            if (!tablets.add(change.unit())) throw new IllegalArgumentException("duplicate tablet");
            entries.add(new Entry(entries.size(), change));
        }
    }

    public List<JobView> jobs() { return entries.stream().map(Entry::view).toList(); }
    public boolean finished() { return entries.stream().noneMatch(e -> e.status == Status.QUEUED || e.status == Status.RUNNING); }

    /** 한 tick 진행. 회전 순서의 progressive filling으로 공유 endpoint에서 작은 예산도 공정하게 나눈다. */
    public Tick step() {
        long tick = nextTick++;
        Map<NodeId, Integer> occupied = new HashMap<>();
        int running = 0;
        for (Entry e : entries) if (e.status == Status.RUNNING) {
            running++;
            occupied.merge(e.change.from(), 1, Integer::sum);
            occupied.merge(e.change.to(), 1, Integer::sum);
        }
        List<Integer> admitted = new ArrayList<>();
        List<Integer> order = List.copyOf(policy.order(tick, jobs(), settings));
        var ordered = new HashSet<Integer>();
        for (int id : order) if (id < 0 || id >= entries.size() || !ordered.add(id)
                || entries.get(id).status != Status.QUEUED) throw new IllegalArgumentException("invalid policy admission");
        for (int id : order) {
            Entry e = entries.get(id);
            if (running == settings.maxConcurrentJobs()) break;
            if (e.status != Status.QUEUED || occupied.getOrDefault(e.change.from(), 0) >= settings.maxJobsPerNode()
                    || occupied.getOrDefault(e.change.to(), 0) >= settings.maxJobsPerNode()) continue;
            e.driver = Objects.requireNonNull(factory.apply(e.change), "driver");
            e.start = tick;
            e.driver.begin();
            e.status = Status.RUNNING;
            policy.started(tick, e.view());
            running++;
            occupied.merge(e.change.from(), 1, Integer::sum);
            occupied.merge(e.change.to(), 1, Integer::sum);
            admitted.add(e.index);
        }
        List<Entry> active = entries.stream().filter(e -> e.status == Status.RUNNING).toList();
        Map<Integer, Long> proposed = policy.allocate(tick, active.stream().map(Entry::view).toList(), settings);
        Map<Integer, Long> allocated = proposed == null ? allocate(active, tick) : Map.copyOf(proposed);
        validateAllocation(active, allocated);
        Map<Integer, Long> consumed = new LinkedHashMap<>();
        Map<NodeId, Long> nodes = new HashMap<>();
        for (Entry e : active) {
            long before = e.driver.sentBytes();
            if (tick - e.start >= settings.maxJobTicks()) {
                e.driver.stop();
                e.status = Status.TIMED_OUT;
            } else {
                e.driver.step(allocated.get(e.index));
                if (e.driver.done()) e.status = Status.DONE;
                else if (e.driver.failed()) e.status = Status.FAILED;
            }
            long used = e.driver.sentBytes() - before;
            if (used < 0 || used > allocated.get(e.index)) throw new IllegalStateException("driver exceeded byte budget");
            consumed.put(e.index, used);
            nodes.merge(e.change.from(), used, Math::addExact);
            nodes.merge(e.change.to(), used, Math::addExact);
            if (e.status != Status.RUNNING) e.end = tick + 1;
        }
        return new Tick(tick, List.copyOf(admitted), Map.copyOf(allocated), Map.copyOf(consumed), Map.copyOf(nodes), jobs());
    }

    private void validateAllocation(List<Entry> active, Map<Integer, Long> allocated) {
        if (allocated.size() != active.size()) throw new IllegalArgumentException("missing/unknown policy allocation");
        long total = 0;
        Map<NodeId, Long> nodes = new HashMap<>();
        for (Entry e : active) {
            Long grant = allocated.get(e.index);
            if (grant == null || grant < 0 || grant > settings.clusterBytesPerTick() - total) throw new IllegalArgumentException("invalid policy byte budget");
            total += grant;
            for (NodeId node : List.of(e.change.from(), e.change.to())) {
                long used = nodes.getOrDefault(node, 0L);
                if (grant > settings.nodeBytesPerTick() - used) throw new IllegalArgumentException("policy node budget exceeded");
                nodes.put(node, used + grant);
            }
        }
    }

    private Map<Integer, Long> allocate(List<Entry> active, long tick) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        Map<NodeId, Long> remaining = new HashMap<>();
        for (Entry e : active) {
            result.put(e.index, 0L);
            remaining.put(e.change.from(), settings.nodeBytesPerTick());
            remaining.put(e.change.to(), settings.nodeBytesPerTick());
        }
        long cluster = settings.clusterBytesPerTick();
        int offset = active.isEmpty() ? 0 : (int) (tick % active.size());
        while (cluster > 0) {
            List<Entry> eligible = new ArrayList<>();
            for (int i = 0; i < active.size(); i++) {
                Entry e = active.get((i + offset) % active.size());
                if (remaining.get(e.change.from()) > 0 && remaining.get(e.change.to()) > 0) eligible.add(e);
            }
            if (eligible.isEmpty()) break;
            long share = Math.max(1, cluster / eligible.size());
            for (Entry e : eligible) {
                long grant = Math.min(cluster, Math.min(share, Math.min(remaining.get(e.change.from()), remaining.get(e.change.to()))));
                result.merge(e.index, grant, Math::addExact);
                remaining.compute(e.change.from(), (n, bytes) -> bytes - grant);
                remaining.compute(e.change.to(), (n, bytes) -> bytes - grant);
                cluster -= grant;
            }
        }
        return result;
    }
}
