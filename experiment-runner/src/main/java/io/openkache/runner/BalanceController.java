package io.openkache.runner;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.balance.placement.CostAwareRebalancing;
import io.openkache.core.balance.placement.GreedyLoadPlacement;
import io.openkache.core.balance.placement.HysteresisRebalancing;
import io.openkache.core.balance.placement.PlacementDecision;
import io.openkache.core.balance.placement.PlacementInput;
import io.openkache.core.balance.placement.PlacementPolicy;
import io.openkache.core.balance.split.EStoreSplitPolicy;
import io.openkache.core.balance.split.HeatSplitPolicy;
import io.openkache.core.balance.split.MidpointSplitPolicy;
import io.openkache.core.balance.split.SplitDecision;
import io.openkache.core.balance.split.SplitPolicy;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * B1·B2·B3를 잇는 균형 조정 루프. Runner가 보낸 요청으로 key별 부하를 관찰하고, window마다
 * <ol>
 *   <li>분할 시작 조건(B2 design.md §4.1·Benchmark C 규칙, {@link ClusterConfig.Balance})을 넘은 Tablet에 B2 {@link SplitPolicy}를
 *       부르고, 결정을 B1 Controller에 CONTROL SPLIT으로 보낸다.
 *   <li>나눈 뒤의 표와 같은 window 부하로 B2 {@link PlacementPolicy}를 부르고, 이동 계획을 B3 이동(MIGRATE, 보통 pre-copy)으로
 *       하나씩 실행한다.
 * </ol>
 * 정책은 core에 있는 B2 구현을 그대로 쓴다. 이 클래스는 관측·호출·지시만 한다.
 *
 * <p>{@link #observe}는 요청 thread에서, {@link #run}은 전용 thread 하나에서 호출한다.
 */
final class BalanceController {

    /** Tablet 하나를 node로 옮기고 기록을 돌려준다. */
    @FunctionalInterface
    interface Mover {
        ClusterRunner.MigrationRecord move(long tablet, long to, double atMs) throws IOException;
    }

    private static final int OP_CONTROL = 8;

    private final RunnerConfig config;
    private final ClusterConfig.Balance balance;
    private final ClusterRouting routing;
    private final Mover mover;
    private final SplitPolicy splitPolicy;
    private final PlacementPolicy placementPolicy;
    private final AtomicReference<ConcurrentHashMap<Long, AtomicLongArray>> current =
            new AtomicReference<>(new ConcurrentHashMap<>());
    private final List<String> windows = new ArrayList<>();
    private final Map<TabletId, Integer> hotStreak = new HashMap<>();
    private final Map<TabletId, Long> createdWindow = new HashMap<>();
    private final Map<TabletId, Long> lastMovedWindow = new HashMap<>();
    private volatile boolean stopped;
    private int splits;
    private int moves;

    BalanceController(RunnerConfig config, ClusterConfig.Balance balance, ClusterRouting routing, Mover mover) {
        this.config = config;
        this.balance = balance;
        this.routing = routing;
        this.mover = mover;
        this.splitPolicy = switch (balance.split()) {
            case "heat" -> new HeatSplitPolicy(balance.theta());
            case "midpoint" -> new MidpointSplitPolicy();
            case "estore" -> new EStoreSplitPolicy();
            default -> null;
        };
        this.placementPolicy = switch (balance.placement()) {
            case "hysteresis" -> new HysteresisRebalancing();
            case "greedy" -> new GreedyLoadPlacement();
            case "cost-aware" -> new CostAwareRebalancing();
            default -> null;
        };
    }

    /** 측정 구간 요청 하나를 센다(요청 thread). */
    void observe(long keyHash, boolean write) {
        current.get().computeIfAbsent(keyHash, k -> new AtomicLongArray(2)).incrementAndGet(write ? 1 : 0);
    }

    void stop() {
        stopped = true;
    }

    /** 측정 시작 {@code measuredStart}(nanoTime) 기준으로 window를 돈다. {@link #stop} 뒤 진행 중 window를 마치고 끝낸다. */
    void run(long measuredStart) {
        long start = measuredStart + balance.startMs() * 1_000_000L;
        sleepUntil(start);
        current.set(new ConcurrentHashMap<>());
        long window = 0;
        while (!stopped) {
            long windowStart = System.nanoTime();
            sleepUntil(windowStart + balance.windowMs() * 1_000_000L);
            if (stopped) break;
            Map<Long, AtomicLongArray> counts = current.getAndSet(new ConcurrentHashMap<>());
            long windowEnd = System.nanoTime();
            try {
                windows.add(evaluate(window, counts, ms(windowStart - measuredStart), ms(windowEnd - measuredStart),
                        measuredStart));
            } catch (IOException | RuntimeException e) {
                windows.add(String.format(Locale.ROOT, "{\"window\": %d, \"error\": %s}", window, quote(e.toString())));
            }
            // 이동이 길었으면 그 사이 쌓인 요청은 다음 window와 섞이지 않게 버린다
            current.set(new ConcurrentHashMap<>());
            window++;
        }
    }

    private String evaluate(long window, Map<Long, AtomicLongArray> counts, double startMs, double endMs,
                            long measuredStart) throws IOException {
        RoutingTable table = routing.refreshNow();
        int tabletsBefore = table.size();
        int nodes = routing.nodeCount();
        long total = 0;
        for (AtomicLongArray c : counts.values()) total += c.get(0) + c.get(1);
        double capacity = total / (nodes * balance.targetUtilization());
        Map<TabletId, Long> loads = tabletLoads(table, counts);
        long[] nodeLoad = nodeLoads(table, loads, nodes);
        String before = loadJson(nodeLoad);
        double imbalance = imbalance(nodeLoad);

        List<String> splitEvents = new ArrayList<>();
        if (splitPolicy != null && total > 0) {
            List<Tablet> hot = new ArrayList<>();
            for (Tablet t : table.tablets()) {
                if (loads.getOrDefault(t.id(), 0L) > capacity / 4) hotStreak.merge(t.id(), 1, Integer::sum);
                else hotStreak.remove(t.id());
                Long created = createdWindow.get(t.id());
                boolean cooling = created != null && window - created < balance.cooldownWindows();
                if (hotStreak.getOrDefault(t.id(), 0) >= balance.sustainWindows() && !cooling) hot.add(t);
            }
            hot.sort(Comparator.comparingLong((Tablet t) -> loads.getOrDefault(t.id(), 0L)).reversed());
            for (Tablet t : hot) {
                if (table.size() >= balance.tabletLimit()) break;
                SplitDecision decision = splitPolicy.decide(new TabletLoad(t.range(), window, keyLoads(t, counts)));
                if (decision instanceof SplitDecision.Split split) {
                    RoutingTable after = split(table, t, split.at());
                    if (after == null) {
                        splitEvents.add(event(t, "\"result\": \"rejected\""));
                        continue;
                    }
                    Set<TabletId> created = new HashSet<>();
                    for (Tablet n : after.tablets()) if (!table.contains(n.id()) || n.id().equals(t.id())) created.add(n.id());
                    for (TabletId id : created) {
                        createdWindow.put(id, window);
                        hotStreak.remove(id);
                    }
                    table = after;
                    splits++;
                    splitEvents.add(event(t, String.format(Locale.ROOT, "\"result\": \"split\", \"at\": \"%s\", \"tablets\": %s",
                            Long.toUnsignedString(split.at()), created.stream().map(id -> String.valueOf(id.value())).sorted().toList())));
                } else if (decision instanceof SplitDecision.NoSplit no) {
                    splitEvents.add(event(t, "\"result\": \"no-split\", \"reason\": \"" + no.reason() + "\""));
                }
            }
        }

        List<String> moveEvents = new ArrayList<>();
        if (placementPolicy != null && total > 0) {
            Map<TabletId, Long> afterSplit = tabletLoads(table, counts);
            Map<TabletId, Long> bytes = new HashMap<>();
            Map<NodeId, Double> capacityMap = new HashMap<>();
            for (Tablet t : table.tablets()) {
                afterSplit.putIfAbsent(t.id(), 0L);
                bytes.put(t.id(), estimatedBytes(config, t));
            }
            for (int n = 0; n < nodes; n++) capacityMap.put(new NodeId(n), Math.max(capacity, 1.0));
            PlacementDecision decision = placementPolicy.decide(
                    new PlacementInput(table, afterSplit, bytes, capacityMap, window, lastMovedWindow));
            for (Change change : decision.moves()) {
                ClusterRunner.MigrationRecord r = mover.move(change.unit().value(), change.to().value(),
                        ms(System.nanoTime() - measuredStart));
                if (r.phase() == 4) {
                    lastMovedWindow.put(change.unit(), window);
                    moves++;
                }
                moveEvents.add(String.format(Locale.ROOT,
                        "{\"tablet\": %d, \"from\": %d, \"to\": %d, \"load\": %d, \"estimatedBytes\": %d, \"sentMs\": %.3f, "
                                + "\"finishedMs\": %.3f, \"phase\": %d, \"note\": %s, \"server\": %s}",
                        change.unit().value(), change.from().value(), change.to().value(), afterSplit.get(change.unit()),
                        bytes.get(change.unit()), r.sentMs(), r.finishedMs(), r.phase(), quote(r.rejection()), r.serverJson()));
            }
        }
        RoutingTable finalTable = routing.refreshNow();
        long[] planned = nodeLoads(finalTable, tabletLoads(finalTable, counts), nodes);
        return String.format(Locale.ROOT,
                "{\"window\": %d, \"startMs\": %.1f, \"endMs\": %.1f, \"requests\": %d, \"tablets\": %d, "
                        + "\"nodeLoad\": %s, \"imbalance\": %.3f, \"splits\": [%s], \"moves\": [%s], "
                        + "\"tabletsAfter\": %d, \"nodeLoadAfterPlan\": %s, \"imbalanceAfterPlan\": %.3f}",
                window, startMs, endMs, total, tabletsBefore, before, imbalance,
                String.join(", ", splitEvents), String.join(", ", moveEvents), finalTable.size(), loadJson(planned),
                imbalance(planned));
    }

    /** CONTROL SPLIT을 Controller(node 0)에 보낸다. 성공하면 새 표, CAS 실패·거절이면 null. */
    private RoutingTable split(RoutingTable table, Tablet t, long at) throws IOException {
        byte[] body = ByteBuffer.allocate(25).put((byte) 1).putLong(table.version().value()).putLong(t.id().value())
                .putLong(at).array();
        RunnerConfig.Target controller = routing.node(0);
        try (WireClient peer = new WireClient(controller.host(), routing.peerPort(), config.requestTimeoutMs())) {
            WireClient.Response r = peer.peer(OP_CONTROL, body);
            if (r.status() != WireProtocol.STATUS_OK) return null;
        }
        return routing.refreshNow();
    }

    private static Map<TabletId, Long> tabletLoads(RoutingTable table, Map<Long, AtomicLongArray> counts) {
        Map<TabletId, Long> loads = new HashMap<>();
        for (Tablet t : table.tablets()) loads.put(t.id(), 0L);
        counts.forEach((hash, c) -> loads.merge(table.lookup(hash).id(), c.get(0) + c.get(1), Long::sum));
        return loads;
    }

    private static long[] nodeLoads(RoutingTable table, Map<TabletId, Long> loads, int nodes) {
        long[] result = new long[nodes];
        for (Tablet t : table.tablets()) {
            int owner = (int) t.owner().value();
            if (owner < nodes) result[owner] += loads.getOrDefault(t.id(), 0L);
        }
        return result;
    }

    private static List<KeyLoad> keyLoads(Tablet t, Map<Long, AtomicLongArray> counts) {
        List<KeyLoad> keys = new ArrayList<>();
        counts.forEach((hash, c) -> {
            if (t.range().contains(hash)) keys.add(new KeyLoad(hash, c.get(0), c.get(1)));
        });
        keys.sort((a, b) -> Long.compareUnsigned(a.keyHash(), b.keyHash()));
        return keys;
    }

    /** key가 해시 공간에 고르게 퍼져 있다고 보고 구간 폭 비율 × 전체 데이터로 Tablet 크기를 어림한다. */
    static long estimatedBytes(RunnerConfig config, Tablet t) {
        long width = t.range().end() - t.range().start();
        double fraction = width == 0 ? 1.0 : unsigned(width) / Math.pow(2, 64);
        return (long) (fraction * config.keyCount() * (config.valueSize() + config.keyLength()));
    }

    private static double unsigned(long v) {
        return v >= 0 ? v : (v >>> 1) * 2.0 + (v & 1);
    }

    private static double imbalance(long[] nodeLoad) {
        long sum = 0;
        long max = 0;
        for (long l : nodeLoad) {
            sum += l;
            max = Math.max(max, l);
        }
        return sum == 0 ? 1.0 : max / (sum / (double) nodeLoad.length);
    }

    private static String loadJson(long[] nodeLoad) {
        return Arrays.toString(nodeLoad);
    }

    private static String event(Tablet t, String body) {
        return String.format(Locale.ROOT, "{\"tablet\": %d, \"owner\": %d, %s}", t.id().value(), t.owner().value(), body);
    }

    /** 결과 JSON의 balance 항목. run이 끝난 뒤 부른다. */
    String json() {
        return String.format(Locale.ROOT,
                "{\"split\": \"%s\", \"splitVariant\": %s, \"theta\": %.2f, \"placement\": \"%s\", \"placementVariant\": %s, "
                        + "\"windowMs\": %d, \"sustainWindows\": %d, \"cooldownWindows\": %d, \"tabletLimit\": %d, "
                        + "\"targetUtilization\": %.2f, \"splits\": %d, \"moves\": %d, \"windows\": [%n    %s%n  ]}",
                balance.split(), splitPolicy == null ? "null" : quote(splitPolicy.variant()), balance.theta(),
                balance.placement(), placementPolicy == null ? "null" : quote(placementPolicy.variant()),
                balance.windowMs(), balance.sustainWindows(), balance.cooldownWindows(), balance.tabletLimit(),
                balance.targetUtilization(), splits, moves, String.join(",\n    ", windows));
    }

    private void sleepUntil(long deadline) {
        long now;
        while (!stopped && (now = System.nanoTime()) < deadline) LockSupport.parkNanos(Math.min(deadline - now, 50_000_000L));
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }

    private static String quote(String text) {
        return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ") + '"';
    }
}
