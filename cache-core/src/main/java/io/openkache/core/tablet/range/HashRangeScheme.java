package io.openkache.core.tablet.range;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * 해시 범위 Tablet({@code hash-range-tablet}): 해시 공간을 가변 길이 구간으로 나누고, 구간은 split·merge로 늘거나 줄 수 있다.
 * Google Slicer, ScyllaDB Tablets 계열.
 *
 * <p>참고: Slicer(OSDI 2016) §4.4, ScyllaDB Tablets 문서. 정리는
 * {@code experiments/b1-tablet-distribution/references/slicer-scylla-tablets.md}, 결정은 {@code design.md} §4.2.
 *
 * <p>fixed-slot과 표·조회·이동 연산은 같다. 다른 점은 두 가지다.
 * <ul>
 *   <li>{@link #supportsSplit()}가 {@code true}다. 어느 Tablet을 언제 쪼개고 합칠지는 {@code balance}가 정하고
 *       {@link RoutingTable#withSplit}/{@link RoutingTable#withMerge}로 적용한다. 이 클래스는 경계를 스스로 바꾸지 않으므로
 *       계획의 {@code prepared}는 항상 입력 표 그대로다.</li>
 *   <li>노드 균형은 Tablet <b>개수</b>가 아니라 맡은 해시 공간 <b>길이</b>로 잰다. split 뒤에는 Tablet 길이가 제각각이라 개수로 세면
 *       key 균형이 틀어지기 때문이다. 길이가 모두 같은 처음 표에서는 fixed-slot과 같은 결과가 나온다.</li>
 * </ul>
 *
 * <p>처음 표는 노드당 {@code T}개, 총 {@code n·T}개의 균등 구간을 연속 블록으로 배치한다(Slicer의 처음 할당도 균등 분할).
 * 노드 추가는 "가장 긴 노드의 마지막 Tablet"을 새 노드가 그 노드를 넘지 않을 때까지 가져온다. 그래서 새 노드의 길이는
 * {@code [평균 − 가장 큰 Tablet 길이, 추가 전 최대]} 안에 들어오고, 기존 노드끼리는 움직이지 않는다. 노드 제거는 떠나는 노드의
 * Tablet을 긴 것부터 "가장 짧은 노드"에 준다. 동률이면 {@link NodeId}가 작은 쪽이다.
 */
public final class HashRangeScheme implements DistributionScheme {

    /** 기본 노드당 Tablet 수. Slicer는 서버당 50~150개를 유지한다. 사전 실험에서 32~64개면 노드 추가 직후 균형 1.03 이내였다. */
    public static final int DEFAULT_TABLETS_PER_NODE = 64;

    /** Tablet 한 줄의 직렬화 크기: start 8B + id 8B + owner 8B + epoch 8B. 경계가 가변이라 start와 id를 줄마다 보내야 한다. */
    static final int BYTES_PER_TABLET = 32;

    private final int tabletsPerNode;

    public HashRangeScheme(int tabletsPerNode) {
        if (tabletsPerNode < 1) {
            throw new IllegalArgumentException("tabletsPerNode must be >= 1: " + tabletsPerNode);
        }
        this.tabletsPerNode = tabletsPerNode;
    }

    public HashRangeScheme() {
        this(DEFAULT_TABLETS_PER_NODE);
    }

    public int tabletsPerNode() {
        return tabletsPerNode;
    }

    @Override
    public String name() {
        return "hash-range-tablet";
    }

    @Override
    public RoutingTable initial(List<NodeId> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (new HashSet<>(nodes).size() != nodes.size()) {
            throw new IllegalArgumentException("duplicate nodes: " + nodes);
        }
        int n = nodes.size();
        int count = Math.multiplyExact(n, tabletsPerNode);
        List<TabletRange> ranges = TabletRange.uniform(count);
        List<Tablet> tablets = new ArrayList<>(count);
        OwnershipEpoch first = OwnershipEpoch.ZERO.next();
        // 연속 블록 배치: 노드 j는 Tablet [j·T, (j+1)·T). fixed-slot과 같은 이유(표 압축)다.
        for (int j = 0; j < n; j++) {
            for (int k = 0; k < tabletsPerNode; k++) {
                int i = j * tabletsPerNode + k;
                tablets.add(new Tablet(new TabletId(i), ranges.get(i), nodes.get(j), first));
            }
        }
        return RoutingTable.of(RoutingVersion.ZERO.next(), tablets, count);
    }

    @Override
    public Plan onNodeAdded(RoutingTable table, NodeId node) {
        if (table.nodes().contains(node)) {
            throw new IllegalArgumentException("node already in table: " + node);
        }
        Map<NodeId, List<Tablet>> owned = ownedByNode(table);
        if (table.size() < owned.size() + 1) {
            throw new IllegalArgumentException(
                    table.size() + " tablets cannot serve " + (owned.size() + 1) + " nodes; split first");
        }
        Map<NodeId, Long> weight = weights(owned);
        List<Change> moves = new ArrayList<>();
        long mine = 0;
        while (true) {
            // Tablet을 하나만 가진 노드는 donor가 못 된다. 주면 표에서 사라지기 때문이다.
            NodeId donor = pick(weight, owned, true, 2);
            if (donor == null) {
                break;
            }
            List<Tablet> list = owned.get(donor);
            Tablet candidate = list.get(list.size() - 1);
            long w = weight(candidate.range());
            // 받고 나서 donor가 받기 전 길이 이상이 되면 균형이 더 좋아지지 않으므로 멈춘다(같으면 자리만 바뀌는 이동 낭비)
            if (mine + w >= weight.get(donor)) {
                break;
            }
            list.remove(list.size() - 1);
            weight.put(donor, weight.get(donor) - w);
            mine += w;
            moves.add(new Change(candidate.id(), donor, node));
        }
        return Plan.movesOnly(table, moves);
    }

    @Override
    public Plan onNodeRemoved(RoutingTable table, NodeId node) {
        if (!table.nodes().contains(node)) {
            throw new IllegalArgumentException("node not in table: " + node);
        }
        Map<NodeId, List<Tablet>> owned = ownedByNode(table);
        List<Tablet> leaving = new ArrayList<>(owned.remove(node));
        if (owned.isEmpty()) {
            throw new IllegalStateException("cannot remove the last node: " + node);
        }
        Map<NodeId, Long> weight = weights(owned);
        // 긴 것부터 가장 짧은 노드에 준다(LPT). 동률이면 시작점이 작은 쪽부터. 길이가 모두 같으면 fixed-slot과 같은 순서다
        leaving.sort((a, b) -> {
            int byWeight = Long.compare(weight(b.range()), weight(a.range()));
            return byWeight != 0 ? byWeight : Long.compareUnsigned(a.range().start(), b.range().start());
        });
        List<Change> moves = new ArrayList<>(leaving.size());
        for (Tablet t : leaving) {
            NodeId receiver = pick(weight, owned, false, 0);
            weight.put(receiver, weight.get(receiver) + weight(t.range()));
            moves.add(new Change(t.id(), node, receiver));
        }
        return Plan.movesOnly(table, moves);
    }

    @Override
    public boolean supportsSplit() {
        return true;
    }

    @Override
    public boolean supportsMove() {
        return true;
    }

    /** 헤더 16B + Tablet마다 {@link #BYTES_PER_TABLET}. fixed-slot과 달리 경계가 가변이라 구간 압축이 안 된다. */
    @Override
    public int metadataBytes(RoutingTable table) {
        return 16 + table.size() * BYTES_PER_TABLET;
    }

    /**
     * 구간 길이를 4로 나눈 값. 노드별 합이 2^62를 넘지 않아 부호 있는 long으로 더할 수 있다(전체 공간 2^64는 long에 안 들어간다).
     * 잘리는 최대 3은 균형 판단에 영향이 없다(길이 4 미만 Tablet은 무게 0이 되지만 그런 Tablet은 실험·운영에서 만들지 않는다).
     */
    static long weight(TabletRange r) {
        if (r.equals(TabletRange.FULL)) {
            return 1L << 62;
        }
        return (r.end() - r.start()) >>> 2; // end == 0(2^64)이면 -start == 2^64 - start 로 부호 없는 길이가 맞다
    }

    private static Map<NodeId, Long> weights(Map<NodeId, List<Tablet>> owned) {
        Map<NodeId, Long> w = new HashMap<>();
        for (Map.Entry<NodeId, List<Tablet>> e : owned.entrySet()) {
            long sum = 0;
            for (Tablet t : e.getValue()) {
                sum += weight(t.range());
            }
            w.put(e.getKey(), sum);
        }
        return w;
    }

    /** 노드별 Tablet 목록(시작점 오름차순). */
    private static Map<NodeId, List<Tablet>> ownedByNode(RoutingTable table) {
        Map<NodeId, List<Tablet>> m = new HashMap<>();
        for (Tablet t : table.tablets()) {
            m.computeIfAbsent(t.owner(), k -> new ArrayList<>()).add(t);
        }
        return m;
    }

    /** 가장 긴(또는 짧은) 노드. Tablet을 {@code minTablets}개 이상 가진 노드만 후보. 동률이면 NodeId가 작은 쪽. 후보가 없으면 null. */
    private static NodeId pick(Map<NodeId, Long> weight, Map<NodeId, List<Tablet>> owned, boolean longest, int minTablets) {
        NodeId best = null;
        long bestWeight = 0;
        for (Map.Entry<NodeId, Long> e : weight.entrySet()) {
            if (owned.get(e.getKey()).size() < minTablets) {
                continue;
            }
            long w = e.getValue();
            boolean better = best == null
                    || (longest ? w > bestWeight : w < bestWeight)
                    || (w == bestWeight && e.getKey().value() < best.value());
            if (better) {
                best = e.getKey();
                bestWeight = w;
            }
        }
        return best;
    }
}
