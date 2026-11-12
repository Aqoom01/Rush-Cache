package io.openkache.core.tablet.slot;

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
 * 고정 해시 슬롯({@code fixed-slot}): 해시 공간을 처음부터 {@code S}개의 균등 구간(슬롯)으로 잘라 두고 슬롯 단위로만 옮긴다.
 * 슬롯 수는 바뀌지 않으므로 split·merge는 없다.
 *
 * <p>참고: Redis Cluster specification (16384 slots, MOVED/ASK), Hazelcast(271), Aerospike(4096). 정리는
 * {@code experiments/b1-tablet-distribution/references/redis-cluster-hash-slots.md}.
 *
 * <p>Redis는 {@code CRC16(key) mod 16384}로 슬롯을 정하지만, 여기서는 공통 해시의 상위 비트, 즉 균등 구간의 번호를 슬롯으로 쓴다.
 * 분포는 같고 {@link RoutingTable#lookup}을 그대로 쓸 수 있다. 슬롯 {@code i}의 {@link TabletId}는 {@code i}다.
 *
 * <p>처음 표는 연속 블록으로 배치한다(노드1: 0~S/n−1, 노드2: 그다음 …). Redis의 {@code CLUSTER SLOTS}처럼 연속 슬롯의 주인이 같으면
 * 표를 구간으로 압축해 보낼 수 있어서, 표 직렬화 크기가 슬롯 수가 아니라 노드 수에 비례한다(비교 실험 AUX 결과로 결정).
 *
 * <p>노드 추가는 "가장 많이 가진 노드의 마지막 슬롯"을 새 노드가 {@code S / (n+1)}개를 가질 때까지 가져온다. 입력 표가 균형(노드 간 차이
 * ≤ 1)이면 결과도 균형이다. {@code balance}가 임의로 옮겨 균형이 깨진 표에서는 새 노드 몫만 보장하고 나머지 불균형은 그대로 둔다
 * (그 조정은 {@code balance}의 몫). 이동 수가 이론
 * 최소이고 기존 노드끼리는 움직이지 않는다. 노드 제거는 떠나는 노드의 슬롯을 "가장 적게 가진 노드"에 하나씩 준다. 동률이면
 * {@link NodeId}가 작은 쪽이다.
 */
public final class FixedSlotScheme implements DistributionScheme {

    /** 기본 슬롯 수. 노드 10대 이하에서 노드당 400개 이상이라 정수 나눗셈 편차가 0.3% 이하다. */
    public static final int DEFAULT_SLOTS = 4096;

    private final int slots;

    public FixedSlotScheme(int slots) {
        if (slots < 1) {
            throw new IllegalArgumentException("slots must be >= 1: " + slots);
        }
        this.slots = slots;
    }

    public FixedSlotScheme() {
        this(DEFAULT_SLOTS);
    }

    public int slots() {
        return slots;
    }

    @Override
    public String name() {
        return "fixed-slot";
    }

    @Override
    public RoutingTable initial(List<NodeId> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (new HashSet<>(nodes).size() != nodes.size()) {
            throw new IllegalArgumentException("duplicate nodes: " + nodes);
        }
        requireSlotsFor(nodes.size());
        List<TabletRange> ranges = TabletRange.uniform(slots);
        List<Tablet> tablets = new ArrayList<>(slots);
        OwnershipEpoch first = OwnershipEpoch.ZERO.next();
        // 연속 블록 배치: 노드 j는 슬롯 [j·S/n, (j+1)·S/n). 나머지는 앞 노드부터 1개씩 더 받는다.
        // 연속 슬롯의 주인이 같아야 표를 구간으로 압축해 보낼 수 있다 (라운드로빈이면 구간이 S개가 된다).
        int n = nodes.size();
        int base = slots / n;
        int extra = slots % n;
        int slot = 0;
        for (int j = 0; j < n; j++) {
            int count = base + (j < extra ? 1 : 0);
            for (int k = 0; k < count; k++, slot++) {
                tablets.add(new Tablet(new TabletId(slot), ranges.get(slot), nodes.get(j), first));
            }
        }
        return RoutingTable.of(RoutingVersion.ZERO.next(), tablets, slots);
    }

    @Override
    public Plan onNodeAdded(RoutingTable table, NodeId node) {
        if (table.nodes().contains(node)) {
            throw new IllegalArgumentException("node already in table: " + node);
        }
        Map<NodeId, List<Tablet>> owned = ownedByNode(table);
        requireSlotsFor(owned.size() + 1);
        int target = slots / (owned.size() + 1);
        List<Change> moves = new ArrayList<>(target);
        for (int i = 0; i < target; i++) {
            NodeId donor = pick(owned, true);
            List<Tablet> list = owned.get(donor);
            Tablet taken = list.remove(list.size() - 1);
            moves.add(new Change(taken.id(), donor, node));
        }
        return Plan.movesOnly(table, moves);
    }

    @Override
    public Plan onNodeRemoved(RoutingTable table, NodeId node) {
        if (!table.nodes().contains(node)) {
            throw new IllegalArgumentException("node not in table: " + node);
        }
        Map<NodeId, List<Tablet>> owned = ownedByNode(table);
        List<Tablet> leaving = owned.remove(node);
        if (owned.isEmpty()) {
            throw new IllegalStateException("cannot remove the last node: " + node);
        }
        List<Change> moves = new ArrayList<>(leaving.size());
        for (Tablet t : leaving) {
            NodeId receiver = pick(owned, false);
            owned.get(receiver).add(t);
            moves.add(new Change(t.id(), node, receiver));
        }
        return Plan.movesOnly(table, moves);
    }

    @Override
    public boolean supportsSplit() {
        return false;
    }

    @Override
    public boolean supportsMove() {
        return true;
    }

    /**
     * 연속한 슬롯이 같은 주인이면 구간 하나로 압축했을 때의 크기: 구간마다 (시작 슬롯 4B + 노드 8B + epoch 8B), 헤더 16B.
     * Redis의 {@code CLUSTER SLOTS}도 같은 식으로 구간을 압축해 보낸다.
     */
    @Override
    public int metadataBytes(RoutingTable table) {
        int runs = 0;
        NodeId previous = null;
        for (Tablet t : table.tablets()) {
            if (!t.owner().equals(previous)) {
                runs++;
                previous = t.owner();
            }
        }
        return 16 + runs * 20;
    }

    /** 슬롯이 노드보다 적으면 슬롯을 하나도 못 받는 노드가 생겨 표에 나타나지 않는다. 그런 표는 만들지 않는다. */
    private void requireSlotsFor(int nodeCount) {
        if (nodeCount > slots) {
            throw new IllegalArgumentException(nodeCount + " nodes exceed " + slots + " slots");
        }
    }

    /** 노드별 Tablet 목록(시작점 오름차순). 노드 순서는 표에 처음 나타나는 순서. */
    private static Map<NodeId, List<Tablet>> ownedByNode(RoutingTable table) {
        Map<NodeId, List<Tablet>> m = new HashMap<>();
        for (Tablet t : table.tablets()) {
            m.computeIfAbsent(t.owner(), k -> new ArrayList<>()).add(t);
        }
        return m;
    }

    /** 가장 많이(또는 적게) 가진 노드. 동률이면 NodeId가 작은 쪽. */
    private static NodeId pick(Map<NodeId, List<Tablet>> owned, boolean most) {
        NodeId best = null;
        int bestCount = 0;
        for (Map.Entry<NodeId, List<Tablet>> e : owned.entrySet()) {
            int count = e.getValue().size();
            boolean better = best == null
                    || (most ? count > bestCount : count < bestCount)
                    || (count == bestCount && e.getKey().value() < best.value());
            if (better) {
                best = e.getKey();
                bestCount = count;
            }
        }
        return best;
    }
}
