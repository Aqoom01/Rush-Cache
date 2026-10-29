package io.openkache.core.tablet.resize;

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
 * 크기 조절 슬롯({@code resizable-slot}): fixed-slot처럼 해시 공간을 균등 슬롯으로 나누되, 슬롯 수를 운영 중에 <b>전체 2배 / 절반</b>으로만
 * 바꾼다. ScyllaDB tablets 계열(테이블의 tablet 수를 한꺼번에 늘리고 줄인다). 슬롯 수는 항상 2의 거듭제곱이다.
 *
 * <p>참고: ScyllaDB "Data Distribution with Tablets" 문서의 tablet resize(split/merge of all tablets). 정리는
 * {@code experiments/b1-tablet-distribution/references/scylla-tablet-resize.md}, 결정은 {@code design.md} §4.6.
 *
 * <p>fixed-slot과 다른 점은 슬롯 수 S가 노드 수에 따라 움직인다는 것뿐이다. 노드당 슬롯 수 S/n이 {@code minPerNode} 아래로 떨어지면 2배,
 * {@code maxPerNode} 위로 올라가면 절반으로 한다. 기본은 목표 T의 절반과 2배다(히스테리시스: 2배 직후 S/n ≥ T, 절반 직후 S/n ≤ T).
 * hash-range-tablet과 달리 슬롯 하나만 쪼갤 수는 없으므로 {@link #supportsSplit()}는 {@code false}다.
 *
 * <ul>
 *   <li><b>2배({@link #doubled})</b>: 모든 슬롯을 한가운데서 쪼갠다. 왼쪽 조각은 id를 유지하고 오른쪽 조각은 새 id를 받는다
 *       ({@link RoutingTable#withSplit}과 같은 규칙). 주인·epoch는 그대로이므로 데이터 이동이 없다. 노드 추가 때 필요하면
 *       {@link #onNodeAdded}가 계획의 {@code prepared}로 먼저 적용한다.</li>
 *   <li><b>절반({@link #halving} → {@link #merged})</b>: 이웃한 짝(표의 2k, 2k+1번째)을 합친다. 짝의 주인이 다르면 먼저 한쪽으로 옮겨야
 *       하므로(ScyllaDB도 sibling을 같은 복제 집합에 모은 뒤 합친다) 두 단계다. {@link #halving}은 짝을 같은 주인으로 모으는 이동 계획이고,
 *       이동이 끝나면 {@link #merged}가 메타데이터만 바꿔 합친다. 노드 제거 계획의 이동은 모두 떠나는 노드에서 나가야 하므로
 *       (불변식 6) 절반은 {@link #onNodeRemoved} 안에서 하지 않고, 호출자가 {@link #decide}를 보고 따로 한다.</li>
 * </ul>
 *
 * <p>노드 추가는 "가장 많이 가진 노드의 마지막 슬롯"을 새 노드가 그 노드와 같아지기 직전까지 가져온다(hash-range-tablet의 길이 규칙을 개수로
 * 옮긴 것). 2배 직후에는 노드 간 차이가 2가 될 수 있는데, 이 규칙은 그 표에서도 결과를 차이 1 이하로 만든다. 노드 제거는 떠나는 노드의
 * 슬롯을 "가장 적게 가진 노드"에 하나씩 주되, 동률이면 그 슬롯의 짝을 가진 노드를 우선한다(뒤따르는 절반의 이동을 줄인다). 그다음은
 * {@link NodeId}가 작은 쪽이다.
 */
public final class ResizableSlotScheme implements DistributionScheme {

    /** 기본 노드당 목표 슬롯 수. fixed-slot 기본값 4096을 노드 8대로 나눈 값이다. */
    public static final int DEFAULT_SLOTS_PER_NODE = 512;

    /** 슬롯 수 상한. 이 이상으로는 2배 하지 않는다. */
    static final int MAX_SLOTS = 1 << 24;

    /** 표를 받고 할 일. */
    public enum Resize {
        NONE,
        DOUBLE,
        HALVE
    }

    private final int slotsPerNode;
    private final int minPerNode;
    private final int maxPerNode;

    /**
     * @param slotsPerNode 노드당 목표 슬롯 수 T. 처음 표는 노드 수 × T 이상인 가장 작은 2의 거듭제곱
     * @param minPerNode 노드당 슬롯 수가 이 아래로 떨어지면 2배
     * @param maxPerNode 노드당 슬롯 수가 이 위로 올라가면 절반
     */
    public ResizableSlotScheme(int slotsPerNode, int minPerNode, int maxPerNode) {
        if (slotsPerNode < 1 || minPerNode < 1 || maxPerNode < 1) {
            throw new IllegalArgumentException("slot counts must be >= 1: " + slotsPerNode + ", " + minPerNode + ", " + maxPerNode);
        }
        if (minPerNode > slotsPerNode || slotsPerNode > maxPerNode) {
            throw new IllegalArgumentException("need minPerNode <= slotsPerNode <= maxPerNode: " + minPerNode + ", " + slotsPerNode + ", " + maxPerNode);
        }
        if (maxPerNode < 2L * minPerNode) {
            throw new IllegalArgumentException("maxPerNode must be >= 2 * minPerNode or resizing oscillates: " + minPerNode + ", " + maxPerNode);
        }
        this.slotsPerNode = slotsPerNode;
        this.minPerNode = minPerNode;
        this.maxPerNode = maxPerNode;
    }

    /** 목표 T, 하한 T/2, 상한 2T. */
    public ResizableSlotScheme(int slotsPerNode) {
        this(slotsPerNode, Math.max(1, slotsPerNode / 2), Math.multiplyExact(slotsPerNode, 2));
    }

    public ResizableSlotScheme() {
        this(DEFAULT_SLOTS_PER_NODE);
    }

    public int slotsPerNode() {
        return slotsPerNode;
    }

    public int minPerNode() {
        return minPerNode;
    }

    public int maxPerNode() {
        return maxPerNode;
    }

    @Override
    public String name() {
        return "resizable-slot";
    }

    /** 노드 수 × T 이상인 가장 작은 2의 거듭제곱. */
    public int initialSlots(int nodeCount) {
        long want = (long) nodeCount * slotsPerNode;
        if (want > MAX_SLOTS) {
            throw new IllegalArgumentException(nodeCount + " nodes x " + slotsPerNode + " exceeds " + MAX_SLOTS + " slots");
        }
        int s = 1;
        while (s < want) {
            s <<= 1;
        }
        return s;
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
        int slots = initialSlots(n);
        List<TabletRange> ranges = TabletRange.uniform(slots);
        List<Tablet> tablets = new ArrayList<>(slots);
        OwnershipEpoch first = OwnershipEpoch.ZERO.next();
        // 연속 블록 배치(fixed-slot과 같은 이유: 표 압축). 나머지는 앞 노드부터 1개씩 더 받는다
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

    /** 슬롯 {@code slots}개를 노드 {@code nodeCount}대가 나눠 가질 때 할 일. 절반은 모든 노드가 슬롯을 하나 이상 유지할 수 있을 때만. */
    public Resize decide(int slots, int nodeCount) {
        int perNode = slots / nodeCount;
        if (perNode < minPerNode) {
            return slots * 2L <= MAX_SLOTS ? Resize.DOUBLE : Resize.NONE;
        }
        if (perNode > maxPerNode && slots / 2 >= nodeCount) {
            return Resize.HALVE;
        }
        return Resize.NONE;
    }

    public Resize decide(RoutingTable table) {
        return decide(table.size(), table.nodes().size());
    }

    /**
     * 모든 슬롯을 한가운데서 쪼갠 표. 왼쪽 조각은 id·주인·epoch를 유지하고, 오른쪽 조각은 {@link RoutingTable#nextTabletId()}부터 차례로
     * 새 id를 받는다. 버전은 1 오른다. 주인이 안 바뀌므로 데이터 이동이 없다.
     */
    public RoutingTable doubled(RoutingTable table) {
        requireUniformPowerOfTwo(table);
        int size = table.size();
        if (size * 2L > MAX_SLOTS) {
            throw new IllegalStateException("cannot double beyond " + MAX_SLOTS + " slots: " + size);
        }
        List<TabletRange> ranges = TabletRange.uniform(size * 2);
        List<Tablet> out = new ArrayList<>(size * 2);
        long nextId = table.nextTabletId();
        for (int i = 0; i < size; i++) {
            Tablet t = table.tablets().get(i);
            out.add(new Tablet(t.id(), ranges.get(2 * i), t.owner(), t.epoch()));
            out.add(new Tablet(new TabletId(nextId + i), ranges.get(2 * i + 1), t.owner(), t.epoch()));
        }
        return RoutingTable.of(table.version().next(), out, Math.addExact(nextId, size));
    }

    /**
     * 절반 1단계: 이웃한 짝(2k, 2k+1번째)의 주인이 다르면 한쪽으로 모으는 이동 계획. 어느 쪽으로 모을지는 "지금 슬롯이 더 적은 노드"로 정하고
     * (동률이면 NodeId가 작은 쪽), 그래도 노드 간 짝 수 차이가 1을 넘으면 가장 많은 노드의 짝을 가장 적은 노드로 옮겨 1 이하로 만든다.
     * {@code prepared}는 입력 표 그대로다. 이동이 모두 반영된 뒤 {@link #merged}를 부른다.
     */
    public Plan halving(RoutingTable table) {
        requireUniformPowerOfTwo(table);
        int size = table.size();
        if (size < 2) {
            throw new IllegalStateException("cannot halve a single slot");
        }
        List<Tablet> tablets = table.tablets();
        Map<NodeId, Integer> pairs = new HashMap<>();
        NodeId[] pairOwner = new NodeId[size / 2];
        for (NodeId n : table.nodes()) {
            pairs.put(n, 0);
        }
        List<Change> moves = new ArrayList<>();
        // 1) 주인이 같은 짝은 그대로. 다른 짝은 뒤로 미뤄 둔다(지금 적은 노드가 어디인지는 같은 짝을 센 뒤에 안다)
        List<Integer> mixed = new ArrayList<>();
        for (int k = 0; k < size / 2; k++) {
            NodeId a = tablets.get(2 * k).owner();
            NodeId b = tablets.get(2 * k + 1).owner();
            if (a.equals(b)) {
                pairOwner[k] = a;
                pairs.merge(a, 1, Integer::sum);
            } else {
                mixed.add(k);
            }
        }
        // 2) 섞인 짝은 둘 중 짝이 적은 노드로. 이동은 짝마다 정확히 1건이다
        for (int k : mixed) {
            Tablet left = tablets.get(2 * k);
            Tablet right = tablets.get(2 * k + 1);
            NodeId a = left.owner();
            NodeId b = right.owner();
            int ca = pairs.get(a);
            int cb = pairs.get(b);
            boolean toA = ca < cb || (ca == cb && a.value() < b.value());
            NodeId to = toA ? a : b;
            Tablet moving = toA ? right : left;
            moves.add(new Change(moving.id(), moving.owner(), to));
            pairOwner[k] = to;
            pairs.merge(to, 1, Integer::sum);
        }
        // 3) 짝 수 차이를 1 이하로. 가장 많은 노드의 마지막 짝을 가장 적은 노드로(짝 하나 = 이동 2건)
        while (true) {
            NodeId most = extreme(pairs, true);
            NodeId least = extreme(pairs, false);
            if (pairs.get(most) - pairs.get(least) <= 1) {
                break;
            }
            int k = lastPairOf(pairOwner, most);
            for (int i = 2 * k; i <= 2 * k + 1; i++) {
                Tablet t = tablets.get(i);
                NodeId from = t.owner();
                int pending = indexOfMove(moves, t.id()); // 2단계에서 이미 옮기기로 한 슬롯이면 그 이동을 고친다
                if (from.equals(least)) {
                    if (pending >= 0) {
                        moves.remove(pending); // 원래 least 것이었는데 most로 모으기로 했던 슬롯: 안 옮기면 된다
                    }
                } else if (pending >= 0) {
                    moves.set(pending, new Change(t.id(), from, least));
                } else {
                    moves.add(new Change(t.id(), from, least));
                }
            }
            pairOwner[k] = least;
            pairs.merge(most, -1, Integer::sum);
            pairs.merge(least, 1, Integer::sum);
        }
        return Plan.movesOnly(table, moves);
    }

    /**
     * 절반 2단계: 이웃한 짝을 합친 표. 짝의 주인이 모두 같아야 한다({@link #halving}의 이동이 끝난 뒤). 합친 슬롯은 왼쪽 id와 둘 중 큰 epoch를
     * 가진다({@link RoutingTable#withMerge}와 같은 규칙). 버전은 1 오른다.
     *
     * @throws IllegalStateException 주인이 다른 짝이 남아 있을 때
     */
    public RoutingTable merged(RoutingTable table) {
        requireUniformPowerOfTwo(table);
        int size = table.size();
        if (size < 2) {
            throw new IllegalStateException("cannot halve a single slot");
        }
        List<TabletRange> ranges = TabletRange.uniform(size / 2);
        List<Tablet> out = new ArrayList<>(size / 2);
        for (int k = 0; k < size / 2; k++) {
            Tablet a = table.tablets().get(2 * k);
            Tablet b = table.tablets().get(2 * k + 1);
            if (!a.owner().equals(b.owner())) {
                throw new IllegalStateException("siblings are not co-located: " + a + ", " + b + "; apply halving() moves first");
            }
            OwnershipEpoch e = a.epoch().isNewerThan(b.epoch()) ? a.epoch() : b.epoch();
            out.add(new Tablet(a.id(), ranges.get(k), a.owner(), e));
        }
        return RoutingTable.of(table.version().next(), out, table.nextTabletId());
    }

    @Override
    public Plan onNodeAdded(RoutingTable table, NodeId node) {
        if (table.nodes().contains(node)) {
            throw new IllegalArgumentException("node already in table: " + node);
        }
        RoutingTable prepared = table;
        int n = table.nodes().size() + 1;
        while (decide(prepared.size(), n) == Resize.DOUBLE) {
            prepared = doubled(prepared);
        }
        if (prepared.size() < n) {
            throw new IllegalArgumentException(n + " nodes exceed " + prepared.size() + " slots");
        }
        Map<NodeId, List<Tablet>> owned = ownedByNode(prepared);
        List<Change> moves = new ArrayList<>();
        int mine = 0;
        while (true) {
            NodeId donor = pick(owned, true);
            List<Tablet> list = owned.get(donor);
            // 받고 나서 donor와 같아지거나 넘으면 자리만 바뀌는 이동이므로 멈춘다. 2배 직후의 차이 2도 이 규칙으로 1 이하가 된다
            if (mine + 1 >= list.size()) {
                break;
            }
            Tablet taken = list.remove(list.size() - 1);
            mine++;
            moves.add(new Change(taken.id(), donor, node));
        }
        return new Plan(prepared, moves);
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
        List<Tablet> tablets = table.tablets();
        Map<TabletId, Integer> position = new HashMap<>();
        for (int i = 0; i < tablets.size(); i++) {
            position.put(tablets.get(i).id(), i);
        }
        Map<TabletId, NodeId> assigned = new HashMap<>();
        List<Change> moves = new ArrayList<>(leaving.size());
        for (Tablet t : leaving) {
            int i = position.get(t.id());
            Tablet pair = tablets.get(i ^ 1); // 짝: 2k ↔ 2k+1
            NodeId sibling = pair.owner().equals(node) ? assigned.get(pair.id()) : pair.owner(); // 짝도 떠나는 중이면 방금 정한 곳
            NodeId receiver = pickLeast(owned, sibling);
            owned.get(receiver).add(t);
            assigned.put(t.id(), receiver);
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
     * fixed-slot과 같은 구간 압축(연속한 슬롯의 주인이 같으면 구간 하나: 시작 슬롯 4B + 노드 8B + epoch 8B) + 헤더 16B + 슬롯 수와 resize
     * 이력 8B. 슬롯 경계는 균등이라 보낼 필요가 없고, id는 resize 이력이 정해진 순서(왼쪽 유지·오른쪽 새 번호)라 위치에서 복원된다.
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
        return 24 + runs * 20;
    }

    /** 슬롯 수가 2의 거듭제곱이고 경계가 균등 분할인지. 이 variant의 표는 split·merge를 따로 하지 않으므로 항상 그래야 한다. */
    private static void requireUniformPowerOfTwo(RoutingTable table) {
        int size = table.size();
        if (Integer.bitCount(size) != 1) {
            throw new IllegalStateException("slot count is not a power of two: " + size);
        }
        List<TabletRange> uniform = TabletRange.uniform(size);
        for (int i = 0; i < size; i++) {
            if (!table.tablets().get(i).range().equals(uniform.get(i))) {
                throw new IllegalStateException("slot " + i + " is not a uniform slot: " + table.tablets().get(i));
            }
        }
    }

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

    /** 가장 적게 가진 노드. 동률이면 {@code preferred}(짝의 주인), 그다음 NodeId가 작은 쪽. */
    private static NodeId pickLeast(Map<NodeId, List<Tablet>> owned, NodeId preferred) {
        NodeId best = pick(owned, false);
        if (preferred != null && owned.containsKey(preferred) && owned.get(preferred).size() == owned.get(best).size()) {
            return preferred;
        }
        return best;
    }

    private static NodeId extreme(Map<NodeId, Integer> pairs, boolean most) {
        NodeId best = null;
        int bestCount = 0;
        for (Map.Entry<NodeId, Integer> e : pairs.entrySet()) {
            int count = e.getValue();
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

    private static int indexOfMove(List<Change> moves, TabletId id) {
        for (int m = 0; m < moves.size(); m++) {
            if (moves.get(m).unit().equals(id)) {
                return m;
            }
        }
        return -1;
    }

    private static int lastPairOf(NodeId[] pairOwner, NodeId node) {
        for (int k = pairOwner.length - 1; k >= 0; k--) {
            if (node.equals(pairOwner[k])) {
                return k;
            }
        }
        throw new IllegalStateException("node owns no pair: " + node);
    }
}
