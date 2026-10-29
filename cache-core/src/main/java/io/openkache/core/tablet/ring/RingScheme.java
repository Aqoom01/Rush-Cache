package io.openkache.core.tablet.ring;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * 링 vnode({@code ring-vnode}): consistent hashing 링 위에 노드마다 토큰 V개를 찍고, key는 해시 위치에서 시계 방향으로 처음 만나는
 * 토큰의 노드가 맡는다. Dynamo Strategy 1, Cassandra vnode 계열.
 *
 * <p>참고: Karger(STOC 1997) §4, Dynamo(SOSP 2007) §4.2·§6.2, Cassandra 문서. 정리는
 * {@code experiments/b1-tablet-distribution/references/dynamo-cassandra-vnode.md}, 결정은 {@code design.md} §4.4.
 *
 * <p>표로 표현하면 토큰 사이 구간이 Tablet이고 주인은 구간 <b>끝</b> 토큰의 노드다. 링의 이음새(마지막 토큰 → 2^64 → 0 → 첫 토큰)는
 * {@link RoutingTable}이 wrap을 못 하므로 Tablet 둘({@code [0, t0)}, {@code [t_last, 2^64)})로 나뉘고 둘 다 첫 토큰의 노드가 맡는다.
 * 그래서 Tablet 수는 토큰 수 + 1이다.
 *
 * <p>fixed-slot·hash-range-tablet과 다른 점은 <b>주인을 고를 수 없다</b>는 것이다. 토큰 위치가 곧 배치라 임의 이동이 없고
 * ({@link #supportsMove()} = false), 뜨거운 구간을 쪼개 다른 노드로 보낼 수도 없다({@link #supportsSplit()} = false).
 * 노드가 들어오면 새 토큰 바로 앞 구간만 새 노드로 가고(기존 노드끼리는 이동 없음), 나가면 그 노드의 구간은 시계 방향 다음 토큰의
 * 노드가 흡수한다. "표를 두면 무엇을 얻는가"를 재는 기준선이다.
 *
 * <p>토큰 위치는 {@link Placement}로 고른다. 둘 다 난수를 쓰지 않아 같은 입력이면 같은 표가 나온다(불변식 2).
 * <ul>
 *   <li>{@link Placement#RANDOM}: {@code KeyHash(nodeId, i)}. 무작위 배치(Dynamo Strategy 1, Cassandra 2.x)의 균형 문제를
 *       그대로 재현한다.</li>
 *   <li>{@link Placement#BALANCED}: 토큰마다 "가장 많이 맡은 노드의 가장 긴 호"의 앞부분을 새 노드가 공정 몫(1/(n+1))에 가까워질 만큼
 *       잘라 온다. Cassandra 3.0+ 배치 알고리즘(노드별 소유량이 고르도록 토큰 위치를 고름)에서 복제 인식 부분을 뺀 것이다.
 *       처음 표는 첫 노드만 {@code RANDOM}으로 찍고 나머지 노드는 이 규칙으로 차례로 들어온다.</li>
 * </ul>
 *
 * <p><b>표의 경계가 곧 토큰이다.</b> 노드가 나가면 그 토큰을 지우지 않고 시계 방향 다음 토큰의 노드가 <b>물려받는다</b>
 * (Dynamo §6.2 Strategy 3의 "떠난 노드의 토큰을 남은 노드에 재분배"). 나가는 순간의 조회 결과는 토큰을 지운 링과 같고, 다음에 노드가
 * 들어올 때는 물려받은 토큰도 경계로 작동한다. 새 토큰이 이미 있는 경계와 같으면(같은 NodeId가 다시 들어오는 경우) 그 경계에서 끝나는
 * Tablet을 통째로 되찾는다. 토큰을 지운 링이 필요하면(같은 주인의 이웃 토큰은 조회에 영향이 없다) 이동이 끝난 뒤
 * {@link #compact(RoutingTable)}로 합친다. 표에 토큰 생사를 따로 적지 않기 위한 선택이다.
 */
public final class RingScheme implements DistributionScheme {

    /** 새 노드의 토큰을 어디에 찍을지. */
    public enum Placement {
        /** {@code KeyHash(nodeId, i)}로 결정적 무작위. */
        RANDOM,
        /** 가장 많이 맡은 노드의 가장 긴 호에서, 새 노드가 공정 몫에 가까워질 만큼 잘라 온다. */
        BALANCED
    }

    /** 기본 노드당 토큰 수. Cassandra 4.0 기본값(배치 알고리즘과 함께)이다. 무작위 배치면 256 이상이 필요하다. */
    public static final int DEFAULT_TOKENS_PER_NODE = 16;

    /** 토큰 한 개의 직렬화 크기: 토큰 8B + NodeId 8B(다른 variant와 같은 크기). 노드끼리 공유하는 것은 표가 아니라 토큰 목록이다. */
    static final int BYTES_PER_TOKEN = 16;

    private final int tokensPerNode;
    private final Placement placement;

    public RingScheme(int tokensPerNode, Placement placement) {
        if (tokensPerNode < 1) {
            throw new IllegalArgumentException("tokensPerNode must be >= 1: " + tokensPerNode);
        }
        this.tokensPerNode = tokensPerNode;
        this.placement = Objects.requireNonNull(placement, "placement");
    }

    public RingScheme(int tokensPerNode) {
        this(tokensPerNode, Placement.RANDOM);
    }

    public RingScheme() {
        this(DEFAULT_TOKENS_PER_NODE);
    }

    public int tokensPerNode() {
        return tokensPerNode;
    }

    public Placement placement() {
        return placement;
    }

    @Override
    public String name() {
        return placement == Placement.RANDOM ? "ring-vnode" : "ring-vnode-alloc";
    }

    /**
     * 노드 {@code node}의 {@code i}번째 무작위 토큰. 공통 {@link KeyHash}를 (nodeId 8B, i 4B)에 적용한다.
     * 0은 토큰이 될 수 없으므로({@code [0, 0)}은 빈 구간) 1로 민다.
     */
    public static long token(NodeId node, int i) {
        byte[] b = ByteBuffer.allocate(12).putLong(node.value()).putInt(i).array();
        long t = KeyHash.of(Bytes.copyOf(b));
        return t == 0 ? 1 : t;
    }

    @Override
    public RoutingTable initial(List<NodeId> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (new HashSet<>(nodes).size() != nodes.size()) {
            throw new IllegalArgumentException("duplicate nodes: " + nodes);
        }
        // 토큰 → 노드. 키는 부호 비트를 뒤집은 토큰(signed 비교로 unsigned 순서)
        TreeMap<Long, NodeId> tokens = new TreeMap<>();
        placeRandom(tokens, nodes.get(0));
        if (placement == Placement.RANDOM) {
            for (int j = 1; j < nodes.size(); j++) {
                placeRandom(tokens, nodes.get(j));
            }
            return tableOf(tokens);
        }
        // BALANCED: 첫 노드 뒤로 한 노드씩 들어오는 것과 같다. 이동을 반영한 뒤 버전 1·epoch 1로 다시 만든다
        RoutingTable t = tableOf(tokens);
        for (int j = 1; j < nodes.size(); j++) {
            t = onNodeAdded(t, nodes.get(j)).applyAll();
        }
        return tableOf(tokensOf(t));
    }

    @Override
    public Plan onNodeAdded(RoutingTable table, NodeId node) {
        if (table.nodes().contains(node)) {
            throw new IllegalArgumentException("node already in table: " + node);
        }
        RoutingTable prepared = table;
        // 새 노드가 받을 Tablet. 새 토큰 t가 떨어진 Tablet [a, b)는 [a, t) → 새 노드, [t, b) → 토큰 b의 주인(그대로)이다.
        // 단, b도 이번에 찍은 새 토큰이면 [t, b)도 새 노드 것이다
        Set<TabletId> destined = new LinkedHashSet<>();
        Map<NodeId, Long> weight = placement == Placement.BALANCED ? weights(table) : null;
        long mine = 0;
        long fair = placement == Placement.BALANCED ? (1L << 62) / (table.nodes().size() + 1) : 0;
        for (int i = 0; i < tokensPerNode; i++) {
            long t;
            if (placement == Placement.RANDOM) {
                t = token(node, i);
            } else {
                Cut cut = cutFromRichest(prepared, destined, weight, fair - mine, tokensPerNode - i);
                t = cut.token();
                weight.put(cut.from(), weight.get(cut.from()) - cut.weight());
                mine += cut.weight();
            }
            Tablet hit = prepared.lookup(t);
            TabletId last = prepared.tablets().get(prepared.size() - 1).id();
            if (t == hit.range().start()) {
                // 경계가 이미 있다: 떠난 노드가 남긴(물려준) 토큰이거나(같은 NodeId 재합류면 V개 전부) 드문 해시 충돌이다.
                // 토큰을 되찾는 것이므로 그 경계에서 끝나는 Tablet [a, t)를 통째로 새 노드가 맡는다. 한 칸 밀어 쪼개면 폭 1짜리
                // Tablet만 받게 되므로 그렇게 하지 않는다. t >= 1이라 [a, t)는 항상 있다
                Tablet reclaimed = prepared.lookup(t - 1);
                destined.add(reclaimed.id());
                if (reclaimed.range().start() == 0) {
                    // t가 링에서 가장 작은 토큰이 됐다. 이음새 Tablet [t_last, 2^64)는 첫 토큰의 것이므로 함께 새 노드로 간다
                    destined.add(last);
                }
                continue;
            }
            TabletId right = new TabletId(prepared.nextTabletId());
            boolean wasFirst = hit.range().start() == 0;
            prepared = prepared.withSplit(hit.id(), t);
            if (destined.contains(hit.id())) {
                destined.add(right);
            } else {
                destined.add(hit.id());
                if (wasFirst) {
                    // 새 토큰이 링에서 가장 작은 토큰이 됐다. 이음새 Tablet [t_last, 2^64)는 첫 토큰의 것이므로 함께 새 노드로 간다
                    destined.add(last);
                }
            }
        }
        List<Change> moves = new ArrayList<>(destined.size());
        for (TabletId id : destined) {
            moves.add(new Change(id, prepared.get(id).owner(), node));
        }
        return new Plan(prepared, moves);
    }

    @Override
    public Plan onNodeRemoved(RoutingTable table, NodeId node) {
        if (!table.nodes().contains(node)) {
            throw new IllegalArgumentException("node not in table: " + node);
        }
        if (table.nodes().size() == 1) {
            throw new IllegalStateException("cannot remove the last node: " + node);
        }
        List<Tablet> all = table.tablets();
        List<Change> moves = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            Tablet t = all.get(i);
            if (!t.owner().equals(node)) {
                continue;
            }
            // 시계 방향 다음 Tablet의 주인. 그것도 떠나는 노드면 계속 간다(끝에서 0으로 wrap)
            int j = i;
            NodeId receiver;
            do {
                j = (j + 1) % all.size();
                receiver = all.get(j).owner();
            } while (receiver.equals(node));
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
        return false;
    }

    /** 헤더 16B + 토큰마다 {@link #BYTES_PER_TOKEN}. 토큰 수 = Tablet 수 − 1 (이음새 Tablet 둘이 토큰 하나를 공유). */
    @Override
    public int metadataBytes(RoutingTable table) {
        return 16 + (table.size() - 1) * BYTES_PER_TOKEN;
    }

    /**
     * 이웃하고 주인이 같은 Tablet을 전부 합친 표. 노드 제거 뒤 남은 경계를 지울 때 쓴다. 같은 노드의 이웃한 토큰도 합쳐지므로
     * (뒤 토큰은 조회에 영향이 없다) 결과는 "토큰 수 + 1"보다 작을 수 있다. 이음새 Tablet 둘은 합치지 않는다.
     * 합칠 때마다 표 버전이 1씩 오른다.
     */
    public static RoutingTable compact(RoutingTable table) {
        RoutingTable t = table;
        int i = 0;
        // 마지막 두 Tablet(이음새)은 합치지 않는다. 합치면 토큰이 하나도 없는 노드가 생길 수 있다(노드가 하나 남았을 때)
        while (i + 2 < t.size()) {
            Tablet a = t.tablets().get(i);
            Tablet b = t.tablets().get(i + 1);
            if (a.owner().equals(b.owner())) {
                t = t.withMerge(a.id(), b.id());
            } else {
                i++;
            }
        }
        return t;
    }

    // ---- 토큰 배치 ----

    private void placeRandom(TreeMap<Long, NodeId> tokens, NodeId node) {
        for (int i = 0; i < tokensPerNode; i++) {
            long t = token(node, i);
            while (tokens.containsKey(t ^ Long.MIN_VALUE)) {
                t += 1;
                if (t == 0) {
                    t = 1;
                }
            }
            tokens.put(t ^ Long.MIN_VALUE, node);
        }
    }

    /** BALANCED가 한 토큰으로 잘라 오는 조각: 토큰 위치, 내주는 노드, 조각 무게. */
    private record Cut(long token, NodeId from, long weight) {}

    /**
     * 가장 많이 맡은 노드(동률이면 NodeId 작은 쪽)의 가장 긴 호(동률이면 시작점 작은 쪽) 앞부분을 잘라 온다. 조각 무게는
     * "남은 부족분 ÷ 남은 토큰 수"를 목표로 하되 호보다 작아야 한다(호 전체를 가져가면 토큰이 겹친다).
     * 이음새 Tablet 둘은 {@code t_last}에서 시작하는 호 하나로 본다. 이미 새 노드로 가기로 한 조각({@code destined})은 후보에서
     * 뺀다(같은 조각을 두 번 잘라 무게를 이중으로 세지 않도록).
     */
    private static Cut cutFromRichest(
            RoutingTable prepared, Set<TabletId> destined, Map<NodeId, Long> weight, long deficit, int tokensLeft) {
        NodeId richest = null;
        for (Map.Entry<NodeId, Long> e : weight.entrySet()) {
            if (richest == null || e.getValue() > weight.get(richest)
                    || (e.getValue().equals(weight.get(richest)) && e.getKey().value() < richest.value())) {
                richest = e.getKey();
            }
        }
        List<Tablet> all = prepared.tablets();
        Tablet first = all.get(0);
        Tablet last = all.get(all.size() - 1);
        long bestStart = 0;
        long bestWeight = -1;
        if (last.owner().equals(richest) && !destined.contains(last.id())) {
            bestStart = last.range().start();
            bestWeight = weight(last.range()) + weight(first.range());
        }
        for (int i = 1; i + 1 < all.size(); i++) {
            Tablet t = all.get(i);
            if (!t.owner().equals(richest) || destined.contains(t.id())) {
                continue;
            }
            long w = weight(t.range());
            if (w > bestWeight || (w == bestWeight && Long.compareUnsigned(t.range().start(), bestStart) < 0)) {
                bestWeight = w;
                bestStart = t.range().start();
            }
        }
        if (bestWeight < 2) {
            throw new IllegalStateException("richest node " + richest + " has no arc to cut");
        }
        long want = Math.max(1, deficit / Math.max(1, tokensLeft));
        long piece = Math.min(want, bestWeight - 1);
        return new Cut(bestStart + (piece << 2), richest, piece);
    }

    /** 구간 길이를 4로 나눈 값(노드별 합이 long에 들어가게). hash-range-tablet과 같은 규칙. */
    static long weight(TabletRange r) {
        if (r.equals(TabletRange.FULL)) {
            return 1L << 62;
        }
        return (r.end() - r.start()) >>> 2;
    }

    private static Map<NodeId, Long> weights(RoutingTable table) {
        Map<NodeId, Long> w = new HashMap<>();
        for (Tablet t : table.tablets()) {
            w.merge(t.owner(), weight(t.range()), Long::sum);
        }
        return w;
    }

    private static long avoidZero(long t) {
        return t == 0 ? 1 : t;
    }

    /** 표의 경계 → 그 경계에서 끝나는 Tablet의 주인. 이음새(2^64)는 토큰이 아니다. */
    private static TreeMap<Long, NodeId> tokensOf(RoutingTable table) {
        TreeMap<Long, NodeId> tokens = new TreeMap<>();
        for (Tablet t : table.tablets()) {
            if (t.range().end() != 0) {
                tokens.put(t.range().end() ^ Long.MIN_VALUE, t.owner());
            }
        }
        return tokens;
    }

    /** 토큰 목록 → 표. Tablet j = [t_{j-1}, t_j), 주인 = t_j의 노드. 끝에 [t_last, 2^64)를 첫 토큰의 노드로 붙인다. */
    private static RoutingTable tableOf(TreeMap<Long, NodeId> tokens) {
        List<Tablet> tablets = new ArrayList<>(tokens.size() + 1);
        OwnershipEpoch first = OwnershipEpoch.ZERO.next();
        long prev = 0;
        long id = 0;
        NodeId firstOwner = null;
        for (var e : tokens.entrySet()) {
            long t = avoidZero(e.getKey() ^ Long.MIN_VALUE);
            if (firstOwner == null) {
                firstOwner = e.getValue();
            }
            tablets.add(new Tablet(new TabletId(id++), new TabletRange(prev, t), e.getValue(), first));
            prev = t;
        }
        tablets.add(new Tablet(new TabletId(id++), new TabletRange(prev, 0), firstOwner, first));
        return RoutingTable.of(RoutingVersion.ZERO.next(), tablets, id);
    }
}
