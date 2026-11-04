package io.openkache.core.tablet.ring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * 노드 추가·제거를 무작위 순서로 섞어 돌리며 ring-vnode가 "진짜 링"과 같은 답을 내는지 검사한다.
 *
 * <p>오라클: 계획의 {@code prepared}에 새로 생긴 경계가 곧 새 노드의 토큰이다. 토큰 → 노드 맵을 따로 들고, 매 단계 무작위 해시의
 * {@code lookup} 주인이 "시계 방향으로 처음 만나는 토큰의 노드"와 같아야 한다. 노드 제거는 떠나는 노드의 토큰을 시계 방향 다음 토큰의
 * 노드가 물려받고, {@code compact}는 조회를 바꾸지 않아야 한다(합친 뒤에는 표의 경계로 오라클을 다시 만든다).
 * 배치 규칙 둘 다 검사한다. 그 밖에: 추가 시 모든 이동이 새 노드로 가고 기존 노드의 Tablet 수는 그대로, 제거 시 떠나는 노드에서만 나간다.
 */
class RingRandomOpsTest {

    @Test
    void randomJoinsAndLeavesMatchTheRingOracle() {
        for (RingScheme.Placement p : RingScheme.Placement.values()) {
            for (int perNode : new int[] {1, 4, 16}) {
                for (long seed = 1; seed <= 4; seed++) {
                    run(new RingScheme(perNode, p), new Random(seed), 80);
                }
            }
        }
    }

    private static void run(RingScheme scheme, Random rnd, int steps) {
        List<NodeId> members = new ArrayList<>(List.of(new NodeId(1), new NodeId(2)));
        int nextId = 3;
        RoutingTable t = scheme.initial(members);
        TreeMap<Long, NodeId> ring = ringOf(t);
        checkLookup(ring, t, rnd);

        for (int step = 0; step < steps; step++) {
            boolean add = members.size() < 2 || (members.size() < 12 && rnd.nextBoolean());
            if (add) {
                NodeId node = new NodeId(nextId++);
                Plan plan = scheme.onNodeAdded(t, node);
                assertEquals(plan.moves(), scheme.onNodeAdded(t, node).moves(), "deterministic");
                Set<Long> before = starts(t);
                int newTokens = 0;
                for (Tablet tablet : plan.prepared().tablets()) {
                    if (!before.contains(tablet.range().start())) {
                        ring.put(tablet.range().start() ^ Long.MIN_VALUE, node);
                        newTokens++;
                    }
                }
                assertEquals(scheme.tokensPerNode(), newTokens, "one new boundary per token");
                for (Change c : plan.moves()) {
                    assertEquals(node, c.to());
                }
                RoutingTable next = plan.applyAll();
                // 기존 노드는 Tablet을 잃지 않는다. 예외는 이음새 하나: 새 토큰이 가장 작은 토큰이 되면 [t_last, 2^64)가 새 노드로 간다
                int lost = 0;
                for (NodeId m : members) {
                    int diff = t.ownedBy(m).size() - next.ownedBy(m).size();
                    assertTrue(diff == 0 || diff == 1, "old node " + m + " lost " + diff);
                    lost += diff;
                }
                assertTrue(lost <= 1, "at most the seam tablet leaves an old node");
                assertTrue(next.ownedBy(node).size() >= scheme.tokensPerNode());
                members.add(node);
                t = next;
            } else {
                NodeId node = members.get(rnd.nextInt(members.size()));
                Plan plan = scheme.onNodeRemoved(t, node);
                assertEquals(t, plan.prepared());
                for (Change c : plan.moves()) {
                    assertEquals(node, c.from());
                }
                RoutingTable next = plan.applyAll();
                assertEquals(0, next.ownedBy(node).size());
                assertEquals(t.size(), next.size(), "boundaries stay");
                handOver(ring, node);
                members.remove(node);
                t = next;
                if (rnd.nextInt(4) == 0) {
                    RoutingTable compacted = RingScheme.compact(t);
                    checkLookup(ring, compacted, rnd); // 합쳐도 조회는 그대로
                    // 같은 주인의 이웃한 토큰이 지워지므로 "토큰 수 + 1" 이하. 이후 오라클은 남은 경계로 다시 만든다
                    assertTrue(compacted.size() <= ring.size() + 1, "compacted size " + compacted.size());
                    t = compacted;
                    ring = ringOf(t);
                }
            }
            assertEquals(new HashSet<>(members), t.nodes());
            checkLookup(ring, t, rnd);
        }
    }

    /** 표의 경계 → 그 경계에서 끝나는 Tablet의 주인. 토큰 t는 (이전 토큰, t]를 맡는다. */
    private static TreeMap<Long, NodeId> ringOf(RoutingTable t) {
        TreeMap<Long, NodeId> ring = new TreeMap<>();
        for (Tablet tablet : t.tablets()) {
            if (tablet.range().end() != 0) {
                ring.put(tablet.range().end() ^ Long.MIN_VALUE, tablet.owner());
            }
        }
        return ring;
    }

    /** 떠나는 노드의 토큰을 시계 방향 다음 토큰(떠나는 노드 것이 아닌)의 주인에게 넘긴다. */
    private static void handOver(TreeMap<Long, NodeId> ring, NodeId leaving) {
        List<Long> keys = new ArrayList<>(ring.keySet());
        for (int i = 0; i < keys.size(); i++) {
            if (!ring.get(keys.get(i)).equals(leaving)) {
                continue;
            }
            int j = i;
            NodeId receiver;
            do {
                j = (j + 1) % keys.size();
                receiver = ring.get(keys.get(j));
            } while (receiver.equals(leaving));
            ring.put(keys.get(i), receiver);
        }
    }

    private static Set<Long> starts(RoutingTable t) {
        Set<Long> s = new HashSet<>();
        for (Tablet tablet : t.tablets()) {
            s.add(tablet.range().start());
        }
        return s;
    }

    private static void checkLookup(TreeMap<Long, NodeId> ring, RoutingTable t, Random rnd) {
        for (int i = 0; i < 500; i++) {
            long h = rnd.nextLong();
            assertEquals(clockwise(ring, h), t.lookup(h).owner(), "hash " + Long.toUnsignedString(h));
        }
        for (long k : ring.keySet()) {
            long token = k ^ Long.MIN_VALUE;
            assertEquals(clockwise(ring, token), t.lookup(token).owner());
            assertEquals(clockwise(ring, token - 1), t.lookup(token - 1).owner());
        }
    }

    private static NodeId clockwise(TreeMap<Long, NodeId> ring, long hash) {
        Map.Entry<Long, NodeId> e = ring.higherEntry(hash ^ Long.MIN_VALUE);
        return e != null ? e.getValue() : ring.firstEntry().getValue();
    }
}
