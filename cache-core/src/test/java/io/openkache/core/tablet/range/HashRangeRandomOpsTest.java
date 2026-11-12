package io.openkache.core.tablet.range;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * split·merge·노드 추가·제거를 무작위 순서로 섞어 돌리며 hash-range-tablet의 규칙을 검사한다.
 *
 * <p>검사: 계획은 경계를 안 바꾸고 결정적이다. 추가 시 모든 이동이 새 노드로 가고 새 노드 길이는 [평균 − 가장 긴 Tablet, 추가 전 최대]
 * 안이며 기존 노드는 늘지 않는다. 제거 시 떠나는 노드에서만 나가고 노드 간 길이 차이는 max(이전 차이, 가장 긴 Tablet)을 넘지 않는다.
 * 어느 경우든 모든 노드가 표에 남고, 옮기지 않은 Tablet은 그대로, 옮긴 Tablet은 epoch가 1 오른다.
 */
class HashRangeRandomOpsTest {

    @Test
    void randomSplitsMergesJoinsAndLeavesKeepRules() {
        for (int perNode : new int[] {1, 4, 16}) {
            for (long seed = 1; seed <= 4; seed++) {
                run(new HashRangeScheme(perNode), new Random(seed), 120);
            }
        }
    }

    private static void run(HashRangeScheme scheme, Random rnd, int steps) {
        List<NodeId> members = new ArrayList<>(List.of(new NodeId(1), new NodeId(2)));
        int nextId = 3;
        RoutingTable t = scheme.initial(members);

        for (int step = 0; step < steps; step++) {
            int op = rnd.nextInt(10);
            if (op < 3) {
                t = randomSplit(t, rnd);
            } else if (op < 5) {
                t = randomMerge(t, rnd);
            } else if (op < 8 && members.size() < 10) {
                if (t.size() < members.size() + 1) {
                    t = randomSplit(t, rnd); // Tablet이 노드보다 적으면 새 노드가 못 들어온다
                    continue;
                }
                NodeId node = new NodeId(nextId++);
                Plan plan = scheme.onNodeAdded(t, node);
                assertEquals(plan.moves(), scheme.onNodeAdded(t, node).moves(), "plan must be deterministic");
                RoutingTable next = checkCommon(t, plan, members);
                Map<NodeId, Long> before = weights(t, members);
                long maxBefore = before.values().stream().mapToLong(Long::longValue).max().orElseThrow();
                for (Change c : plan.moves()) {
                    assertEquals(node, c.to());
                }
                members.add(node);
                Map<NodeId, Long> after = weights(next, members);
                long mean = after.values().stream().mapToLong(Long::longValue).sum() / members.size();
                long mine = after.get(node);
                assertTrue(mine >= mean - maxTablet(t), "new node too light: " + mine + " mean=" + mean);
                assertTrue(mine <= maxBefore, "new node heavier than previous max");
                for (NodeId m : before.keySet()) {
                    assertTrue(after.get(m) <= before.get(m), "existing node grew: " + m);
                }
                t = next;
            } else if (members.size() > 1) {
                NodeId node = members.remove(rnd.nextInt(members.size()));
                Plan plan = scheme.onNodeRemoved(t, node);
                assertEquals(plan.moves(), scheme.onNodeRemoved(t, node).moves(), "plan must be deterministic");
                assertEquals(t.ownedBy(node).size(), plan.moves().size());
                for (Change c : plan.moves()) {
                    assertEquals(node, c.from());
                    assertTrue(members.contains(c.to()));
                }
                long spreadBefore = spread(weights(t, members));
                RoutingTable next = checkCommon(t, plan, members);
                assertFalse(next.nodes().contains(node));
                long spreadAfter = spread(weights(next, members));
                assertTrue(spreadAfter <= Math.max(spreadBefore, maxTablet(t)),
                        "spread " + spreadAfter + " > max(" + spreadBefore + ", " + maxTablet(t) + ")");
                t = next;
            }
        }
    }

    /** 계획의 공통 규칙을 검사하고 적용한 표를 돌려준다. */
    private static RoutingTable checkCommon(RoutingTable t, Plan plan, List<NodeId> membersAfter) {
        assertTrue(plan.prepared() == t, "hash-range never changes boundaries by itself");
        Set<TabletId> moved = new HashSet<>();
        for (Change c : plan.moves()) {
            assertTrue(moved.add(c.unit()), "a tablet must not be moved twice in one plan");
            assertEquals(t.get(c.unit()).owner(), c.from());
        }
        RoutingTable next = plan.applyAll();
        assertEquals(t.size(), next.size());
        for (int i = 0; i < t.size(); i++) {
            Tablet before = t.tablets().get(i);
            Tablet after = next.tablets().get(i);
            assertEquals(before.id(), after.id());
            assertEquals(before.range(), after.range());
            if (moved.contains(before.id())) {
                assertEquals(before.epoch().value() + 1, after.epoch().value());
            } else {
                assertEquals(before, after, "untouched tablet changed: " + before);
            }
        }
        return next;
    }

    private static RoutingTable randomSplit(RoutingTable t, Random rnd) {
        for (int attempt = 0; attempt < 8; attempt++) {
            Tablet target = t.tablets().get(rnd.nextInt(t.size()));
            TabletRange r = target.range();
            long len = r.end() - r.start();
            if (len == 0 || Long.compareUnsigned(len, 2) < 0) {
                continue; // FULL(길이 2^64)은 안 나오고, 길이 1은 못 쪼갠다
            }
            // 한가운데가 아니라 아무 데서나 쪼개 길이가 제각각이 되게 한다
            long at = r.start() + 1 + Long.remainderUnsigned(rnd.nextLong(), len - 1);
            return t.withSplit(target.id(), at);
        }
        return t;
    }

    private static RoutingTable randomMerge(RoutingTable t, Random rnd) {
        int i = rnd.nextInt(t.size());
        for (int k = 0; k + 1 < t.size(); k++) {
            int j = (i + k) % (t.size() - 1);
            Tablet a = t.tablets().get(j);
            Tablet b = t.tablets().get(j + 1);
            if (a.owner().equals(b.owner())) {
                return t.withMerge(a.id(), b.id());
            }
        }
        return t;
    }

    private static Map<NodeId, Long> weights(RoutingTable t, List<NodeId> nodes) {
        Map<NodeId, Long> w = new HashMap<>();
        for (NodeId n : nodes) {
            long sum = 0;
            for (Tablet tablet : t.ownedBy(n)) {
                sum += HashRangeScheme.weight(tablet.range());
            }
            w.put(n, sum);
        }
        return w;
    }

    private static long maxTablet(RoutingTable t) {
        long max = 0;
        for (Tablet tablet : t.tablets()) {
            max = Math.max(max, HashRangeScheme.weight(tablet.range()));
        }
        return max;
    }

    private static long spread(Map<NodeId, Long> w) {
        long min = Long.MAX_VALUE;
        long max = 0;
        for (long v : w.values()) {
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        return max - min;
    }
}
