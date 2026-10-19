package io.openkache.core.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * split·merge·이동을 무작위로 섞어 적용하며 매 단계 불변식을 검사한다. seed 고정이라 재현된다.
 *
 * <p>검사: 공간을 빈틈없이 덮음(생성자가 보장), 이진 탐색 결과가 선형 탐색과 같음, 버전이 매번 정확히 1 증가, 주인이 바뀐 Tablet만
 * epoch가 오름, 식별자가 다시 쓰이지 않음, 모든 Tablet의 epoch가 해당 Tablet(또는 split 조상)의 주인 변경 횟수와 일치.
 */
class RoutingTableRandomOpsTest {

    private static final int STEPS = 3000;
    private static final int NODES = 5;

    @Test
    void randomOperationsKeepInvariants() {
        for (long seed = 1; seed <= 5; seed++) {
            run(new Random(seed));
        }
    }

    private static void run(Random rnd) {
        List<NodeId> nodes = new ArrayList<>();
        for (int i = 1; i <= NODES; i++) {
            nodes.add(new NodeId(i));
        }
        List<TabletRange> ranges = TabletRange.uniform(8);
        List<Tablet> initial = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            initial.add(new Tablet(new TabletId(i), ranges.get(i), nodes.get(i % NODES), OwnershipEpoch.ZERO.next()));
        }
        RoutingTable t = RoutingTable.of(RoutingVersion.ZERO.next(), initial, 8);
        Map<TabletId, Long> ownerChanges = new HashMap<>(); // 식별자별 주인 변경 횟수 (split 자식은 부모 값을 물려받음)
        for (Tablet x : initial) {
            ownerChanges.put(x.id(), 0L);
        }
        long maxIdSeen = 7;
        int splits = 0;
        int merges = 0;
        int moves = 0;

        for (int step = 0; step < STEPS; step++) {
            long versionBefore = t.version().value();
            int size = t.size();
            Tablet target = t.tablets().get(rnd.nextInt(size));
            int op = rnd.nextInt(3);
            if (op == 0 && size < 400) { // split
                TabletRange r = target.range();
                long span = (r.end() == 0 ? 0 : r.end()) - r.start(); // 부호 없는 길이 (end==0이면 2^64-start 를 wrap 으로 표현)
                if (Long.compareUnsigned(span, 2) < 0 && !(r.end() == 0 && r.start() == 0)) {
                    continue; // 길이 1은 못 나눔
                }
                long offset = span == 0 ? rnd.nextLong() | 1 : Long.remainderUnsigned(rnd.nextLong(), span);
                if (offset == 0) {
                    offset = 1;
                }
                long at = r.start() + offset;
                RoutingTable next = t.withSplit(target.id(), at);
                assertEquals(size + 1, next.size());
                TabletId child = new TabletId(t.nextTabletId());
                assertTrue(child.value() > maxIdSeen, "ids must never be reused");
                maxIdSeen = child.value();
                ownerChanges.put(child, ownerChanges.get(target.id()));
                assertEquals(target.epoch(), next.get(child).epoch());
                assertEquals(target.owner(), next.get(child).owner());
                t = next;
                splits++;
            } else if (op == 1) { // merge with right neighbour if same owner
                int i = t.tablets().indexOf(target);
                if (i + 1 < size && t.tablets().get(i + 1).owner().equals(target.owner())) {
                    Tablet right = t.tablets().get(i + 1);
                    RoutingTable next = t.withMerge(target.id(), right.id());
                    assertEquals(size - 1, next.size());
                    long expected = Math.max(target.epoch().value(), right.epoch().value());
                    assertEquals(expected, next.get(target.id()).epoch().value());
                    ownerChanges.put(target.id(), Math.max(ownerChanges.get(target.id()), ownerChanges.get(right.id())));
                    ownerChanges.remove(right.id());
                    t = next;
                    merges++;
                } else {
                    continue;
                }
            } else { // move to a different node via apply(Change)
                NodeId to = nodes.get(rnd.nextInt(NODES));
                if (to.equals(target.owner())) {
                    continue;
                }
                RoutingTable next = t.apply(new Change(target.id(), target.owner(), to));
                assertEquals(target.epoch().value() + 1, next.get(target.id()).epoch().value());
                assertEquals(to, next.get(target.id()).owner());
                ownerChanges.merge(target.id(), 1L, Long::sum);
                for (Tablet other : t.tablets()) {
                    if (!other.id().equals(target.id())) {
                        assertEquals(other, next.get(other.id()), "untouched tablet changed");
                    }
                }
                t = next;
                moves++;
            }
            assertEquals(versionBefore + 1, t.version().value(), "version must increase by exactly 1");
            checkLookupAgainstLinearScan(t, rnd);
            for (Tablet x : t.tablets()) {
                assertEquals(1 + ownerChanges.get(x.id()), x.epoch().value(), "epoch must equal 1 + owner changes for " + x.id());
            }
        }
        // 세 연산이 실제로 충분히 섞여 실행됐는지 (continue 로만 빠지면 검사가 공허해진다)
        assertTrue(splits > 300, "splits=" + splits);
        assertTrue(merges > 100, "merges=" + merges);
        assertTrue(moves > 300, "moves=" + moves);
    }

    private static void checkLookupAgainstLinearScan(RoutingTable t, Random rnd) {
        List<Tablet> all = t.tablets();
        for (int k = 0; k < 20; k++) {
            long h = rnd.nextLong();
            Tablet expected = null;
            for (Tablet x : all) {
                if (x.range().contains(h)) {
                    expected = x;
                    break;
                }
            }
            assertSame(expected, t.lookup(h));
        }
        // 경계점도 확인: 각 Tablet의 start와 start-1
        for (int i = 0; i < all.size(); i++) {
            long s = all.get(i).range().start();
            assertSame(all.get(i), t.lookup(s));
            if (i > 0) {
                assertSame(all.get(i - 1), t.lookup(s - 1));
            }
        }
    }
}
