package io.openkache.core.tablet.experiment;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 분배 지표. 전부 {@link RoutingTable}의 공개 연산만 쓰므로 variant와 무관하다.
 *
 * <p>노드는 {@code NodeId} 값 {@code 1..n}을 쓴다고 가정하고 배열 인덱스로 센다.
 */
final class Metrics {

    private Metrics() {}

    /** 노드별 Tablet 수의 max / mean. 표만 보면 나오는 결정적 값. */
    static double unitMaxMean(RoutingTable table, List<NodeId> nodes) {
        long[] perNode = new long[maxId(nodes) + 1];
        for (Tablet t : table.tablets()) {
            perNode[(int) t.owner().value()]++;
        }
        return maxMean(perNode, nodes);
    }

    /** 노드별 key 수의 max / mean. */
    static double keyMaxMean(RoutingTable table, long[] hashes, List<NodeId> nodes) {
        long[] perNode = new long[maxId(nodes) + 1];
        for (long h : hashes) {
            perNode[(int) table.lookup(h).owner().value()]++;
        }
        return maxMean(perNode, nodes);
    }

    /** 노드별 요청 수의 max / mean. 요청은 key 인덱스별 횟수. */
    static double requestMaxMean(RoutingTable table, long[] hashes, int[] requests, List<NodeId> nodes) {
        long[] perNode = new long[maxId(nodes) + 1];
        for (int i = 0; i < hashes.length; i++) {
            if (requests[i] != 0) {
                perNode[(int) table.lookup(hashes[i]).owner().value()] += requests[i];
            }
        }
        return maxMean(perNode, nodes);
    }

    /** 두 표 사이에 주인이 바뀐 key의 비율. */
    static double movedRatio(RoutingTable before, RoutingTable after, long[] hashes) {
        long moved = 0;
        for (long h : hashes) {
            if (!before.lookup(h).owner().equals(after.lookup(h).owner())) {
                moved++;
            }
        }
        return (double) moved / hashes.length;
    }

    /** Tablet(단위)별 요청 수. 인덱스는 {@code table.tablets()} 순서. */
    static long[] unitLoads(RoutingTable table, long[] hashes, int[] requests) {
        Map<TabletId, Integer> index = new HashMap<>();
        List<Tablet> tablets = table.tablets();
        for (int i = 0; i < tablets.size(); i++) {
            index.put(tablets.get(i).id(), i);
        }
        long[] loads = new long[tablets.size()];
        for (int i = 0; i < hashes.length; i++) {
            if (requests[i] != 0) {
                loads[index.get(table.lookup(hashes[i]).id())] += requests[i];
            }
        }
        return loads;
    }

    /**
     * 단위를 통째로 옮기는 것만으로 도달할 수 있는 요청 균형의 한계(max / mean). 가장 무거운 단위부터 가장 한가한 노드에 넣는
     * LPT 그리디로 계산한다. 표는 바꾸지 않는다. 어떤 재배치 정책도 이 값보다 (거의) 좋아질 수 없다.
     */
    static double greedyBound(long[] unitLoads, int nodeCount) {
        Integer[] order = new Integer[unitLoads.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> Long.compare(unitLoads[b], unitLoads[a]));
        long[] nodeLoad = new long[nodeCount];
        for (int unit : order) {
            int best = 0;
            for (int n = 1; n < nodeCount; n++) {
                if (nodeLoad[n] < nodeLoad[best]) {
                    best = n;
                }
            }
            nodeLoad[best] += unitLoads[unit];
        }
        long total = 0;
        long max = 0;
        for (long l : nodeLoad) {
            total += l;
            max = Math.max(max, l);
        }
        return total == 0 ? 1.0 : max / ((double) total / nodeCount);
    }

    /** 가장 무거운 단위 하나가 전체 요청에서 차지하는 비율. 이 값 × 노드 수가 그리디 한계의 하한이다. */
    static double hottestUnitShare(long[] unitLoads) {
        long total = 0;
        long max = 0;
        for (long l : unitLoads) {
            total += l;
            max = Math.max(max, l);
        }
        return total == 0 ? 0 : (double) max / total;
    }

    /** 가장 많이 요청된 key 하나의 비율. 어떤 분배로도 이 key는 쪼갤 수 없다. */
    static double hottestKeyShare(int[] requests) {
        long total = 0;
        long max = 0;
        for (int r : requests) {
            total += r;
            max = Math.max(max, r);
        }
        return total == 0 ? 0 : (double) max / total;
    }

    /** {@code lookup} 1회의 평균 나노초. 워밍업 후 측정하고, 결과를 버리지 않도록 합을 반환값에 섞는다. */
    static double lookupNanos(RoutingTable table, long[] hashes) {
        long sink = 0;
        for (long h : hashes) {
            sink += table.lookup(h).id().value();
        }
        long start = System.nanoTime();
        for (long h : hashes) {
            sink += table.lookup(h).id().value();
        }
        long elapsed = System.nanoTime() - start;
        double perLookup = (double) elapsed / hashes.length;
        return sink == Long.MIN_VALUE ? -1 : perLookup; // sink 사용 (JIT가 루프를 지우지 못하게)
    }

    /**
     * key가 두 노드 이상에 흩어진 묶음의 비율. key가 하나뿐인 묶음은 흩어질 수 없으므로 분모에서 뺀다. 해시 태그 variant는 0이어야 하고,
     * key 전체를 해시하는 variant는 묶음 크기가 노드 수보다 크면 거의 1이다.
     */
    static double groupsSplitRatio(RoutingTable table, long[] hashes, int[] groupOf, int groups) {
        long[] counts = groupNodeCounts(table, hashes, groupOf, groups);
        int stride = counts.length / groups;
        long eligible = 0;
        long split = 0;
        for (int g = 0; g < groups; g++) {
            long size = 0;
            int nodesHit = 0;
            for (int n = 0; n < stride; n++) {
                long c = counts[g * stride + n];
                size += c;
                if (c != 0) {
                    nodesHit++;
                }
            }
            if (size >= 2) {
                eligible++;
                if (nodesHit > 1) {
                    split++;
                }
            }
        }
        return eligible == 0 ? 0 : (double) split / eligible;
    }

    /**
     * 같은 묶음의 서로 다른 두 key가 같은 노드에 있을 확률(묶음 크기로 가중한 순서쌍 기준). multi-key 요청이 한 노드에서 끝날 확률이다.
     * 해시 태그 variant는 1.0, key 전체를 해시하면 노드 수 n에 대해 약 1/n.
     */
    static double sameNodePairRatio(RoutingTable table, long[] hashes, int[] groupOf, int groups) {
        long[] counts = groupNodeCounts(table, hashes, groupOf, groups);
        int stride = counts.length / groups;
        double pairs = 0;
        double samePairs = 0;
        for (int g = 0; g < groups; g++) {
            long size = 0;
            for (int n = 0; n < stride; n++) {
                long c = counts[g * stride + n];
                size += c;
                samePairs += (double) c * (c - 1);
            }
            pairs += (double) size * (size - 1);
        }
        return pairs == 0 ? 0 : samePairs / pairs;
    }

    /** 가장 큰 묶음의 key 비율. 해시 태그 variant에서는 이 값 × 노드 수가 key 균형의 하한이다. */
    static double largestGroupShare(int[] groupOf, int groups) {
        long[] size = new long[groups];
        for (int g : groupOf) {
            size[g]++;
        }
        long max = 0;
        for (long s : size) {
            max = Math.max(max, s);
        }
        return groupOf.length == 0 ? 0 : (double) max / groupOf.length;
    }

    /** 묶음 × 노드별 key 수. 인덱스 {@code g * (maxNodeId + 1) + nodeId}. */
    private static long[] groupNodeCounts(RoutingTable table, long[] hashes, int[] groupOf, int groups) {
        int stride = maxId(new ArrayList<>(table.nodes())) + 1;
        long[] counts = new long[Math.multiplyExact(groups, stride)];
        for (int i = 0; i < hashes.length; i++) {
            counts[groupOf[i] * stride + (int) table.lookup(hashes[i]).owner().value()]++;
        }
        return counts;
    }

    private static double maxMean(long[] perNode, List<NodeId> nodes) {
        long total = 0;
        long max = 0;
        for (NodeId n : nodes) {
            long v = perNode[(int) n.value()];
            total += v;
            max = Math.max(max, v);
        }
        return total == 0 ? 1.0 : max / ((double) total / nodes.size());
    }

    private static int maxId(List<NodeId> nodes) {
        int max = 0;
        for (NodeId n : nodes) {
            max = Math.max(max, (int) n.value());
        }
        return max;
    }
}
