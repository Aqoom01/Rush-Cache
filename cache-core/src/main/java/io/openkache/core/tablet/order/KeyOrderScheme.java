package io.openkache.core.tablet.order;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * 키 순서 범위 Tablet({@code key-order-tablet}): key를 해시하지 않고 <b>바이트 순서 그대로</b> 64비트 점으로 바꿔(앞 8바이트, big-endian)
 * 그 순서 공간을 범위로 나눈다. Bigtable·HBase의 tablet(region), TiKV의 region 계열. 비슷한 key가 같은 Tablet, 같은 노드에 있으므로
 * 범위 조회가 한 Tablet에서 끝나고, 순차 key(시간·자동 증가 번호)는 맨 끝 Tablet 하나에 몰린다.
 *
 * <p>표·조회·노드 증감·split은 {@link HashRangeScheme}과 완전히 같다. 다른 것은 {@link #keyHash}(순서 보존 변환)와, 데이터를 보고 경계를
 * 미리 정하는 {@link #initial(List, List)}(pre-split)뿐이다. 그래서 무작위 key에서는 {@code hash-range-tablet}과 결과가 같고, key에
 * 순서·공통 접두사가 있을 때만 달라진다.
 *
 * <p><b>변환의 한계.</b> 64비트에 임의 길이 key의 순서를 모두 담을 수는 없다. 앞 8바이트가 같은 key는 같은 점이 되어 더 쪼갤 수 없고(예:
 * {@code "user:000123"}과 {@code "user:000456"}은 둘 다 {@code "user:000"}), 그 뒤의 순서도 사라진다. Bigtable은 경계를 실제 key로 두므로
 * 이 제한이 없지만, 우리는 모든 variant가 같은 {@link RoutingTable}(64비트 구간)을 쓰기 위해 이렇게 둔다. 응용은 구분력이 앞 8바이트 안에
 * 오도록 key를 설계해야 한다(길이 8의 big-endian 번호처럼). 짧은 key는 0으로 채운다. 순서는 {@code core.balance.KeyRange.ORDER}(부호
 * 없는 바이트 사전순)와 앞 8바이트 안에서 일치한다.
 *
 * <p>정리는 {@code experiments/b1-tablet-distribution/references/bigtable-key-order-tablet.md}, 결정은 {@code design.md} §4.7.
 */
public final class KeyOrderScheme implements DistributionScheme {

    private final HashRangeScheme delegate;

    public KeyOrderScheme(int tabletsPerNode) {
        this.delegate = new HashRangeScheme(tabletsPerNode);
    }

    public KeyOrderScheme() {
        this(HashRangeScheme.DEFAULT_TABLETS_PER_NODE);
    }

    public int tabletsPerNode() {
        return delegate.tabletsPerNode();
    }

    @Override
    public String name() {
        return "key-order-tablet";
    }

    /** 앞 8바이트를 big-endian 부호 없는 정수로. 8바이트보다 짧으면 뒤를 0으로 채운다. */
    @Override
    public long keyHash(Bytes key) {
        return pointOf(key);
    }

    /** {@link #keyHash}와 같다. 순서 보존: 앞 8바이트의 바이트 사전순과 부호 없는 정수 순서가 같다. */
    public static long pointOf(Bytes key) {
        long p = 0;
        int n = Math.min(8, key.size());
        for (int i = 0; i < n; i++) {
            p = (p << 8) | (key.get(i) & 0xFFL);
        }
        return p << (8 * (8 - n));
    }

    /** 처음 표: 순서 공간을 노드당 T개로 균등 분할. 데이터 분포를 모를 때의 기본값이며 순차 key에서는 한 구간에 몰린다. */
    @Override
    public RoutingTable initial(List<NodeId> nodes) {
        return delegate.initial(nodes);
    }

    /**
     * 데이터를 보고 경계를 정한 처음 표(pre-split). {@code sampleKeys}를 순서 공간의 점으로 바꿔 정렬하고, 노드 수 × T개의 구간이 표본을
     * 같은 수씩 나눠 갖도록 분위수에 경계를 둔다. 같은 점이 많아 서로 다른 경계가 모자라면 구간 수가 그만큼 줄어든다(최소 노드 수).
     * 배치는 연속 블록이다. HBase의 pre-splitting, Bigtable의 기존 데이터 기준 tablet 경계와 같은 생각이다.
     *
     * @throws IllegalArgumentException 표본이 비었거나, 서로 다른 경계가 노드 수 − 1개보다 적을 때
     */
    public RoutingTable initial(List<NodeId> nodes, List<Bytes> sampleKeys) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (new HashSet<>(nodes).size() != nodes.size()) {
            throw new IllegalArgumentException("duplicate nodes: " + nodes);
        }
        if (sampleKeys.isEmpty()) {
            throw new IllegalArgumentException("sampleKeys must not be empty");
        }
        long[] points = new long[sampleKeys.size()];
        for (int i = 0; i < points.length; i++) {
            points[i] = pointOf(sampleKeys.get(i)) ^ Long.MIN_VALUE; // 부호 비트를 뒤집어 signed 정렬로 unsigned 순서
        }
        Arrays.sort(points);
        int n = nodes.size();
        int want = Math.multiplyExact(n, tabletsPerNode());
        List<Long> boundaries = new ArrayList<>(want);
        long previous = Long.MIN_VALUE; // 점 0 (첫 구간의 시작)
        for (int i = 1; i < want; i++) {
            long b = points[(int) ((long) i * points.length / want)];
            if (b != previous) {
                boundaries.add(b ^ Long.MIN_VALUE);
                previous = b;
            }
        }
        int count = boundaries.size() + 1;
        if (count < n) {
            throw new IllegalArgumentException("only " + count + " distinct ranges from samples for " + n + " nodes");
        }
        List<Tablet> tablets = new ArrayList<>(count);
        OwnershipEpoch first = OwnershipEpoch.ZERO.next();
        int base = count / n;
        int extra = count % n;
        int id = 0;
        for (int j = 0; j < n; j++) {
            int c = base + (j < extra ? 1 : 0);
            for (int k = 0; k < c; k++, id++) {
                long start = id == 0 ? 0 : boundaries.get(id - 1);
                long end = id == count - 1 ? 0 : boundaries.get(id);
                tablets.add(new Tablet(new TabletId(id), new TabletRange(start, end), nodes.get(j), first));
            }
        }
        return RoutingTable.of(RoutingVersion.ZERO.next(), tablets, count);
    }

    @Override
    public Plan onNodeAdded(RoutingTable table, NodeId node) {
        return delegate.onNodeAdded(table, node);
    }

    @Override
    public Plan onNodeRemoved(RoutingTable table, NodeId node) {
        return delegate.onNodeRemoved(table, node);
    }

    @Override
    public boolean supportsSplit() {
        return delegate.supportsSplit();
    }

    @Override
    public boolean supportsMove() {
        return delegate.supportsMove();
    }

    /** hash-range와 같은 32 B/Tablet. Bigtable처럼 경계를 실제 key로 두면 key 길이만큼 더 들지만 우리는 경계가 8바이트 점이다. */
    @Override
    public int metadataBytes(RoutingTable table) {
        return delegate.metadataBytes(table);
    }
}
