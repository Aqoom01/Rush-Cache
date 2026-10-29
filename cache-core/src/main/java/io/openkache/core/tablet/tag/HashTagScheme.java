package io.openkache.core.tablet.tag;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.Plan;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.range.HashRangeScheme;
import java.util.List;

/**
 * 해시 태그 Tablet({@code hash-tag-tablet}): key 전체가 아니라 key 안의 hash tag(중괄호로 감싼 부분)만 해시한다. 같은 tag의
 * key는 해시값이 같으므로 언제나 같은 Tablet, 같은 노드에 있다. Redis Cluster의 hash tag, DynamoDB·Cassandra의 partition key 계열.
 *
 * <p>표·조회·노드 증감·split은 {@link HashRangeScheme}과 완전히 같다. 다른 것은 {@link #keyHash}뿐이다. 그래서 tag가 없는 key만 있으면
 * {@code hash-range-tablet}과 결과가 같고, tag가 있으면 분배 단위가 key에서 "tag 묶음"으로 커진다. 묶음은 split으로도 나눌 수 없으므로
 * 묶음 하나가 크거나 뜨거우면 그 노드의 불균형은 어떤 재배치로도 못 푼다. 그 대가로 같은 묶음의 여러 key를 한 요청(multi-key)으로
 * 한 노드에서 처리할 수 있다.
 *
 * <p>tag 규칙은 Redis Cluster 명세를 따른다: key에서 첫 여는 중괄호와 그 뒤 첫 닫는 중괄호 사이가 비어 있지 않으면 그 부분만 해시하고,
 * 그렇지 않으면(여는 괄호가 없거나, 닫는 괄호가 없거나, 사이가 비었으면) key 전체를 해시한다.
 *
 * <p>정리는 {@code experiments/b1-tablet-distribution/references/hash-tag-partition-key.md}, 결정은 {@code design.md} §4.5.
 */
public final class HashTagScheme implements DistributionScheme {

    private static final byte TAG_OPEN = '{';
    private static final byte TAG_CLOSE = '}';

    private final HashRangeScheme delegate;

    public HashTagScheme(int tabletsPerNode) {
        this.delegate = new HashRangeScheme(tabletsPerNode);
    }

    public HashTagScheme() {
        this(HashRangeScheme.DEFAULT_TABLETS_PER_NODE);
    }

    public int tabletsPerNode() {
        return delegate.tabletsPerNode();
    }

    @Override
    public String name() {
        return "hash-tag-tablet";
    }

    /** tag가 있으면 tag만, 없으면 key 전체의 {@link KeyHash}. */
    @Override
    public long keyHash(Bytes key) {
        return KeyHash.of(tagOf(key));
    }

    /**
     * 해시에 쓰는 부분. 첫 여는 중괄호와 그 뒤 첫 닫는 중괄호 사이가 비어 있지 않으면 그 부분, 아니면 {@code key} 그대로.
     *
     * <pre>
     * {user1}:profile  → user1
     * {user1}:{x}      → user1     (첫 쌍만)
     * {{user1}}        → {user1    (첫 여는 괄호부터 첫 닫는 괄호까지)
     * {}user1          → {}user1   (빈 tag는 무시)
     * user1            → user1
     * </pre>
     */
    public static Bytes tagOf(Bytes key) {
        int n = key.size();
        for (int i = 0; i < n; i++) {
            if (key.get(i) != TAG_OPEN) {
                continue;
            }
            for (int j = i + 1; j < n; j++) {
                if (key.get(j) == TAG_CLOSE) {
                    if (j == i + 1) {
                        return key;
                    }
                    byte[] tag = new byte[j - i - 1];
                    for (int k = 0; k < tag.length; k++) {
                        tag[k] = key.get(i + 1 + k);
                    }
                    return Bytes.copyOf(tag);
                }
            }
            return key;
        }
        return key;
    }

    @Override
    public RoutingTable initial(List<NodeId> nodes) {
        return delegate.initial(nodes);
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

    @Override
    public int metadataBytes(RoutingTable table) {
        return delegate.metadataBytes(table);
    }
}
