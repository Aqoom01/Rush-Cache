package io.openkache.core.tablet;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 라우팅 표: 해시 공간 전체를 빈틈·겹침 없이 덮는 Tablet 목록과 표 버전. 불변이다.
 *
 * <p>모든 변경 연산(이동, split, merge)은 새 표를 돌려주고 {@link RoutingVersion}을 1 올린다. 불변이므로 어느 thread에서나
 * 읽을 수 있고, 새 표로 바꿔 끼우는 것은 소유한 worker나 Controller가 한다.
 *
 * <p>조회는 Tablet 시작점 목록에서 이진 탐색한다. 모든 variant가 이 표를 그대로 쓰므로 조회와 변경 연산은 variant와 무관하다.
 * variant는 표를 처음 만드는 규칙과 노드 증감 시 이동 계획({@link DistributionScheme})만 정한다.
 *
 * <p>생성할 때마다 불변식(공간 전체를 덮음, 식별자 중복 없음)을 검사한다. 어기면 프로그래밍 오류이므로 예외를 던진다.
 */
public final class RoutingTable {

    private final RoutingVersion version;
    private final List<Tablet> tablets; // range.start 오름차순(부호 없음)
    private final long[] starts; // 이진 탐색용. 부호 비트를 뒤집어 signed 비교로 unsigned 순서를 얻는다
    private final Map<TabletId, Integer> index;
    private final long nextTabletId;

    private RoutingTable(RoutingVersion version, List<Tablet> tablets, long nextTabletId) {
        this.version = Objects.requireNonNull(version, "version");
        this.tablets = List.copyOf(tablets);
        this.nextTabletId = nextTabletId;
        this.starts = new long[this.tablets.size()];
        Map<TabletId, Integer> idx = new HashMap<>(this.tablets.size() * 2);
        for (int i = 0; i < starts.length; i++) {
            Tablet t = this.tablets.get(i);
            starts[i] = t.range().start() ^ Long.MIN_VALUE;
            if (idx.put(t.id(), i) != null) {
                throw new IllegalArgumentException("duplicate tablet id: " + t.id());
            }
        }
        this.index = Collections.unmodifiableMap(idx);
        validate();
    }

    /**
     * 표를 만든다. {@code tablets}는 시작점 오름차순이어야 하고 공간 전체를 덮어야 한다.
     *
     * @param nextTabletId 다음 split 때 자식에게 줄 식별자. 기존 식별자보다 커야 한다
     */
    public static RoutingTable of(RoutingVersion version, List<Tablet> tablets, long nextTabletId) {
        for (Tablet t : tablets) {
            if (t.id().value() >= nextTabletId) {
                throw new IllegalArgumentException("nextTabletId " + nextTabletId + " is not above " + t.id());
            }
        }
        return new RoutingTable(version, tablets, nextTabletId);
    }

    public RoutingVersion version() {
        return version;
    }

    /** 시작점 오름차순의 Tablet 목록. */
    public List<Tablet> tablets() {
        return tablets;
    }

    public int size() {
        return tablets.size();
    }

    /** 다음 split 때 자식이 받을 식별자. */
    public long nextTabletId() {
        return nextTabletId;
    }

    /** {@code hash}를 맡는 Tablet. 표가 공간 전체를 덮으므로 항상 찾는다. */
    public Tablet lookup(long hash) {
        long x = hash ^ Long.MIN_VALUE;
        int lo = 0;
        int hi = starts.length - 1;
        while (lo < hi) { // x 이하인 마지막 시작점
            int mid = (lo + hi + 1) >>> 1;
            if (starts[mid] <= x) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return tablets.get(lo);
    }

    /** 식별자로 찾는다. 없으면 예외. */
    public Tablet get(TabletId id) {
        return tablets.get(indexOf(id));
    }

    public boolean contains(TabletId id) {
        return index.containsKey(id);
    }

    /** 노드 {@code node}가 맡은 Tablet들. 시작점 오름차순. */
    public List<Tablet> ownedBy(NodeId node) {
        List<Tablet> l = new ArrayList<>();
        for (Tablet t : tablets) {
            if (t.owner().equals(node)) {
                l.add(t);
            }
        }
        return Collections.unmodifiableList(l);
    }

    /** 표에 나타나는 노드들. 처음 나타나는 순서. */
    public Set<NodeId> nodes() {
        Set<NodeId> s = new LinkedHashSet<>();
        for (Tablet t : tablets) {
            s.add(t.owner());
        }
        return Collections.unmodifiableSet(s);
    }

    /** Tablet {@code id}의 주인을 바꾼 새 표. 그 Tablet의 epoch와 표 버전이 함께 오른다. */
    public RoutingTable withOwner(TabletId id, NodeId newOwner) {
        int i = indexOf(id);
        Tablet t = tablets.get(i);
        if (t.owner().equals(newOwner)) {
            throw new IllegalArgumentException(id + " is already owned by " + newOwner);
        }
        List<Tablet> l = new ArrayList<>(tablets);
        l.set(i, t.movedTo(newOwner));
        return new RoutingTable(version.next(), l, nextTabletId);
    }

    /**
     * 이동 계획 한 건을 반영한 새 표. 현재 주인이 {@code change.from()}과 다르면 낡은 계획이므로
     * {@link IllegalStateException}을 던진다.
     */
    public RoutingTable apply(Change change) {
        Tablet t = get(change.unit());
        if (!t.owner().equals(change.from())) {
            throw new IllegalStateException(
                    "stale change: " + change.unit() + " is owned by " + t.owner() + ", not " + change.from());
        }
        return withOwner(change.unit(), change.to());
    }

    /**
     * Tablet {@code id}를 해시값 {@code at}에서 둘로 나눈 새 표. 앞쪽은 원래 식별자, 뒤쪽은 {@link #nextTabletId()}를 받는다.
     * 두 조각 모두 원래 주인과 epoch를 그대로 가진다(주인이 바뀌지 않았으므로).
     */
    public RoutingTable withSplit(TabletId id, long at) {
        int i = indexOf(id);
        Tablet t = tablets.get(i);
        TabletRange.Split parts = t.range().splitAt(at);
        List<Tablet> l = new ArrayList<>(tablets);
        l.set(i, new Tablet(t.id(), parts.left(), t.owner(), t.epoch()));
        l.add(i + 1, new Tablet(new TabletId(nextTabletId), parts.right(), t.owner(), t.epoch()));
        return new RoutingTable(version.next(), l, Math.addExact(nextTabletId, 1));
    }

    /**
     * 이웃한 두 Tablet을 합친 새 표. {@code left}가 시작점이 작은 쪽이어야 하고(순서를 바꾸면 인접하지 않은 것으로 거절),
     * 같은 노드가 맡고 있어야 한다(다른 노드면 먼저 옮긴다). 합친 Tablet은 앞쪽 식별자와
     * 둘 중 큰 epoch를 가진다.
     */
    public RoutingTable withMerge(TabletId left, TabletId right) {
        int i = indexOf(left);
        if (i + 1 >= tablets.size() || !tablets.get(i + 1).id().equals(right)) {
            throw new IllegalArgumentException("tablets are not adjacent: " + left + ", " + right);
        }
        Tablet a = tablets.get(i);
        Tablet b = tablets.get(i + 1);
        if (!a.owner().equals(b.owner())) {
            throw new IllegalArgumentException("tablets have different owners: " + a.owner() + ", " + b.owner());
        }
        OwnershipEpoch e = a.epoch().isNewerThan(b.epoch()) ? a.epoch() : b.epoch();
        List<Tablet> l = new ArrayList<>(tablets);
        l.set(i, new Tablet(a.id(), a.range().mergeWith(b.range()), a.owner(), e));
        l.remove(i + 1);
        return new RoutingTable(version.next(), l, nextTabletId);
    }

    private int indexOf(TabletId id) {
        Integer i = index.get(id);
        if (i == null) {
            throw new IllegalArgumentException("unknown tablet: " + id);
        }
        return i;
    }

    /** 공간 전체를 빈틈·겹침 없이 덮는지 검사한다. */
    private void validate() {
        if (tablets.isEmpty()) {
            throw new IllegalArgumentException("routing table is empty");
        }
        if (tablets.get(0).range().start() != 0) {
            throw new IllegalArgumentException("first tablet must start at 0: " + tablets.get(0));
        }
        for (int i = 0; i + 1 < tablets.size(); i++) {
            if (tablets.get(i).range().end() == 0) {
                throw new IllegalArgumentException("only the last tablet may end at 2^64: " + tablets.get(i));
            }
            if (tablets.get(i).range().end() != tablets.get(i + 1).range().start()) {
                throw new IllegalArgumentException(
                        "gap or overlap between " + tablets.get(i).range() + " and " + tablets.get(i + 1).range());
            }
        }
        if (tablets.get(tablets.size() - 1).range().end() != 0) {
            throw new IllegalArgumentException("last tablet must end at 2^64: " + tablets.get(tablets.size() - 1));
        }
    }

    @Override
    public String toString() {
        return "RoutingTable{version=" + version.value() + ", tablets=" + tablets.size() + ", nodes=" + nodes().size()
                + "}";
    }
}
