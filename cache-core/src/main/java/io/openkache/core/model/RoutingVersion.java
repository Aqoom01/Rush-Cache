package io.openkache.core.model;

/**
 * 라우팅 정보(Key Range → Tablet → Node 매핑)의 버전.
 *
 * <p>값은 0 이상이고 라우팅이 바뀔 때마다 증가한다. 클라이언트가 보낸 버전이 서버보다 낮으면 서버는 {@code MOVED}와 함께 최신
 * 버전을 알려 준다. wire에서는 u64로 전송하지만 Java에서는 0 이상만 허용한다.
 */
public record RoutingVersion(long value) implements Comparable<RoutingVersion> {

    public static final RoutingVersion ZERO = new RoutingVersion(0);

    public RoutingVersion {
        Ids.requireNonNegative("RoutingVersion", value);
    }

    public RoutingVersion next() {
        return new RoutingVersion(Math.addExact(value, 1));
    }

    public boolean isNewerThan(RoutingVersion other) {
        return value > other.value;
    }

    @Override
    public int compareTo(RoutingVersion other) {
        return Long.compare(value, other.value);
    }
}
