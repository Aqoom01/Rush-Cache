package io.openkache.core.tablet.routing;

import io.openkache.core.model.NodeId;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Map 기반 NodeDirectory. 클러스터 구성이 정적이거나 테스트용으로 쓴다.
 *
 * <p>불변이다. 빌더로 만든다.
 */
public final class SimpleNodeDirectory implements NodeDirectory {

    private final Map<NodeId, NodeAddress> addresses;

    private SimpleNodeDirectory(Map<NodeId, NodeAddress> addresses) {
        this.addresses = Map.copyOf(addresses);
    }

    @Override
    public Optional<NodeAddress> lookup(NodeId id) {
        return Optional.ofNullable(addresses.get(id));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<NodeId, NodeAddress> map = new HashMap<>();

        public Builder add(NodeId id, String host, int port) {
            Objects.requireNonNull(id, "id");
            map.put(id, new NodeAddress(host, port));
            return this;
        }

        public Builder add(NodeId id, NodeAddress address) {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(address, "address");
            map.put(id, address);
            return this;
        }

        public SimpleNodeDirectory build() {
            return new SimpleNodeDirectory(map);
        }
    }
}
