package io.openkache.core.tablet.routing;

import io.openkache.core.model.NodeId;
import java.util.Optional;

/**
 * NodeId → NodeAddress 조회. Controller가 push하거나 MOVED redirect 힌트를 쓸 때 TCP 주소를 찾는다.
 *
 * <p>구현은 SimpleNodeDirectory(정적 Map) 또는 외부 디스커버리를 꽂을 수 있다.
 */
public interface NodeDirectory {

    /**
     * {@code id}의 주소를 돌려준다.
     *
     * @return 주소를 모르면 empty
     */
    Optional<NodeAddress> lookup(NodeId id);
}
