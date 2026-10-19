package io.openkache.core.api;

import io.openkache.core.model.NodeId;
import java.util.Objects;

/**
 * 요청을 다시 보낼 노드.
 *
 * @param target 다시 요청할 노드
 */
public record Redirect(NodeId target) {

    public Redirect {
        Objects.requireNonNull(target, "target");
    }
}
