package io.openkache.core.spi;

import io.openkache.core.model.NodeId;
import java.util.Objects;

/**
 * 전송 단위. 보낸 노드, 받을 노드, 메시지를 묶는다.
 *
 * @param from 보낸 노드
 * @param to 받을 노드
 * @param message 전달할 메시지 (불변이어야 한다)
 */
public record PeerEnvelope(NodeId from, NodeId to, PeerMessage message) {

    public PeerEnvelope {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(message, "message");
    }
}
