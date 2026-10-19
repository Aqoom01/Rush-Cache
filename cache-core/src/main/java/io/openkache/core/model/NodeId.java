package io.openkache.core.model;

/**
 * 클러스터 안에서 노드를 식별한다.
 *
 * <p>값은 0 이상이며 서버 설정의 {@code node.id}와 같다. 한 클러스터 안에서 노드마다 달라야 한다.
 */
public record NodeId(long value) {

    public NodeId {
        Ids.requireNonNegative("NodeId", value);
    }
}
