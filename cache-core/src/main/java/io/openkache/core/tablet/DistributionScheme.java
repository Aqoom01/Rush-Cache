package io.openkache.core.tablet;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import java.util.List;

/**
 * 분배 방식(variant). 표를 처음 만드는 규칙과 노드 증감 시 이동 계획을 정한다.
 *
 * <p>표의 조회와 변경 연산은 {@link RoutingTable}이 variant와 무관하게 제공하므로, 이 인터페이스는 variant마다 다른 것만 담는다.
 * {@code balance}와 {@code migration}은 {@link RoutingTable}, {@link Tablet}, {@link Change}, {@link Plan}만 다루고 어떤
 * variant가 꽂혀 있는지 모른다. 이 인터페이스는 Controller와 실험 코드가 쓴다.
 *
 * <p>구현은 상태가 없어야 하며 결정적이어야 한다. 같은 입력에는 같은 결과를 내고, 난수와 실제 시각을 쓰지 않는다. 입력 표를
 * 변경하지 않는다.
 */
public interface DistributionScheme {

    /** 실험 결과의 {@code variant} 값으로 쓰는 이름. 예: {@code "fixed-slot"}. */
    String name();

    /**
     * key를 해시 공간의 점으로 바꾼다. {@link RoutingTable#lookup}의 입력이다. 기본은 key 전체의 {@link KeyHash}이고, 해시 태그 방식은
     * key의 일부(hash tag)만 해시해 같은 태그의 key를 한 Tablet에 모은다. 표와 무관하게 결정적이어야 하며, 클라이언트와 서버는 같은
     * variant의 이 메서드를 써야 한다.
     */
    default long keyHash(Bytes key) {
        return KeyHash.of(key);
    }

    /**
     * 처음 표. 버전은 1이고 모든 Tablet의 epoch는 1이다(epoch 0은 wire에서 "모름"을 뜻하므로 실제 값으로 쓰지 않는다).
     *
     * @param nodes 처음 참여하는 노드. 비어 있으면 {@link IllegalArgumentException}
     */
    RoutingTable initial(List<NodeId> nodes);

    /**
     * {@code node}가 새로 들어올 때의 계획. 모든 이동의 {@code to}는 {@code node}다. 입력 표는 바꾸지 않는다.
     *
     * <p>계획의 이동을 모두 적용하면 노드 간 Tablet 수 차이가 variant의 균형 규칙 안에 들어야 한다. {@code node}가 이미 표에
     * 있으면 {@link IllegalArgumentException}.
     */
    Plan onNodeAdded(RoutingTable table, NodeId node);

    /**
     * {@code node}가 떠날 때의 계획. 모든 이동의 {@code from}은 {@code node}이고, 적용 후 {@code node}는 Tablet을 하나도 갖지
     * 않는다. 입력 표는 바꾸지 않는다.
     *
     * <p>{@code node}가 표에 없으면 {@link IllegalArgumentException}, 남는 노드가 없으면 {@link IllegalStateException}.
     */
    Plan onNodeRemoved(RoutingTable table, NodeId node);

    /** {@link RoutingTable#withSplit}/{@link RoutingTable#withMerge}가 이 variant에서 의미가 있는가. false면 split 실험 대상이 아니다. */
    boolean supportsSplit();

    /** Tablet을 임의의 노드로 옮기는 재배치가 이 variant에서 의미가 있는가. false면 노드 증감 외의 이동은 하지 않는다. */
    boolean supportsMove();

    /** 표를 이 variant의 형식으로 직렬화했을 때의 크기(바이트) 추정. 분배 방식 비교 지표. */
    int metadataBytes(RoutingTable table);
}
