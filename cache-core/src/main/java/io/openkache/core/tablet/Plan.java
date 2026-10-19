package io.openkache.core.tablet;

import java.util.List;
import java.util.Objects;

/**
 * 노드 증감에 대응하는 계획: 메타데이터만 바뀐 표와 그 위에서 실행할 이동 목록.
 *
 * <p>{@code prepared}는 입력 표에서 split·merge처럼 주인이 바뀌지 않는 변경만 적용한 표다. 데이터 이동 없이 Controller가 바로
 * 채택할 수 있다. {@code moves}는 {@code prepared}를 기준으로 한 이동 계획이며, {@code migration}이 데이터를 옮긴 뒤
 * {@link RoutingTable#apply(Change)}로 하나씩 반영한다.
 *
 * <p>variant가 Tablet 경계를 바꾸지 않으면 {@code prepared}는 입력 표 그대로다. 링(vnode) 방식은 새 토큰 위치에서 구간을
 * 나눠야 하므로 {@code prepared}가 입력과 다르다.
 *
 * @param prepared 이동 전에 채택할 표. 입력 표와 같을 수 있다
 * @param moves {@code prepared} 위에서 실행할 이동. 비어 있을 수 있다
 */
public record Plan(RoutingTable prepared, List<Change> moves) {

    public Plan {
        Objects.requireNonNull(prepared, "prepared");
        moves = List.copyOf(moves);
    }

    /** 경계 변경 없이 이동만 하는 계획. */
    public static Plan movesOnly(RoutingTable table, List<Change> moves) {
        return new Plan(table, moves);
    }

    /** {@code moves}를 모두 적용한 표. 테스트와 시뮬레이션용. 실제 운영에서는 이동이 끝난 것부터 하나씩 적용한다. */
    public RoutingTable applyAll() {
        RoutingTable t = prepared;
        for (Change c : moves) {
            t = t.apply(c);
        }
        return t;
    }
}
