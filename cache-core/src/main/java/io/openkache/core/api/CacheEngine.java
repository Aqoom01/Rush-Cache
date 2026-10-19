package io.openkache.core.api;

import io.openkache.core.model.NodeId;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerMessage;
import java.util.concurrent.CompletionStage;

/**
 * 한 노드의 캐시 엔진. 각 연구는 이 인터페이스를 구현한 엔진으로 자기 알고리즘을 실험한다.
 *
 * <p>현재 구현체는 없다. server는 엔진이 연결되기 전까지 GET·SET·DELETE에 {@link ResultStatus#NOT_IMPLEMENTED}로 응답한다.
 *
 * <h2>thread 규칙</h2>
 *
 * <ul>
 *   <li>모든 메서드는 Netty EventLoop에서 호출될 수 있으므로 block하지 않는다. 상태 변경은 owner worker의 mailbox로 넘긴다.
 *   <li>{@link #execute}가 반환한 stage는 owner worker나 다른 thread에서 완료될 수 있다. 호출자는 {@code join()}으로
 *       기다리지 않고 callback으로 응답을 보낸다.
 *   <li>예상 가능한 실패는 stage를 예외로 완료하지 않고 {@link Result}의 상태로 알린다.
 *   <li>{@link #snapshot()}은 어느 thread에서나 호출할 수 있고 불변 복사본을 반환한다.
 * </ul>
 */
public interface CacheEngine extends AutoCloseable {

    /** 클라이언트 연산을 처리한다. deadline이 지나면 {@link ResultStatus#TIMEOUT}으로 완료한다. */
    CompletionStage<Result> execute(Command command, RequestContext context);

    /** 다른 노드가 보낸 메시지를 전달받는다. */
    void onPeerMessage(NodeId from, PeerMessage message);

    /** 예약한 timer가 만료되었음을 전달받는다. */
    void onTimer(TimerEvent event);

    /** 현재 상태의 불변 복사본. */
    EngineSnapshot snapshot();

    /**
     * 새 요청을 거절하고 자원을 해제한다. 여러 번 호출해도 안전하다.
     *
     * <p>처리 중이던 요청의 stage는 {@link ResultStatus#UNAVAILABLE}로 완료한다.
     */
    @Override
    void close();
}
