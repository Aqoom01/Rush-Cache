package io.openkache.core.spi;

import java.util.concurrent.CompletionStage;

/**
 * 다른 노드로 메시지를 보내는 계약. 실제 구현(Netty, 시뮬레이션)은 core 밖에 둔다.
 *
 * <p>{@link #send(PeerEnvelope)}가 반환한 stage의 완료는 <b>로컬 전송 계층이 메시지를 받아들였거나 socket에 flush했다</b>는
 * 뜻이다. 상대 노드가 메시지를 받았거나 처리·commit했다는 뜻이 아니다. 복제 확인 같은 원격 응답은 각 연구가 별도
 * {@link PeerMessage}로 정의한다.
 *
 * <p>구현은 호출 thread를 block하지 않아야 하고 여러 thread에서 동시에 호출해도 안전해야 한다. 연결 실패나 전송 큐 초과는
 * stage를 예외로 완료해 알린다.
 */
public interface Transport {

    CompletionStage<Void> send(PeerEnvelope envelope);
}
