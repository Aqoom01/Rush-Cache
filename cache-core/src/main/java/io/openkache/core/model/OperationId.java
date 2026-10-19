package io.openkache.core.model;

/**
 * 클라이언트가 발급한 논리 연산 식별자.
 *
 * <p>같은 {@link ClientId} 안에서 유일하며, 클라이언트가 같은 연산을 재시도할 때는 같은 값을 다시 보낸다. 중복 실행 판별에
 * 쓴다.
 *
 * <p>연결 안에서 요청과 응답을 짝짓는 wire의 {@code requestId}와는 다르다. {@code requestId}는 재시도마다 새로 발급될 수
 * 있고 core로 전달되지 않는다.
 */
public record OperationId(long value) {

    public OperationId {
        Ids.requireNonNegative("OperationId", value);
    }
}
