package io.openkache.core.model;

/**
 * 요청을 보낸 클라이언트를 식별한다.
 *
 * <p>값은 0 이상이다. {@link OperationId}와 함께 재시도된 논리 연산을 식별하는 데 쓴다.
 */
public record ClientId(long value) {

    public ClientId {
        Ids.requireNonNegative("ClientId", value);
    }
}
