package io.openkache.server.netty;

import java.io.Serial;

/** frame 헤더가 프로토콜 v1을 따르지 않는다. 이 예외가 나면 연결을 닫는다. */
public final class ProtocolException extends Exception {

    @Serial private static final long serialVersionUID = 1L;

    private final long requestId;

    /**
     * @param requestId 헤더에서 읽은 requestId. 헤더가 짧아 읽을 수 없으면 0
     */
    public ProtocolException(String message, long requestId) {
        super(message);
        this.requestId = requestId;
    }

    public long requestId() {
        return requestId;
    }
}
