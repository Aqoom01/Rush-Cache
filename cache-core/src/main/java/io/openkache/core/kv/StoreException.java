package io.openkache.core.kv;

import io.openkache.core.api.ResultStatus;

/** 저장소 연산이 {@code Result} 상태로 끝나야 하는 실패. {@link KvEngine}이 받아 그 상태의 결과로 바꾼다. */
final class StoreException extends Exception {

    private static final long serialVersionUID = 1L;

    private final ResultStatus status;

    StoreException(ResultStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    ResultStatus status() {
        return status;
    }
}
