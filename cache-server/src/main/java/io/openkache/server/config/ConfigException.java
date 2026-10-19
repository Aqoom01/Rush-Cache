package io.openkache.server.config;

import java.io.Serial;

/** 설정을 읽을 수 없거나 값이 잘못되었다. 서버는 기동하지 않는다. */
public final class ConfigException extends RuntimeException {

    @Serial private static final long serialVersionUID = 1L;

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
