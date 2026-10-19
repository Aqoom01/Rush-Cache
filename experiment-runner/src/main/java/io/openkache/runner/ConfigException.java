package io.openkache.runner;

import java.io.Serial;

/** Runner 설정이 잘못되었다. */
public final class ConfigException extends RuntimeException {

    @Serial private static final long serialVersionUID = 1L;

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
