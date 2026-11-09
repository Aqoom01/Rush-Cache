package io.openkache.core.tablet.routing;

import java.nio.charset.StandardCharsets;

/**
 * 노드의 TCP 주소. TableMessageCodec이 MOVED redirect와 표 메시지에서 쓴다.
 *
 * <p>wire 형식(§6-1)은 docs/protocol-v1.md를 따른다.
 */
public record NodeAddress(String host, int port) {

    public NodeAddress {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        host = host.strip();
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1-65535: " + port);
        }
    }

    /** host:port 표현. 로그·디버그용. */
    @Override
    public String toString() {
        return host + ":" + port;
    }

    /** UTF-8 host 바이트 배열. wire 인코딩에서 쓴다. */
    public byte[] hostBytes() {
        return host.getBytes(StandardCharsets.UTF_8);
    }
}
