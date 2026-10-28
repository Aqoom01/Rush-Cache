package io.openkache.server.config;

/** 정적 클러스터 설정에 등록된 SWIM UDP 및 peer TCP 주소. */
public record PeerAddress(String host, int port) {

    public PeerAddress {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("peer host must not be blank");
        }
        host = host.trim();
        if (host.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("peer host must not contain whitespace: " + host);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("peer port must be between 1 and 65535: " + port);
        }
    }
}
