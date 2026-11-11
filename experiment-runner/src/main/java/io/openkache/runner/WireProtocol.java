package io.openkache.runner;

/**
 * {@code docs/protocol-v1.md}의 wire 상수. {@code cache-server}의 {@code server.netty.Protocol}과 같은 값을
 * 독립적으로 유지한다(이 모듈은 Netty를 쓰지 않으려고 cache-server에 의존하지 않는다). 값을 바꾸면 양쪽과
 * {@code docs/protocol-v1.md}를 함께 갱신해야 한다.
 */
final class WireProtocol {

    static final int HEADER_BYTES = 24;
    static final int MAGIC = 0x4F4B;
    static final int VERSION = 1;

    static final int CLASS_CLIENT_REQUEST = 1;
    static final int CLASS_RESPONSE = 2;
    static final int CLASS_PEER = 3;

    static final int OP_GET = 1;
    static final int OP_SET = 2;
    static final int OP_DELETE = 3;
    static final int OP_PING = 4;

    static final int STATUS_OK = 0;
    static final int STATUS_MISS = 1;
    static final int STATUS_BAD_REQUEST = 2;
    static final int STATUS_MOVED = 3;
    static final int STATUS_BUSY = 6;
    static final int STATUS_TIMEOUT = 7;
    static final int STATUS_UNAVAILABLE = 8;

    private WireProtocol() {}
}
