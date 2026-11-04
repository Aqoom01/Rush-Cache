package io.openkache.server.netty;

import io.openkache.core.api.ResultStatus;

/**
 * wire 프로토콜 v1 상수. 형식은 {@code docs/protocol-v1.md}를 따른다.
 *
 * <p>모든 정수는 network byte order(big-endian)다.
 */
public final class Protocol {

    /** length 필드 크기. */
    public static final int LENGTH_FIELD_BYTES = 4;
    /** length 필드 뒤 고정 헤더 크기. length의 최솟값이다. */
    public static final int HEADER_BYTES = 24;
    /** length 필드를 포함한 frame 최대 크기 (2 MiB). */
    public static final int MAX_FRAME_BYTES = 2 * 1024 * 1024;

    public static final int MAGIC = 0x4F4B;
    public static final int VERSION = 1;

    public static final int CLASS_CLIENT_REQUEST = 1;
    public static final int CLASS_RESPONSE = 2;
    public static final int CLASS_PEER = 3;

    /** 예약: 아직 body 형식이 정의되지 않았다. */
    public static final int OP_GET = 1;
    /** 예약: 아직 body 형식이 정의되지 않았다. */
    public static final int OP_SET = 2;
    /** 예약: 아직 body 형식이 정의되지 않았다. */
    public static final int OP_DELETE = 3;
    public static final int OP_PING = 4;

    /** SWIM direct probe 또는 PING_REQ helper가 보내는 probe. */
    public static final int OP_SWIM_PING = 16;
    /** SWIM probe 응답. */
    public static final int OP_SWIM_ACK = 17;
    /** 다른 노드에 간접 probe를 요청한다. */
    public static final int OP_SWIM_PING_REQ = 18;

    private Protocol() {}

    /** core 상태를 wire 상태 코드(u16)로 바꾼다. 코드 값은 한 번 정하면 바꾸지 않는다. */
    public static int statusCode(ResultStatus status) {
        return switch (status) {
            case OK -> 0;
            case MISS -> 1;
            case BAD_REQUEST -> 2;
            case MOVED -> 3;
            case NOT_PRIMARY -> 4;
            case STALE_EPOCH -> 5;
            case BUSY -> 6;
            case TIMEOUT -> 7;
            case UNAVAILABLE -> 8;
            case CAPACITY_EXCEEDED -> 9;
            case NOT_IMPLEMENTED -> 10;
            case INTERNAL_ERROR -> 11;
        };
    }

    /** metrics label용 opcode 이름. 알 수 없는 값은 모두 {@code "unknown"}으로 묶어 label 종류를 제한한다. */
    public static String opcodeName(int opcode) {
        return switch (opcode) {
            case OP_GET -> "get";
            case OP_SET -> "set";
            case OP_DELETE -> "delete";
            case OP_PING -> "ping";
            case OP_SWIM_PING -> "swim_ping";
            case OP_SWIM_ACK -> "swim_ack";
            case OP_SWIM_PING_REQ -> "swim_ping_req";
            default -> "unknown";
        };
    }
}
