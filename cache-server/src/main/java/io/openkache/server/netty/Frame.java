package io.openkache.server.netty;

import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * wire frame 하나. length와 magic, version은 codec이 처리하므로 담지 않는다.
 *
 * <p>server 내부 wire 타입이며 core의 {@code Command}와 별개다. 불변이다.
 *
 * @param messageClass u8. {@link Protocol#CLASS_CLIENT_REQUEST} 등
 * @param opcode u8
 * @param flags u8. v1에서는 0
 * @param status u16. 요청은 0
 * @param requestId 연결 안에서 요청과 응답을 짝짓는 값
 * @param routingVersion u64
 * @param body opcode별 body
 */
public record Frame(
        int messageClass,
        int opcode,
        int flags,
        int status,
        long requestId,
        long routingVersion,
        Bytes body) {

    public Frame {
        requireRange("messageClass", messageClass, 0xFF);
        requireRange("opcode", opcode, 0xFF);
        requireRange("flags", flags, 0xFF);
        requireRange("status", status, 0xFFFF);
        Objects.requireNonNull(body, "body");
        if (Protocol.HEADER_BYTES + body.size() > Protocol.MAX_FRAME_BYTES - Protocol.LENGTH_FIELD_BYTES) {
            throw new IllegalArgumentException("frame too large: body " + body.size() + " bytes");
        }
    }

    /** 클라이언트 요청 frame. body가 빈 PING 등에 쓴다. */
    public static Frame request(int opcode, long requestId) {
        return new Frame(Protocol.CLASS_CLIENT_REQUEST, opcode, 0, 0, requestId, 0, Bytes.empty());
    }

    /**
     * 엔진 없이 보내는 응답 frame(PING, BAD_REQUEST, NOT_IMPLEMENTED 등). ownershipEpoch, valueVersion, value는
     * 모두 0/empty다. routingVersion은 이 호출 시점에 엔진 상태를 모르므로 0이다.
     */
    public static Frame response(long requestId, int opcode, ResultStatus status, String detail) {
        return response(requestId, opcode, status, 0, 0, 0, Bytes.empty(), detail);
    }

    /** 엔진이 처리한 {@link Result}로 응답 frame을 만든다. MOVED/NOT_PRIMARY의 redirect 인코딩은 아직 정의되지 않았다. */
    public static Frame response(long requestId, int opcode, Result result, String detail) {
        if (result.redirect().isPresent()) {
            throw new UnsupportedOperationException(
                    "MOVED/NOT_PRIMARY redirect wire 인코딩은 아직 정의되지 않았다 (docs/protocol-v1.md 참고)");
        }
        byte[] valueBytes = result.value().map(Bytes::toByteArray).orElseGet(() -> new byte[0]);
        return response(
                requestId,
                opcode,
                result.status(),
                result.routingVersion().value(),
                result.ownershipEpoch().value(),
                result.valueVersion(),
                Bytes.copyOf(valueBytes),
                detail);
    }

    /**
     * 응답 body 공통 레이아웃: ownershipEpoch(u64), valueVersion(u64), detailLength(u32), detail,
     * valueLength(u32), value.
     */
    private static Frame response(
            long requestId,
            int opcode,
            ResultStatus status,
            long routingVersion,
            long ownershipEpoch,
            long valueVersion,
            Bytes value,
            String detail) {
        byte[] detailBytes = detail.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.toByteArray();
        ByteBuffer body = ByteBuffer.allocate(8 + 8 + 4 + detailBytes.length + 4 + valueBytes.length);
        body.putLong(ownershipEpoch)
                .putLong(valueVersion)
                .putInt(detailBytes.length)
                .put(detailBytes)
                .putInt(valueBytes.length)
                .put(valueBytes);
        return new Frame(
                Protocol.CLASS_RESPONSE,
                opcode,
                0,
                Protocol.statusCode(status),
                requestId,
                routingVersion,
                Bytes.copyOf(body.array()));
    }

    private static void requireRange(String field, int value, int max) {
        if (value < 0 || value > max) {
            throw new IllegalArgumentException(field + " out of range: " + value);
        }
    }
}
