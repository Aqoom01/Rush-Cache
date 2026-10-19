package io.openkache.server.netty;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import java.io.Serial;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * GET/SET/DELETE frame body와 core {@link Command}를 서로 바꾼다. wire 형식은 {@code docs/protocol-v1.md}를 따른다.
 *
 * <p>요청 body 레이아웃: clientId(u64), operationId(u64), ownershipEpoch(u64), timeoutMs(u32), keyLength(u32),
 * key, [SET만] valueLength(u32), value.
 */
final class CommandCodec {

    private CommandCodec() {}

    /** 잘못된 body를 가리킨다. 헤더는 유효하므로 연결을 닫지 않고 {@code BAD_REQUEST}로만 응답한다. */
    static final class MalformedBodyException extends RuntimeException {
        @Serial private static final long serialVersionUID = 1L;

        MalformedBodyException(String message) {
            super(message);
        }
    }

    record DecodedRequest(
            Command command, ClientId clientId, OperationId operationId, OwnershipEpoch ownershipEpoch, long timeoutMs) {}

    static DecodedRequest decode(int opcode, Bytes body) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(body.toByteArray());
            long clientId = buf.getLong();
            long operationId = buf.getLong();
            long ownershipEpoch = buf.getLong();
            long timeoutMs = Integer.toUnsignedLong(buf.getInt());
            Bytes key = readLengthPrefixed(buf);

            Command command =
                    switch (opcode) {
                        case Protocol.OP_GET -> new Command.Get(key);
                        case Protocol.OP_DELETE -> new Command.Delete(key);
                        case Protocol.OP_SET -> new Command.Set(key, readLengthPrefixed(buf));
                        default -> throw new IllegalArgumentException("not a kv opcode: " + opcode);
                    };

            if (buf.hasRemaining()) {
                throw new MalformedBodyException("trailing bytes after body: " + buf.remaining());
            }
            return new DecodedRequest(
                    command, new ClientId(clientId), new OperationId(operationId), new OwnershipEpoch(ownershipEpoch), timeoutMs);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new MalformedBodyException("malformed request body: " + e.getMessage());
        }
    }

    private static Bytes readLengthPrefixed(ByteBuffer buf) {
        int length = buf.getInt();
        if (length < 0 || length > buf.remaining()) {
            throw new MalformedBodyException("declared length " + length + " exceeds remaining body");
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return Bytes.copyOf(bytes);
    }
}
