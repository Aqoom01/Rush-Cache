package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * {@code docs/protocol-v1.md} 형식의 TCP client. 한 번에 요청 하나만 보내고 응답을 기다리는 동기(closed-loop)
 * 클라이언트다. pipelining이나 재시도는 하지 않는다.
 *
 * <p>thread 하나가 소유하며 공유하지 않는다. {@link Socket}은 thread-safe하지 않다.
 */
final class WireClient implements AutoCloseable {

    /** 응답에서 Runner가 관찰하는 필드. */
    record Response(int status, long valueVersion, byte[] value, String detail) {}

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private long nextRequestId = 1;

    WireClient(String host, int port, long soTimeoutMs) throws IOException {
        this.socket = new Socket(host, port);
        this.socket.setSoTimeout((int) Math.min(soTimeoutMs, Integer.MAX_VALUE));
        this.socket.setTcpNoDelay(true);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    /** PING. 연결 확인용이며 실패하면 예외를 던진다. */
    void ping() throws IOException {
        long requestId = nextRequestId++;
        writeRequest(WireProtocol.OP_PING, requestId, new byte[0]);
        readResponse();
    }

    /**
     * GET/SET/DELETE를 보내고 응답을 기다린다.
     *
     * @param ownershipEpoch 클라이언트가 아는 epoch. 단일 노드 baseline은 항상 0
     * @param timeoutMs 서버에 보내는 상대 timeout(ms). 0이면 서버 기본값
     */
    Response send(Command command, long clientId, long operationId, long ownershipEpoch, long timeoutMs)
            throws IOException {
        int opcode = opcodeOf(command);
        byte[] body = encodeBody(command, clientId, operationId, ownershipEpoch, timeoutMs);
        writeRequest(opcode, operationId, body);
        return readResponse();
    }

    private static int opcodeOf(Command command) {
        return switch (command) {
            case Command.Get get -> WireProtocol.OP_GET;
            case Command.Set set -> WireProtocol.OP_SET;
            case Command.Delete delete -> WireProtocol.OP_DELETE;
        };
    }

    private static byte[] encodeBody(
            Command command, long clientId, long operationId, long ownershipEpoch, long timeoutMs) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(buffer);
        try {
            body.writeLong(clientId);
            body.writeLong(operationId);
            body.writeLong(ownershipEpoch);
            body.writeInt((int) timeoutMs);
            Bytes key = command.key();
            byte[] keyBytes = key.toByteArray();
            body.writeInt(keyBytes.length);
            body.write(keyBytes);
            if (command instanceof Command.Set set) {
                byte[] valueBytes = set.value().toByteArray();
                body.writeInt(valueBytes.length);
                body.write(valueBytes);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("in-memory buffer write must not fail", e);
        }
        return buffer.toByteArray();
    }

    private void writeRequest(int opcode, long requestId, byte[] body) throws IOException {
        out.writeInt(WireProtocol.HEADER_BYTES + body.length);
        out.writeShort(WireProtocol.MAGIC);
        out.writeByte(WireProtocol.VERSION);
        out.writeByte(WireProtocol.CLASS_CLIENT_REQUEST);
        out.writeByte(opcode);
        out.writeByte(0); // flags
        out.writeShort(0); // status
        out.writeLong(requestId);
        out.writeLong(0); // routingVersion: 아는 값 없음(단일 노드 baseline)
        out.write(body);
        out.flush();
    }

    private Response readResponse() throws IOException {
        int length = in.readInt();
        int magic = in.readUnsignedShort();
        if (magic != WireProtocol.MAGIC) {
            throw new IOException("bad magic in response: 0x" + Integer.toHexString(magic));
        }
        in.readUnsignedByte(); // version
        in.readUnsignedByte(); // messageClass (항상 CLASS_RESPONSE로 기대하지만 검증하지 않는다: 최소 구현)
        in.readUnsignedByte(); // opcode
        in.readUnsignedByte(); // flags
        int status = in.readUnsignedShort();
        in.readLong(); // requestId (closed-loop라 매칭 없이 바로 소비한다)
        in.readLong(); // routingVersion
        in.readLong(); // ownershipEpoch
        long valueVersion = in.readLong();
        int detailLength = in.readInt();
        byte[] detailBytes = new byte[detailLength];
        in.readFully(detailBytes);
        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.readFully(value);

        int consumed = 2 + 1 + 1 + 1 + 1 + 2 + 8 + 8 + 8 + 8 + 4 + detailLength + 4 + valueLength;
        if (consumed != length) {
            throw new IOException("response length mismatch: declared " + length + " consumed " + consumed);
        }
        return new Response(status, valueVersion, value, new String(detailBytes, StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            // 정리 중 실패는 결과에 영향이 없으므로 무시한다.
        }
    }
}
