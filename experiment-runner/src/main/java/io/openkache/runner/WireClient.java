package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * {@code docs/protocol-v1.md} 형식의 TCP client. 한 번에 요청 하나만 보내고 응답을 기다리는 동기(closed-loop)
 * 클라이언트다. pipelining이나 재시도는 하지 않는다.
 *
 * <p>요청 I/O는 thread 하나가 소유한다. 실험 취소 시에는 다른 thread가 {@link #close()}로 socket을 닫아
 * block된 read를 해제할 수 있다.
 */
final class WireClient implements AutoCloseable {

    /**
     * 응답에서 Runner가 관찰하는 필드.
     *
     * @param routingVersion 응답에 실린 서버 표 버전(B1 서버만 의미 있음)
     * @param tail value 뒤에 덧붙은 bytes. MOVED면 protocol-v1 §6-1 redirect, 아니면 빈 배열
     */
    record Response(int status, long valueVersion, byte[] value, String detail, long routingVersion, byte[] tail) {}

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private long nextRequestId = 1;

    WireClient(String host, int port, long soTimeoutMs) throws IOException {
        this.socket = new Socket();
        this.socket.connect(
                new InetSocketAddress(host, port), (int) Math.min(soTimeoutMs, Integer.MAX_VALUE));
        this.socket.setSoTimeout((int) Math.min(soTimeoutMs, Integer.MAX_VALUE));
        this.socket.setTcpNoDelay(true);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    /** PING. 연결 확인용이며 실패하면 예외를 던진다. */
    void ping() throws IOException {
        long requestId = nextRequestId++;
        writeRequest(WireProtocol.CLASS_CLIENT_REQUEST, WireProtocol.OP_PING, requestId, 0, new byte[0]);
        readResponse();
    }

    /** peer class 요청(B1 표 조회·이동 지시 등). peer listener 포트로 연 연결에서만 쓴다. */
    Response peer(int opcode, byte[] body) throws IOException {
        writeRequest(WireProtocol.CLASS_PEER, opcode, nextRequestId++, 0, body);
        return readResponse();
    }

    /**
     * GET/SET/DELETE를 보내고 응답을 기다린다.
     *
     * @param ownershipEpoch 클라이언트가 아는 epoch. 단일 노드 baseline은 항상 0
     * @param timeoutMs 서버에 보내는 상대 timeout(ms). 0이면 서버 기본값
     */
    Response send(Command command, long clientId, long operationId, long ownershipEpoch, long timeoutMs)
            throws IOException {
        return send(command, clientId, operationId, ownershipEpoch, timeoutMs, 0);
    }

    /** {@code routingVersion}은 클라이언트가 아는 라우팅 표 버전(헤더 필드). 모르면 0. */
    Response send(Command command, long clientId, long operationId, long ownershipEpoch, long timeoutMs,
                  long routingVersion) throws IOException {
        int opcode = opcodeOf(command);
        byte[] body = encodeBody(command, clientId, operationId, ownershipEpoch, timeoutMs);
        writeRequest(WireProtocol.CLASS_CLIENT_REQUEST, opcode, operationId, routingVersion, body);
        return readResponse();
    }

    private static int opcodeOf(Command command) {
        return switch (command) {
            case Command.Get get -> WireProtocol.OP_GET;
            case Command.Set set -> WireProtocol.OP_SET;
            case Command.Delete delete -> WireProtocol.OP_DELETE;
            // protocol-v1에 EXISTS/COUNT opcode가 아직 없다. WorkloadGenerator도 이 연산을 만들지 않는다.
            case Command.Exists exists -> throw new IllegalArgumentException("EXISTS has no wire opcode yet");
            case Command.Count count -> throw new IllegalArgumentException("COUNT has no wire opcode yet");
        };
    }

    private static Bytes keyOf(Command command) {
        return switch (command) {
            case Command.Get get -> get.key();
            case Command.Set set -> set.key();
            case Command.Delete delete -> delete.key();
            case Command.Exists exists -> exists.key();
            case Command.Count count -> throw new IllegalArgumentException("COUNT has no key");
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
            Bytes key = keyOf(command);
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

    private void writeRequest(int messageClass, int opcode, long requestId, long routingVersion, byte[] body)
            throws IOException {
        out.writeInt(WireProtocol.HEADER_BYTES + body.length);
        out.writeShort(WireProtocol.MAGIC);
        out.writeByte(WireProtocol.VERSION);
        out.writeByte(messageClass);
        out.writeByte(opcode);
        out.writeByte(0); // flags
        out.writeShort(0); // status
        out.writeLong(requestId);
        out.writeLong(routingVersion);
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
        long routingVersion = in.readLong();
        in.readLong(); // ownershipEpoch
        long valueVersion = in.readLong();
        int detailLength = in.readInt();
        byte[] detailBytes = new byte[detailLength];
        in.readFully(detailBytes);
        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.readFully(value);

        int consumed = 2 + 1 + 1 + 1 + 1 + 2 + 8 + 8 + 8 + 8 + 4 + detailLength + 4 + valueLength;
        if (consumed > length) {
            throw new IOException("response length mismatch: declared " + length + " consumed " + consumed);
        }
        // B1 MOVED 응답은 value 뒤에 redirect 주소(§6-1)를 덧붙인다
        byte[] tail = new byte[length - consumed];
        in.readFully(tail);
        return new Response(status, valueVersion, value, new String(detailBytes, StandardCharsets.UTF_8),
                routingVersion, tail);
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
