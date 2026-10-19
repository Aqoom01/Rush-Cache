package io.openkache.server.lifecycle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.server.config.ServerConfig;
import io.openkache.server.netty.Protocol;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 실제 TCP로 서버를 띄워 확인한다. */
class ServerApplicationTest {

    private ServerConfig config;
    private ServerApplication app;

    /** 응답 frame에서 테스트가 확인하는 필드. */
    private record Response(
            int messageClass,
            int opcode,
            int status,
            long requestId,
            long routingVersion,
            long ownershipEpoch,
            long valueVersion,
            String detail,
            byte[] value) {}

    @BeforeEach
    void start() throws Exception {
        config = new ServerConfig(
                new NodeId(1), "test", "127.0.0.1", "127.0.0.1",
                freePort(), freePort(), freePort(), 2, Duration.ofSeconds(2));
        app = ServerApplication.start(config);
    }

    @AfterEach
    void stop() {
        app.close();
    }

    @Test
    void clientPingReturnsOk() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            send(socket, Protocol.CLASS_CLIENT_REQUEST, Protocol.OP_PING, 42);
            Response response = read(socket);

            assertEquals(Protocol.CLASS_RESPONSE, response.messageClass());
            assertEquals(Protocol.OP_PING, response.opcode());
            assertEquals(0, response.status()); // OK
            assertEquals(42, response.requestId());
            assertEquals("", response.detail());
        }
    }

    @Test
    void severalPingsOnOneConnectionKeepRequestIds() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            for (long id = 1; id <= 3; id++) {
                send(socket, Protocol.CLASS_CLIENT_REQUEST, Protocol.OP_PING, id);
            }
            for (long id = 1; id <= 3; id++) {
                assertEquals(id, read(socket).requestId());
            }
        }
    }

    @Test
    void getOnMissingKeyReturnsMiss() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            sendGet(socket, 5, bytes("absent-key"));

            Response response = read(socket);
            assertEquals(1, response.status()); // MISS
            assertEquals(5, response.requestId());
        }
    }

    @Test
    void setThenGetRoundTripsValueAndVersion() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            byte[] key = bytes("k");

            sendSet(socket, 1, key, bytes("v1"));
            Response firstSet = read(socket);
            assertEquals(0, firstSet.status()); // OK
            assertEquals(1, firstSet.valueVersion());

            sendSet(socket, 2, key, bytes("v2"));
            assertEquals(2, read(socket).valueVersion());

            sendGet(socket, 3, key);
            Response get = read(socket);
            assertEquals(0, get.status()); // OK
            assertEquals(2, get.valueVersion());
            assertArrayEquals(bytes("v2"), get.value());
        }
    }

    @Test
    void deleteRemovesKeyAndMissingDeleteIsMiss() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            byte[] key = bytes("to-delete");
            sendSet(socket, 1, key, bytes("v"));
            read(socket);

            sendDelete(socket, 2, key);
            assertEquals(0, read(socket).status()); // OK

            sendDelete(socket, 3, key);
            assertEquals(1, read(socket).status()); // MISS

            sendGet(socket, 4, key);
            assertEquals(1, read(socket).status()); // MISS
        }
    }

    @Test
    void malformedCommandBodyIsBadRequestAndKeepsConnectionOpen() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            send(socket, Protocol.CLASS_CLIENT_REQUEST, Protocol.OP_GET, 5); // body가 비어 짧다.

            Response response = read(socket);
            assertEquals(2, response.status()); // BAD_REQUEST

            send(socket, Protocol.CLASS_CLIENT_REQUEST, Protocol.OP_PING, 6);
            assertEquals(0, read(socket).status()); // 연결은 끊기지 않았다.
        }
    }

    @Test
    void clientListenerRejectsPeerClass() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            send(socket, Protocol.CLASS_PEER, Protocol.OP_PING, 6);

            assertEquals(2, read(socket).status()); // BAD_REQUEST
        }
    }

    @Test
    void peerListenerAnswersPingButNotProtocolMessages() throws IOException {
        try (Socket socket = connect(config.peerPort())) {
            send(socket, Protocol.CLASS_PEER, Protocol.OP_PING, 1);
            assertEquals(0, read(socket).status()); // OK

            send(socket, Protocol.CLASS_PEER, 100, 2);
            Response response = read(socket);
            assertEquals(10, response.status()); // NOT_IMPLEMENTED
            assertTrue(response.detail().contains("not implemented"));
        }
    }

    @Test
    void badMagicIsRejectedAndConnectionClosed() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(Protocol.HEADER_BYTES);
            out.writeShort(0x1234);
            out.write(new byte[Protocol.HEADER_BYTES - 2]);
            out.flush();

            Response response = read(socket);
            assertEquals(2, response.status()); // BAD_REQUEST
            assertTrue(response.detail().contains("magic"));
            assertTrue(isClosedByServer(socket));
        }
    }

    @Test
    void metricsEndpointExposesRequestCountAndJvmMetrics() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            send(socket, Protocol.CLASS_CLIENT_REQUEST, Protocol.OP_PING, 1);
            read(socket);
        }

        HttpURLConnection http = (HttpURLConnection)
                URI.create("http://127.0.0.1:" + config.adminPort() + "/metrics").toURL().openConnection();
        String body = new String(http.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertEquals(200, http.getResponseCode());
        assertTrue(body.contains("openkache_requests_total{listener=\"client\",opcode=\"ping\",status=\"ok\"} 1"));
        assertTrue(body.contains("jvm_memory_used_bytes"));
    }

    @Test
    void closeIsIdempotentAndLeavesNoServerThreads() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            send(socket, Protocol.CLASS_CLIENT_REQUEST, Protocol.OP_PING, 1);
            read(socket);
        }

        app.close();
        app.close();

        List<String> remaining = Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith("openkache-") || name.startsWith("HTTP-Dispatcher"))
                .toList();
        assertEquals(List.of(), remaining);
    }

    private static Socket connect(int port) throws IOException {
        Socket socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5_000);
        return socket;
    }

    private static void send(Socket socket, int messageClass, int opcode, long requestId) throws IOException {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(Protocol.HEADER_BYTES);
        out.writeShort(Protocol.MAGIC);
        out.writeByte(Protocol.VERSION);
        out.writeByte(messageClass);
        out.writeByte(opcode);
        out.writeByte(0); // flags
        out.writeShort(0); // status
        out.writeLong(requestId);
        out.writeLong(0); // routingVersion
        out.flush();
    }

    private static Response read(Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        int length = in.readInt();
        assertEquals(Protocol.MAGIC, in.readUnsignedShort());
        assertEquals(Protocol.VERSION, in.readUnsignedByte());
        int messageClass = in.readUnsignedByte();
        int opcode = in.readUnsignedByte();
        in.readUnsignedByte(); // flags
        int status = in.readUnsignedShort();
        long requestId = in.readLong();
        long routingVersion = in.readLong();
        long ownershipEpoch = in.readLong();
        long valueVersion = in.readLong();
        byte[] detail = new byte[in.readInt()];
        in.readFully(detail);
        byte[] value = new byte[in.readInt()];
        in.readFully(value);
        assertEquals(Protocol.HEADER_BYTES + 24 + detail.length + value.length, length);
        return new Response(
                messageClass,
                opcode,
                status,
                requestId,
                routingVersion,
                ownershipEpoch,
                valueVersion,
                new String(detail, StandardCharsets.UTF_8),
                value);
    }

    private static void sendGet(Socket socket, long requestId, byte[] key) throws IOException {
        sendCommand(socket, Protocol.OP_GET, requestId, key, null);
    }

    private static void sendDelete(Socket socket, long requestId, byte[] key) throws IOException {
        sendCommand(socket, Protocol.OP_DELETE, requestId, key, null);
    }

    private static void sendSet(Socket socket, long requestId, byte[] key, byte[] value) throws IOException {
        sendCommand(socket, Protocol.OP_SET, requestId, key, value);
    }

    /** GET/DELETE는 {@code value}를 {@code null}로 전달해 body에서 뺀다. */
    private static void sendCommand(Socket socket, int opcode, long requestId, byte[] key, byte[] value)
            throws IOException {
        ByteArrayOutputStream bodyBuffer = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyBuffer);
        body.writeLong(1); // clientId
        body.writeLong(requestId); // operationId
        body.writeLong(0); // ownershipEpoch (클라이언트가 아는 값)
        body.writeInt(0); // timeoutMs: 0은 서버 기본값
        body.writeInt(key.length);
        body.write(key);
        if (value != null) {
            body.writeInt(value.length);
            body.write(value);
        }
        byte[] bodyBytes = bodyBuffer.toByteArray();

        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(Protocol.HEADER_BYTES + bodyBytes.length);
        out.writeShort(Protocol.MAGIC);
        out.writeByte(Protocol.VERSION);
        out.writeByte(Protocol.CLASS_CLIENT_REQUEST);
        out.writeByte(opcode);
        out.writeByte(0); // flags
        out.writeShort(0); // status
        out.writeLong(requestId);
        out.writeLong(0); // routingVersion
        out.write(bodyBytes);
        out.flush();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean isClosedByServer(Socket socket) throws IOException {
        try {
            return socket.getInputStream().read() == -1;
        } catch (EOFException e) {
            return true;
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
