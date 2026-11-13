package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.server.netty.Protocol;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** 기존 client wire v1과 통합 실험 전용 peer opcode 96..105. 길이·응답 ID·헤더를 검증한다. */
public final class IntegrationWire {
    public static final int STATUS = 96, BALANCE = 97, MOVE = 98, RESET_METRICS = 99, TABLE = 100, READY = 101, SCALE = 102, STOP_CONTROL = 103, CONFIRM_CRASH = 104, RESUME = 105;
    private IntegrationWire() { }
    public record Packet(int messageClass, int opcode, int status, long requestId, long version, byte[] body) { }
    public record Request(Command command, long client, long operation, long epoch, long timeoutMs) { }
    public static Packet read(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 24 || size > Protocol.MAX_FRAME_BYTES - 4) throw new IOException("invalid frame length");
        byte[] data = in.readNBytes(size); if (data.length != size) throw new EOFException("truncated frame");
        var b = new DataInputStream(new ByteArrayInputStream(data));
        if (b.readUnsignedShort() != Protocol.MAGIC || b.readUnsignedByte() != Protocol.VERSION) throw new IOException("invalid magic/version");
        int kind = b.readUnsignedByte(), opcode = b.readUnsignedByte(), flags = b.readUnsignedByte(), status = b.readUnsignedShort();
        long id = b.readLong(), version = b.readLong();
        if (flags != 0 || id < 0 || version < 0 || kind < 1 || kind > 3 || kind != 2 && status != 0) throw new IOException("invalid frame header");
        return new Packet(kind, opcode, status, id, version, b.readAllBytes());
    }
    public static void write(DataOutputStream out, Packet p) throws IOException {
        if (p.body().length > Protocol.MAX_FRAME_BYTES - 28) throw new IOException("frame too large");
        out.writeInt(24 + p.body().length); out.writeShort(Protocol.MAGIC); out.writeByte(Protocol.VERSION);
        out.writeByte(p.messageClass()); out.writeByte(p.opcode()); out.writeByte(0); out.writeShort(p.status()); out.writeLong(p.requestId()); out.writeLong(p.version());
        out.write(p.body()); out.flush();
    }
    static Bytes field(DataInputStream in) throws IOException {
        int length = in.readInt(); if (length < 0 || length > in.available()) throw new IOException("invalid body field"); return Bytes.copyOf(in.readNBytes(length));
    }
    static void field(DataOutputStream out, Bytes data) throws IOException { out.writeInt(data.size()); out.write(data.toByteArray()); }
    static Request decode(Packet p) throws IOException {
        var in = new DataInputStream(new ByteArrayInputStream(p.body()));
        long client = in.readLong(), op = in.readLong(), epoch = in.readLong(), timeout = Integer.toUnsignedLong(in.readInt());
        Bytes key = field(in);
        Command c = switch (p.opcode()) { case 1 -> new Command.Get(key); case 2 -> new Command.Set(key, field(in)); case 3 -> new Command.Delete(key); default -> throw new IOException("invalid command opcode"); };
        if (client < 0 || op < 0 || epoch < 0 || timeout > 60000 || in.available() != 0) throw new IOException("invalid request");
        return new Request(c, client, op, epoch, timeout);
    }
    static Packet reply(Packet p, Result r, String detail) throws IOException {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeLong(r.ownershipEpoch().value()); out.writeLong(r.valueVersion()); field(out, Bytes.utf8(detail)); field(out, r.value().orElse(Bytes.empty()));
        return new Packet(2, p.opcode(), Protocol.statusCode(r.status()), p.requestId(), r.routingVersion().value(), bytes.toByteArray());
    }
    static Result decodeResult(Packet p) throws IOException {
        var in = new DataInputStream(new ByteArrayInputStream(p.body())); long epoch = in.readLong(), version = in.readLong();
        String detail = new String(field(in).toByteArray(), StandardCharsets.UTF_8); Bytes data = field(in);
        if (in.available() != 0) throw new IOException("unexpected response tail");
        ResultStatus status = switch (p.status()) {
            case 0 -> ResultStatus.OK; case 1 -> ResultStatus.MISS; case 2 -> ResultStatus.BAD_REQUEST; case 3 -> ResultStatus.MOVED;
            case 4 -> ResultStatus.NOT_PRIMARY; case 5 -> ResultStatus.STALE_EPOCH; case 6 -> ResultStatus.BUSY; case 7 -> ResultStatus.TIMEOUT;
            case 8 -> ResultStatus.UNAVAILABLE; case 9 -> ResultStatus.CAPACITY_EXCEEDED; case 10 -> ResultStatus.NOT_IMPLEMENTED; case 11 -> ResultStatus.INTERNAL_ERROR;
            default -> throw new IOException("unknown status");
        };
        Optional<Redirect> redirect = detail.startsWith("redirect-node=") ? Optional.of(new Redirect(new NodeId(Long.parseLong(detail.substring(14))))) : Optional.empty();
        return new Result(status, p.opcode() == 1 && status == ResultStatus.OK ? Optional.of(data) : Optional.empty(), version, new RoutingVersion(p.version()), new OwnershipEpoch(epoch), redirect);
    }
    static Result call(InetSocketAddress address, Command c, long client, long operation, long epoch, int timeoutMs) throws IOException {
        var bytes = new ByteArrayOutputStream(); var body = new DataOutputStream(bytes);
        body.writeLong(client); body.writeLong(operation); body.writeLong(epoch); body.writeInt(timeoutMs);
        Bytes key = c instanceof Command.Get g ? g.key() : c instanceof Command.Set s ? s.key() : ((Command.Delete) c).key(); field(body, key);
        if (c instanceof Command.Set s) field(body, s.value());
        int opcode = c instanceof Command.Get ? 1 : c instanceof Command.Set ? 2 : 3;
        try (var socket = new Socket()) {
            socket.connect(address, Math.min(timeoutMs, 500)); socket.setSoTimeout(timeoutMs); socket.setTcpNoDelay(true);
            write(new DataOutputStream(socket.getOutputStream()), new Packet(1, opcode, 0, operation, 0, bytes.toByteArray()));
            Packet reply = read(new DataInputStream(socket.getInputStream()));
            if (reply.messageClass() != 2 || reply.requestId() != operation || reply.opcode() != opcode) throw new IOException("reply identity mismatch");
            return decodeResult(reply);
        }
    }
}
