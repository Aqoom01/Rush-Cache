package io.openkache.server.ionia;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import java.io.*;
import java.util.*;
import io.openkache.server.observation.TcpTraffic;

/** Explicit bounded O00 wire format; intentionally separate from the baseline single-node protocol-v1. */
public final class IoniaWire {
    public static final int MAGIC = 0x494F4E31, VERSION = 1, MAX_FRAME = 64 * 1024 * 1024;
    public static final int PEER = 1, RECORD = 2, EXECUTE = 3, READ = 4, META = 5, PROBE = 6;
    private IoniaWire() { }
    @FunctionalInterface public interface Writer { void write(DataOutputStream out) throws IOException; }
    public record Packet(long correlation, NodeId source, NodeId target, String configuration, int kind, DataInputStream body) { }
    public static void write(OutputStream destination, long correlation, NodeId source, NodeId target,
                             String configuration, int kind, Writer writer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC); out.writeInt(VERSION); out.writeLong(correlation);
            out.writeLong(source.value()); out.writeLong(target.value()); out.writeUTF(configuration); out.writeByte(kind); writer.write(out);
        }
        if (bytes.size() > MAX_FRAME) throw new IOException("IONIA frame exceeds limit");
        try (var traffic = TcpTraffic.frame(configuration, source.value(), target.value(), kind)) {
            DataOutputStream out = new DataOutputStream(traffic.output(destination)); out.writeInt(bytes.size()); bytes.writeTo(out); out.flush();
            traffic.succeeded(); traffic.written();
        }
    }
    public static Packet read(InputStream source) throws IOException {
        DataInputStream network = new DataInputStream(source); int size = network.readInt();
        if (size < 37 || size > MAX_FRAME) throw new IOException("invalid IONIA frame size");
        byte[] bytes = network.readNBytes(size); if (bytes.length != size) throw new EOFException("truncated IONIA frame");
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        if (in.readInt() != MAGIC || in.readInt() != VERSION) throw new IOException("invalid IONIA magic/version");
        return new Packet(in.readLong(), new NodeId(in.readLong()), new NodeId(in.readLong()), in.readUTF(), in.readUnsignedByte(), in);
    }
    public static void exhausted(DataInputStream in) throws IOException { if (in.available() != 0) throw new IOException("trailing payload"); }
    public static void bytes(DataOutputStream out, Bytes bytes) throws IOException {
        out.writeInt(bytes.size()); out.write(bytes.toByteArray());
    }
    public static Bytes bytes(DataInputStream in) throws IOException {
        int size = in.readInt(); if (size < 0 || size > 32 * 1024 * 1024 || size > in.available()) throw new IOException("invalid byte length");
        return Bytes.copyOf(in.readNBytes(size));
    }
    public static void command(DataOutputStream out, Command command) throws IOException {
        out.writeByte(switch (command) {
            case Command.Get ignored -> 1;
            case Command.Set ignored -> 2;
            case Command.Delete ignored -> 3;
            default -> throw new IOException("unsupported IONIA command");
        });
        bytes(out, IoniaCommands.key(command)); if (command instanceof Command.Set set) bytes(out, set.value());
    }
    public static Command command(DataInputStream in) throws IOException {
        int type = in.readUnsignedByte(); Bytes key = bytes(in);
        return switch (type) { case 1 -> new Command.Get(key); case 2 -> new Command.Set(key, bytes(in));
            case 3 -> new Command.Delete(key); default -> throw new IOException("unknown command"); };
    }
    public static void request(DataOutputStream out, IoniaClientTransport.Request request) throws IOException {
        out.writeLong(request.view()); out.writeLong(request.clientId().value()); out.writeLong(request.operationId().value());
        out.writeLong(request.routingVersion().value()); out.writeLong(request.ownershipEpoch().value()); out.writeLong(request.timeoutNanos());
        command(out, request.command());
    }
    public static IoniaClientTransport.Request request(DataInputStream in) throws IOException {
        long view = in.readLong(); ClientId client = new ClientId(in.readLong()); OperationId operation = new OperationId(in.readLong());
        RoutingVersion routing = new RoutingVersion(in.readLong()); OwnershipEpoch ownership = new OwnershipEpoch(in.readLong()); long timeout = in.readLong();
        return new IoniaClientTransport.Request(view, command(in), client, operation, routing, ownership, timeout);
    }
    public static void identity(DataOutputStream out, long view, ClientId client, OperationId operation) throws IOException {
        out.writeLong(view); out.writeLong(client.value()); out.writeLong(operation.value());
    }
    public static void result(DataOutputStream out, Result result) throws IOException {
        out.writeUTF(result.status().name()); out.writeLong(result.valueVersion());
        out.writeLong(result.routingVersion().value()); out.writeLong(result.ownershipEpoch().value());
        out.writeBoolean(result.value().isPresent()); if (result.value().isPresent()) bytes(out, result.value().orElseThrow());
        if (result.redirect().isPresent()) throw new IOException("O00 static configuration does not support redirects");
    }
    public static Result result(DataInputStream in) throws IOException {
        ResultStatus status = ResultStatus.valueOf(in.readUTF()); long version = in.readLong();
        RoutingVersion routing = new RoutingVersion(in.readLong()); OwnershipEpoch ownership = new OwnershipEpoch(in.readLong());
        Optional<Bytes> value = in.readBoolean() ? Optional.of(bytes(in)) : Optional.empty();
        return new Result(status, value, version, routing, ownership, Optional.empty());
    }
    private static void entries(DataOutputStream out, List<IoniaMessage.LogEntry> entries) throws IOException {
        out.writeInt(entries.size());
        for (IoniaMessage.LogEntry entry : entries) {
            out.writeLong(entry.index()); out.writeLong(entry.clientId().value()); out.writeLong(entry.operationId().value()); command(out, entry.command());
        }
    }
    private static List<IoniaMessage.LogEntry> entries(DataInputStream in) throws IOException {
        int count = in.readInt(); if (count < 0 || count > 1_000_000 || count > in.available() / 29) throw new IOException("invalid log entry count");
        List<IoniaMessage.LogEntry> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long index = in.readLong(); ClientId client = new ClientId(in.readLong()); OperationId operation = new OperationId(in.readLong());
            result.add(new IoniaMessage.LogEntry(index, client, operation, command(in)));
        }
        return result;
    }
    public static void state(DataOutputStream out, IoniaRecovery.State state) throws IOException {
        out.writeLong(state.lastNormalView()); out.writeLong(state.commitIndex()); entries(out, state.consensus()); entries(out, state.durability());
    }
    public static IoniaRecovery.State state(DataInputStream in) throws IOException {
        long normal = in.readLong(), committed = in.readLong();
        return new IoniaRecovery.State(normal, entries(in), entries(in), committed);
    }
    public static void peer(DataOutputStream out, IoniaMessage message) throws IOException {
        int tag = switch (message) {
            case IoniaMessage.Prepare ignored -> 1; case IoniaMessage.PrepareAck ignored -> 2; case IoniaMessage.Commit ignored -> 3;
            case IoniaMessage.Applied ignored -> 4; case IoniaMessage.StartViewChange ignored -> 5; case IoniaMessage.DoViewChange ignored -> 6;
            case IoniaMessage.StartView ignored -> 7; case IoniaMessage.RecoveryRequest ignored -> 8; case IoniaMessage.RecoveryResponse ignored -> 9;
        };
        out.writeByte(tag); out.writeLong(message.view());
        switch (message) {
            case IoniaMessage.Prepare value -> entries(out, value.entries());
            case IoniaMessage.PrepareAck value -> out.writeLong(value.lastLogIndex());
            case IoniaMessage.Commit value -> out.writeLong(value.commitIndex());
            case IoniaMessage.Applied value -> out.writeLong(value.appliedIndex());
            case IoniaMessage.StartViewChange ignored -> { }
            case IoniaMessage.DoViewChange value -> state(out, value.state());
            case IoniaMessage.StartView value -> state(out, value.state());
            case IoniaMessage.RecoveryRequest value -> { out.writeLong(value.nonce().getMostSignificantBits()); out.writeLong(value.nonce().getLeastSignificantBits()); }
            case IoniaMessage.RecoveryResponse value -> {
                out.writeLong(value.nonce().getMostSignificantBits()); out.writeLong(value.nonce().getLeastSignificantBits());
                out.writeBoolean(value.state() != null); if (value.state() != null) state(out, value.state());
            }
        }
    }
    public static IoniaMessage peer(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte(); long view = in.readLong();
        return switch (tag) {
            case 1 -> new IoniaMessage.Prepare(view, entries(in)); case 2 -> new IoniaMessage.PrepareAck(view, in.readLong());
            case 3 -> new IoniaMessage.Commit(view, in.readLong()); case 4 -> new IoniaMessage.Applied(view, in.readLong());
            case 5 -> new IoniaMessage.StartViewChange(view); case 6 -> new IoniaMessage.DoViewChange(view, state(in));
            case 7 -> new IoniaMessage.StartView(view, state(in)); case 8 -> new IoniaMessage.RecoveryRequest(view, new UUID(in.readLong(), in.readLong()));
            case 9 -> new IoniaMessage.RecoveryResponse(view, new UUID(in.readLong(), in.readLong()), in.readBoolean() ? state(in) : null);
            default -> throw new IOException("unknown peer opcode");
        };
    }
}
