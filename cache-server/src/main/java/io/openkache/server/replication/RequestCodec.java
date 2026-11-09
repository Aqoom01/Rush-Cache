package io.openkache.server.replication;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.server.netty.Protocol;
import java.io.*;
import java.util.Optional;

/** Chain/Rosé codec가 공유하는 로그 요청·결과 형식. VR/Raft §8 명령 tag와 같은 배치이며 client opcode를 늘리지 않는다. */
final class RequestCodec {
    static final int MAX_BODY=Protocol.MAX_FRAME_BYTES-Protocol.HEADER_BYTES-Protocol.LENGTH_FIELD_BYTES;
    record Decoded(ClientId client,OperationId operation,Command command) {}
    private RequestCodec() {}
    static void request(DataOutputStream out,ClientId client,OperationId operation,Command command) throws IOException {
        out.writeLong(client.value()); out.writeLong(operation.value());
        switch(command) {
            case Command.Set x -> { out.writeByte(1); field(out,x.key()); field(out,x.value()); }
            case Command.Get x -> { out.writeByte(2); field(out,x.key()); }
            case Command.Delete x -> { out.writeByte(3); field(out,x.key()); }
            case Command.Exists x -> { out.writeByte(4); field(out,x.key()); }
            case Command.Count x -> out.writeByte(5);
        }
        if(out.size()>MAX_BODY) throw new IllegalArgumentException("request exceeds frame limit");
    }
    static Decoded request(DataInputStream in) throws IOException {
        ClientId client=new ClientId(in.readLong()); OperationId op=new OperationId(in.readLong());
        Command c=switch(in.readUnsignedByte()) {
            case 1 -> new Command.Set(field(in),field(in)); case 2 -> new Command.Get(field(in));
            case 3 -> new Command.Delete(field(in)); case 4 -> new Command.Exists(field(in)); case 5 -> new Command.Count();
            default -> throw new IllegalArgumentException("unknown command tag");
        };
        return new Decoded(client,op,c);
    }
    /** status(u8 ordinal), hasValue(u8), [value], valueVersion(u64), routingVersion(u64), ownershipEpoch(u64), hasRedirect(u8), [node u64]. */
    static void result(DataOutputStream out,Result r) throws IOException {
        out.writeByte(r.status().ordinal()); out.writeByte(r.value().isPresent()?1:0);
        if(r.value().isPresent()) field(out,r.value().orElseThrow());
        out.writeLong(r.valueVersion()); out.writeLong(r.routingVersion().value()); out.writeLong(r.ownershipEpoch().value());
        out.writeByte(r.redirect().isPresent()?1:0);
        if(r.redirect().isPresent()) out.writeLong(r.redirect().orElseThrow().target().value());
    }
    static Result result(DataInputStream in) throws IOException {
        int ordinal=in.readUnsignedByte(); if(ordinal>=ResultStatus.values().length) throw new IllegalArgumentException("unknown status");
        ResultStatus status=ResultStatus.values()[ordinal];
        Optional<Bytes> value=bool(in)?Optional.of(field(in)):Optional.empty();
        long version=in.readLong(); RoutingVersion routing=new RoutingVersion(in.readLong()); OwnershipEpoch epoch=new OwnershipEpoch(in.readLong());
        Optional<Redirect> redirect=bool(in)?Optional.of(new Redirect(new NodeId(in.readLong()))):Optional.empty();
        return new Result(status,value,version,routing,epoch,redirect);
    }
    static boolean bool(DataInputStream in) throws IOException {
        int b=in.readUnsignedByte(); if(b>1) throw new IllegalArgumentException("invalid boolean"); return b==1;
    }
    static void field(DataOutputStream out,Bytes value) throws IOException {
        if(value.size()>MAX_BODY-out.size()-4) throw new IllegalArgumentException("field exceeds frame limit");
        out.writeInt(value.size()); out.write(value.toByteArray());
    }
    static Bytes field(DataInputStream in) throws IOException {
        int n=in.readInt(); if(n<0 || n>in.available()) throw new IllegalArgumentException("invalid field length");
        return Bytes.copyOf(in.readNBytes(n));
    }
}
