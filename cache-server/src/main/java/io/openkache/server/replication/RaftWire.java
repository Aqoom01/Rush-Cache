package io.openkache.server.replication;

import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.raft.RaftMessage;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.io.*;
import java.util.*;

/** Raft RPC의 명시적 bounded codec. length 포함 2MiB 한도를 유지한다. */
public final class RaftWire {
    public static final int VOTE=64,VOTE_REPLY=65,APPEND=66,APPEND_REPLY=67;
    private static final int MAX_BODY=Protocol.MAX_FRAME_BYTES-Protocol.HEADER_BYTES-Protocol.LENGTH_FIELD_BYTES;
    private RaftWire() {}
    public static Frame encode(PeerEnvelope e) {
        if(!(e.message() instanceof RaftMessage m)) throw new IllegalArgumentException("not Raft");
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
            out.writeLong(e.from().value()); out.writeLong(e.to().value()); out.writeLong(m.term());
            int opcode=switch(m) {
                case RaftMessage.Vote x -> { out.writeInt(x.lastIndex()); out.writeLong(x.lastTerm()); yield VOTE; }
                case RaftMessage.VoteReply x -> { out.writeByte(x.granted()?1:0); yield VOTE_REPLY; }
                case RaftMessage.AppendReply x -> { out.writeLong(x.rpc()); out.writeByte(x.success()?1:0); out.writeInt(x.matched()); yield APPEND_REPLY; }
                case RaftMessage.Append x -> {
                    out.writeLong(x.rpc()); out.writeInt(x.prevIndex()); out.writeLong(x.prevTerm()); out.writeInt(x.commit()); out.writeInt(x.entries().size());
                    for(var entry:x.entries()) {
                        out.writeLong(entry.term()); out.writeByte(entry.request().isPresent()?1:0);
                        if(entry.request().isPresent()) request(out,entry.request().orElseThrow());
                        if(out.size()>MAX_BODY) throw new IllegalArgumentException("Raft log exceeds frame limit");
                    }
                    yield APPEND;
                }
            };
            return new Frame(Protocol.CLASS_PEER,opcode,0,0,0,0,Bytes.copyOf(bytes.toByteArray()));
        } catch(IOException x) { throw new IllegalArgumentException("Raft encode",x); }
    }
    private static void request(DataOutputStream out,RaftMessage.Request r) throws IOException {
        out.writeLong(r.client().value()); out.writeLong(r.operation().value());
        switch(r.command()) {
            case Command.Set x -> { out.writeByte(1); field(out,x.key()); field(out,x.value()); }
            case Command.Get x -> { out.writeByte(2); field(out,x.key()); }
            case Command.Delete x -> { out.writeByte(3); field(out,x.key()); }
            case Command.Exists x -> { out.writeByte(4); field(out,x.key()); }
            case Command.Count x -> out.writeByte(5);
        }
    }
    private static void field(DataOutputStream out,Bytes value) throws IOException {
        if(value.size()>MAX_BODY-out.size()-4) throw new IllegalArgumentException("Raft field exceeds frame limit");
        out.writeInt(value.size()); out.write(value.toByteArray());
    }
    public static PeerEnvelope decode(Frame f) {
        if(f.messageClass()!=Protocol.CLASS_PEER || f.flags()!=0 || f.status()!=0 || f.body().size()>MAX_BODY)
            throw new IllegalArgumentException("invalid Raft frame");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(f.body().toByteArray()));
            NodeId from=new NodeId(in.readLong()),to=new NodeId(in.readLong()); long term=in.readLong();
            RaftMessage m=switch(f.opcode()) {
                case VOTE -> new RaftMessage.Vote(term,in.readInt(),in.readLong());
                case VOTE_REPLY -> new RaftMessage.VoteReply(term,bool(in));
                case APPEND_REPLY -> new RaftMessage.AppendReply(term,in.readLong(),bool(in),in.readInt());
                case APPEND -> {
                    long rpc=in.readLong(); int prev=in.readInt(); long prevTerm=in.readLong(); int commit=in.readInt(),count=in.readInt();
                    if(count<0 || count>in.available()/9) throw new IllegalArgumentException("invalid Raft entry count");
                    var log=new ArrayList<RaftMessage.Entry>(count);
                    for(int i=0;i<count;i++) {
                        long entryTerm=in.readLong();
                        log.add(new RaftMessage.Entry(entryTerm,bool(in)?Optional.of(request(in)):Optional.empty()));
                    }
                    yield new RaftMessage.Append(term,rpc,prev,prevTerm,log,commit);
                }
                default -> throw new IllegalArgumentException("unknown Raft opcode");
            };
            if(in.available()!=0) throw new IllegalArgumentException("trailing Raft bytes");
            return new PeerEnvelope(from,to,m);
        } catch(IOException x) { throw new IllegalArgumentException("short Raft body",x); }
    }
    private static boolean bool(DataInputStream in) throws IOException {
        int b=in.readUnsignedByte(); if(b>1) throw new IllegalArgumentException("invalid Raft boolean"); return b==1;
    }
    private static RaftMessage.Request request(DataInputStream in) throws IOException {
        ClientId client=new ClientId(in.readLong()); OperationId op=new OperationId(in.readLong());
        Command c=switch(in.readUnsignedByte()) {
            case 1 -> new Command.Set(field(in),field(in)); case 2 -> new Command.Get(field(in));
            case 3 -> new Command.Delete(field(in)); case 4 -> new Command.Exists(field(in)); case 5 -> new Command.Count();
            default -> throw new IllegalArgumentException("unknown Raft command tag");
        };
        return new RaftMessage.Request(client,op,c);
    }
    private static Bytes field(DataInputStream in) throws IOException {
        int n=in.readInt(); if(n<0 || n>in.available()) throw new IllegalArgumentException("invalid Raft field length");
        return Bytes.copyOf(in.readNBytes(n));
    }
}
