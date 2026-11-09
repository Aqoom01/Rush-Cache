package io.openkache.server.replication;

import io.openkache.core.model.*;
import io.openkache.core.replication.chain.ChainMessage;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.io.*;

/** Chain/CRAQ peer 메시지의 명시적 bounded codec. prefix는 from/to/epoch이며 2MiB frame 한도를 유지한다. */
public final class ChainWire {
    public static final int FORWARD=80,PROGRESS=81,ACK=82,COMPLETED=83,VERSION_QUERY=84,VERSION_REPLY=85;
    private ChainWire() {}
    public static Frame encode(PeerEnvelope e) {
        if(!(e.message() instanceof ChainMessage m)) throw new IllegalArgumentException("not Chain");
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
            out.writeLong(e.from().value()); out.writeLong(e.to().value()); out.writeLong(m.epoch());
            int opcode=switch(m) {
                case ChainMessage.Forward x -> { out.writeInt(x.index()); RequestCodec.request(out,x.request().client(),x.request().operation(),x.request().command()); yield FORWARD; }
                case ChainMessage.Progress x -> { out.writeInt(x.applied()); yield PROGRESS; }
                case ChainMessage.Ack x -> { out.writeInt(x.committed()); yield ACK; }
                case ChainMessage.Completed x -> {
                    out.writeInt(x.index()); out.writeLong(x.client().value()); out.writeLong(x.operation().value()); RequestCodec.result(out,x.result()); yield COMPLETED;
                }
                case ChainMessage.VersionQuery x -> { out.writeLong(x.queryId()); RequestCodec.field(out,x.key()); yield VERSION_QUERY; }
                case ChainMessage.VersionReply x -> { out.writeLong(x.queryId()); RequestCodec.field(out,x.key()); out.writeInt(x.version()); yield VERSION_REPLY; }
            };
            if(bytes.size()>RequestCodec.MAX_BODY) throw new IllegalArgumentException("Chain frame exceeds limit");
            return new Frame(Protocol.CLASS_PEER,opcode,0,0,0,0,Bytes.copyOf(bytes.toByteArray()));
        } catch(IOException x) { throw new IllegalArgumentException("Chain encode",x); }
    }
    public static PeerEnvelope decode(Frame f) {
        if(f.messageClass()!=Protocol.CLASS_PEER || f.flags()!=0 || f.status()!=0 || f.body().size()>RequestCodec.MAX_BODY)
            throw new IllegalArgumentException("invalid Chain frame");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(f.body().toByteArray()));
            NodeId from=new NodeId(in.readLong()),to=new NodeId(in.readLong()); long epoch=in.readLong();
            ChainMessage m=switch(f.opcode()) {
                case FORWARD -> { int index=in.readInt(); var r=RequestCodec.request(in); yield new ChainMessage.Forward(epoch,index,new ChainMessage.Request(r.client(),r.operation(),r.command())); }
                case PROGRESS -> new ChainMessage.Progress(epoch,in.readInt());
                case ACK -> new ChainMessage.Ack(epoch,in.readInt());
                case COMPLETED -> { int index=in.readInt(); var client=new ClientId(in.readLong()); var op=new OperationId(in.readLong()); yield new ChainMessage.Completed(epoch,index,client,op,RequestCodec.result(in)); }
                case VERSION_QUERY -> { long id=in.readLong(); yield new ChainMessage.VersionQuery(epoch,id,RequestCodec.field(in)); }
                case VERSION_REPLY -> { long id=in.readLong(); Bytes key=RequestCodec.field(in); yield new ChainMessage.VersionReply(epoch,id,key,in.readInt()); }
                default -> throw new IllegalArgumentException("unknown Chain opcode");
            };
            if(in.available()!=0) throw new IllegalArgumentException("trailing Chain bytes");
            return new PeerEnvelope(from,to,m);
        } catch(IOException x) { throw new IllegalArgumentException("short Chain body",x); }
    }
}
