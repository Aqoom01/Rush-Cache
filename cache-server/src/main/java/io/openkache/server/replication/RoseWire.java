package io.openkache.server.replication;

import io.openkache.core.model.*;
import io.openkache.core.replication.rose.RoseMessage;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.io.*;

/** Rosé 단일 파티션 변형의 peer codec. prefix는 from/to/epoch이며 Append 하나에 요청 하나만 담는다. */
public final class RoseWire {
    public static final int APPEND=96,APPLIED=97;
    private RoseWire() {}
    public static Frame encode(PeerEnvelope e) {
        if(!(e.message() instanceof RoseMessage m)) throw new IllegalArgumentException("not Rose");
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
            out.writeLong(e.from().value()); out.writeLong(e.to().value()); out.writeLong(m.epoch());
            int opcode=switch(m) {
                case RoseMessage.Append x -> { out.writeInt(x.index()); RequestCodec.request(out,x.request().client(),x.request().operation(),x.request().command()); yield APPEND; }
                case RoseMessage.Applied x -> { out.writeInt(x.index()); yield APPLIED; }
            };
            if(bytes.size()>RequestCodec.MAX_BODY) throw new IllegalArgumentException("Rose frame exceeds limit");
            return new Frame(Protocol.CLASS_PEER,opcode,0,0,0,0,Bytes.copyOf(bytes.toByteArray()));
        } catch(IOException x) { throw new IllegalArgumentException("Rose encode",x); }
    }
    public static PeerEnvelope decode(Frame f) {
        if(f.messageClass()!=Protocol.CLASS_PEER || f.flags()!=0 || f.status()!=0 || f.body().size()>RequestCodec.MAX_BODY)
            throw new IllegalArgumentException("invalid Rose frame");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(f.body().toByteArray()));
            NodeId from=new NodeId(in.readLong()),to=new NodeId(in.readLong()); long epoch=in.readLong();
            RoseMessage m=switch(f.opcode()) {
                case APPEND -> { int index=in.readInt(); var r=RequestCodec.request(in); yield new RoseMessage.Append(epoch,index,new RoseMessage.Request(r.client(),r.operation(),r.command())); }
                case APPLIED -> new RoseMessage.Applied(epoch,in.readInt());
                default -> throw new IllegalArgumentException("unknown Rose opcode");
            };
            if(in.available()!=0) throw new IllegalArgumentException("trailing Rose bytes");
            return new PeerEnvelope(from,to,m);
        } catch(IOException x) { throw new IllegalArgumentException("short Rose body",x); }
    }
}
