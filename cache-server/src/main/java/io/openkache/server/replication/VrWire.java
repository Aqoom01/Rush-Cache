package io.openkache.server.replication;

import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.vr.VrMessage;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.io.*;
import java.util.*;

/** VR 전용 bounded v1 codec. Java 객체 직렬화 없이 명시적인 길이와 tag를 사용한다. */
public final class VrWire {
    public static final int PREPARE=48, DELTA=49, COMMIT=50, PREPARE_OK=51,
            START_CHANGE=52, DO_CHANGE=53, START_VIEW=54, GET_STATE=55, CHANGE_ACK=56;
    // 기존 frame의 2MiB 한도에서 28-byte length/header를 제외한다.
    private static final int MAX_BODY=2*1024*1024-28;
    private VrWire() {}
    public static Frame encode(PeerEnvelope envelope) {
        if(!(envelope.message() instanceof VrMessage m)) throw new IllegalArgumentException("not VR");
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
            out.writeLong(envelope.from().value()); out.writeLong(envelope.to().value()); out.writeLong(m.view());
            int opcode=switch(m) {
                case VrMessage.Prepare x -> { log(out,x.commit(),x.log()); yield PREPARE; }
                case VrMessage.DeltaPrepare x -> { out.writeInt(x.firstOp()); log(out,x.commit(),x.entries()); yield DELTA; }
                case VrMessage.Commit x -> { out.writeInt(x.commit()); yield COMMIT; }
                case VrMessage.PrepareOk x -> { out.writeInt(x.length()); yield PREPARE_OK; }
                case VrMessage.StartViewChange x -> START_CHANGE;
                case VrMessage.DoViewChange x -> { out.writeLong(x.lastNormal()); log(out,x.commit(),x.log()); yield DO_CHANGE; }
                case VrMessage.StartView x -> { log(out,x.commit(),x.log()); yield START_VIEW; }
                case VrMessage.GetState x -> GET_STATE;
                case VrMessage.ViewChangeAck x -> CHANGE_ACK;
            };
            if(bytes.size()>MAX_BODY) throw new IllegalArgumentException("VR frame exceeds 2MiB; checkpoint/fragmentation required");
            return new Frame(Protocol.CLASS_PEER,opcode,0,0,0,0,Bytes.copyOf(bytes.toByteArray()));
        } catch(IOException e) { throw new IllegalArgumentException("VR encode",e); }
    }
    private static void log(DataOutputStream out,int commit,List<VrMessage.Request> log) throws IOException {
        out.writeInt(commit); out.writeInt(log.size());
        for(var r:log) {
            out.writeLong(r.client().value()); out.writeLong(r.operation().value());
            switch(r.command()) {
                case Command.Set c -> { out.writeByte(1); field(out,c.key()); field(out,c.value()); }
                case Command.Get c -> { out.writeByte(2); field(out,c.key()); }
                case Command.Delete c -> { out.writeByte(3); field(out,c.key()); }
                case Command.Exists c -> { out.writeByte(4); field(out,c.key()); }
                case Command.Count c -> out.writeByte(5);
            }
            if(out.size()>MAX_BODY) throw new IllegalArgumentException("VR log exceeds frame limit");
        }
    }
    private static void field(DataOutputStream out,Bytes value) throws IOException {
        if(value.size()>MAX_BODY-out.size()-4) throw new IllegalArgumentException("VR field exceeds frame limit");
        out.writeInt(value.size()); out.write(value.toByteArray());
    }
    public static PeerEnvelope decode(Frame f) {
        if(f.messageClass()!=Protocol.CLASS_PEER || f.flags()!=0 || f.status()!=0 || f.body().size()>MAX_BODY)
            throw new IllegalArgumentException("invalid VR frame");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(f.body().toByteArray()));
            NodeId from=new NodeId(in.readLong()),to=new NodeId(in.readLong()); long view=in.readLong();
            VrMessage m=switch(f.opcode()) {
                case PREPARE -> { int commit=in.readInt(); yield new VrMessage.Prepare(view,requests(in),commit); }
                case DELTA -> { int first=in.readInt(),commit=in.readInt(); yield new VrMessage.DeltaPrepare(view,first,requests(in),commit); }
                case COMMIT -> new VrMessage.Commit(view,in.readInt());
                case PREPARE_OK -> new VrMessage.PrepareOk(view,in.readInt());
                case START_CHANGE -> new VrMessage.StartViewChange(view);
                case DO_CHANGE -> { long last=in.readLong(); int commit=in.readInt(); yield new VrMessage.DoViewChange(view,last,requests(in),commit); }
                case START_VIEW -> { int commit=in.readInt(); yield new VrMessage.StartView(view,requests(in),commit); }
                case GET_STATE -> new VrMessage.GetState(view);
                case CHANGE_ACK -> new VrMessage.ViewChangeAck(view);
                default -> throw new IllegalArgumentException("unknown VR opcode");
            };
            if(in.available()!=0) throw new IllegalArgumentException("trailing VR bytes");
            return new PeerEnvelope(from,to,m);
        } catch(IOException e) { throw new IllegalArgumentException("short VR body",e); }
    }
    private static List<VrMessage.Request> requests(DataInputStream in) throws IOException {
        int count=in.readInt();
        if(count<0 || count>in.available()/17) throw new IllegalArgumentException("invalid VR request count");
        var requests=new ArrayList<VrMessage.Request>(count);
        for(int i=0;i<count;i++) {
            ClientId client=new ClientId(in.readLong()); OperationId op=new OperationId(in.readLong());
            Command command=switch(in.readUnsignedByte()) {
                case 1 -> new Command.Set(field(in),field(in));
                case 2 -> new Command.Get(field(in));
                case 3 -> new Command.Delete(field(in));
                case 4 -> new Command.Exists(field(in));
                case 5 -> new Command.Count();
                default -> throw new IllegalArgumentException("unknown VR command tag");
            };
            requests.add(new VrMessage.Request(client,op,command));
        }
        return List.copyOf(requests);
    }
    private static Bytes field(DataInputStream in) throws IOException {
        int n=in.readInt(); if(n<0 || n>in.available()) throw new IllegalArgumentException("invalid VR field length");
        return Bytes.copyOf(in.readNBytes(n));
    }
}
