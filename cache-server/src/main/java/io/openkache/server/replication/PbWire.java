package io.openkache.server.replication;

import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.nio.ByteBuffer;

/** PB 전용 v1 확장. length/header는 기존 Frame codec, body 정수는 big-endian이다. */
public final class PbWire {
    public static final int REPLICATE=32, ACK=33, CONTROL=34, STATUS=35, LOCAL_GET=36, LOG_DIGEST=37, MASTER_CONTROL=38, FENCE=39;
    private PbWire() {}
    /** 불변 peer envelope를 frame으로 변환한다. 동일 연결에서 write 순서를 유지한다. */
    public static Frame encode(PeerEnvelope envelope) {
        ByteBuffer b; int opcode;
        if(envelope.message() instanceof PrimaryBackupMessage.Replicate r) {
            opcode=REPLICATE; var s=r.command();
            b=ByteBuffer.allocate(40+s.key().size()+s.value().size()); prefix(b,envelope,r.epoch());
            b.putLong(r.opNumber()).putInt(s.key().size()).put(s.key().toByteArray()).putInt(s.value().size()).put(s.value().toByteArray());
        } else if(envelope.message() instanceof PrimaryBackupMessage.ReplicateAck a) {
            opcode=ACK; b=ByteBuffer.allocate(32); prefix(b,envelope,a.epoch()); b.putLong(a.opNumber());
        } else if(envelope.message() instanceof CrashOnlyPrimaryBackupEngine.Control c) {
            opcode=CONTROL; b=ByteBuffer.allocate(25); prefix(b,envelope,c.epoch()); b.put((byte)c.kind().ordinal());
        } else if(envelope.message() instanceof PrimaryBackupFence.Message m) {
            opcode=FENCE; b=ByteBuffer.allocate(41+m.digest().size()); prefix(b,envelope,m.epoch());
            b.put((byte)m.kind().ordinal()).putLong(m.round()).putLong(m.durationNanos()).put(m.digest().toByteArray());
        } else throw new IllegalArgumentException("unsupported PB message");
        return new Frame(Protocol.CLASS_PEER,opcode,0,0,0,0,Bytes.copyOf(b.array()));
    }
    private static void prefix(ByteBuffer b,PeerEnvelope e,OwnershipEpoch epoch) {
        b.putLong(e.from().value()).putLong(e.to().value()).putLong(epoch.value());
    }
    /** 부정확한 길이/ID/enum/trailing bytes는 IllegalArgumentException으로 거절한다. */
    public static PeerEnvelope decode(Frame frame) {
        try {
            if(frame.messageClass()!=Protocol.CLASS_PEER || frame.flags()!=0 || frame.status()!=0) throw new IllegalArgumentException("invalid peer header");
            ByteBuffer b=ByteBuffer.wrap(frame.body().toByteArray()); NodeId from=new NodeId(b.getLong()),to=new NodeId(b.getLong());
            OwnershipEpoch epoch=new OwnershipEpoch(b.getLong()); PeerMessage message;
            switch(frame.opcode()) {
                case REPLICATE -> { long op=b.getLong(); message=new PrimaryBackupMessage.Replicate(epoch,op,new Command.Set(bytes(b),bytes(b))); }
                case ACK -> message=new PrimaryBackupMessage.ReplicateAck(epoch,b.getLong());
                case CONTROL -> { int kind=Byte.toUnsignedInt(b.get()); if(kind>=CrashOnlyPrimaryBackupEngine.Kind.values().length) throw new IllegalArgumentException("unknown control");
                    message=new CrashOnlyPrimaryBackupEngine.Control(epoch,CrashOnlyPrimaryBackupEngine.Kind.values()[kind]); }
                case FENCE -> {
                    int kind=Byte.toUnsignedInt(b.get());
                    if(kind>=PrimaryBackupFence.Kind.values().length) throw new IllegalArgumentException("unknown fencing kind");
                    var type=PrimaryBackupFence.Kind.values()[kind];
                    long round=b.getLong(), duration=b.getLong();
                    byte[] digest=new byte[type==PrimaryBackupFence.Kind.VOTE?32:0];
                    b.get(digest);
                    message=new PrimaryBackupFence.Message(epoch,type,round,duration,Bytes.copyOf(digest));
                }
                default -> throw new IllegalArgumentException("unknown PB opcode");
            }
            if(b.hasRemaining()) throw new IllegalArgumentException("trailing PB bytes"); return new PeerEnvelope(from,to,message);
        } catch(java.nio.BufferUnderflowException e) { throw new IllegalArgumentException("short PB body",e); }
    }
    private static Bytes bytes(ByteBuffer b) {
        int length=b.getInt(); if(length<0 || length>b.remaining()) throw new IllegalArgumentException("invalid field length");
        byte[] data=new byte[length]; b.get(data); return Bytes.copyOf(data);
    }
}
