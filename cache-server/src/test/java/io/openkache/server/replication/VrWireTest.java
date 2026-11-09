package io.openkache.server.replication;

import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.vr.VrMessage;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class VrWireTest {
    @Test void allMessagesAndCommandsRoundTrip() {
        var commands=List.of(new Command.Set(Bytes.utf8("k"),Bytes.utf8("v")),new Command.Get(Bytes.utf8("k")),
                new Command.Delete(Bytes.utf8("k")),new Command.Exists(Bytes.utf8("k")),new Command.Count());
        var log=new ArrayList<VrMessage.Request>();
        for(int i=0;i<commands.size();i++) log.add(new VrMessage.Request(new ClientId(9),new OperationId(i),commands.get(i)));
        List<VrMessage> messages=List.of(new VrMessage.Prepare(1,log,2),new VrMessage.DeltaPrepare(1,1,log,2),
                new VrMessage.Commit(1,2),new VrMessage.PrepareOk(1,5),new VrMessage.StartViewChange(1),
                new VrMessage.DoViewChange(1,0,log,2),new VrMessage.StartView(1,log,2),new VrMessage.GetState(1),new VrMessage.ViewChangeAck(1));
        for(var m:messages) {
            var e=new PeerEnvelope(new NodeId(0),new NodeId(1),m);
            assertEquals(e,VrWire.decode(VrWire.encode(e)));
        }
    }
    @Test void rejectsUnboundedCountTruncationTrailingBytesAndOversizedLog() {
        var e=new PeerEnvelope(new NodeId(0),new NodeId(1),new VrMessage.Prepare(0,List.of(),0));
        Frame f=VrWire.encode(e); byte[] bytes=f.body().toByteArray();
        java.nio.ByteBuffer.wrap(bytes).putInt(28,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,()->VrWire.decode(new Frame(3,48,0,0,0,0,Bytes.copyOf(bytes))));
        assertThrows(IllegalArgumentException.class,()->VrWire.decode(new Frame(3,50,0,0,0,0,Bytes.copyOf(new byte[23]))));
        assertThrows(IllegalArgumentException.class,()->VrWire.decode(new Frame(3,48,0,0,0,0,Bytes.copyOf(new byte[33]))));
        var huge=new VrMessage.Request(new ClientId(0),new OperationId(0),new Command.Set(Bytes.utf8("k"),Bytes.copyOf(new byte[2*1024*1024])));
        assertThrows(IllegalArgumentException.class,()->VrWire.encode(new PeerEnvelope(new NodeId(0),new NodeId(1),new VrMessage.StartView(1,List.of(huge),0))));
    }
}
