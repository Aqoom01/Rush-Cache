package io.openkache.server.replication;

import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.raft.RaftMessage;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RaftWireTest {
    @Test void everyRpcCommandAndNoOpRoundTrip() {
        var log=new ArrayList<RaftMessage.Entry>(); log.add(new RaftMessage.Entry(1,Optional.empty()));
        List<Command> commands=List.of(new Command.Set(Bytes.utf8("k"),Bytes.utf8("v")),new Command.Get(Bytes.utf8("k")),
                new Command.Delete(Bytes.utf8("k")),new Command.Exists(Bytes.utf8("k")),new Command.Count());
        for(int i=0;i<commands.size();i++) log.add(new RaftMessage.Entry(2,Optional.of(new RaftMessage.Request(new ClientId(7),new OperationId(i),commands.get(i)))));
        List<RaftMessage> messages=List.of(new RaftMessage.Vote(2,6,2),new RaftMessage.VoteReply(2,true),new RaftMessage.VoteReply(2,false),
                new RaftMessage.Append(2,1,0,0,log,3),new RaftMessage.Append(2,2,6,2,List.of(),6),
                new RaftMessage.AppendReply(2,1,true,6),new RaftMessage.AppendReply(2,2,false,0));
        for(var m:messages) {
            var e=new PeerEnvelope(new NodeId(0),new NodeId(1),m); assertEquals(e,RaftWire.decode(RaftWire.encode(e)));
        }
    }
    @Test void rejectsInvalidCountBooleanLengthsAndOversizedEntry() {
        var empty=RaftWire.encode(new PeerEnvelope(new NodeId(0),new NodeId(1),new RaftMessage.Append(1,1,0,0,List.of(),0)));
        byte[] data=empty.body().toByteArray(); ByteBuffer.wrap(data).putInt(48,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,()->RaftWire.decode(new Frame(3,66,0,0,0,0,Bytes.copyOf(data))));
        var vote=RaftWire.encode(new PeerEnvelope(new NodeId(0),new NodeId(1),new RaftMessage.VoteReply(1,true)));
        byte[] bad=vote.body().toByteArray(); bad[24]=2;
        assertThrows(IllegalArgumentException.class,()->RaftWire.decode(new Frame(3,65,0,0,0,0,Bytes.copyOf(bad))));
        assertThrows(IllegalArgumentException.class,()->RaftWire.decode(new Frame(3,64,0,0,0,0,Bytes.copyOf(new byte[23]))));
        assertThrows(IllegalArgumentException.class,()->RaftWire.decode(new Frame(3,64,0,0,0,0,Bytes.copyOf(new byte[37]))));
        var huge=new RaftMessage.Entry(1,Optional.of(new RaftMessage.Request(new ClientId(1),new OperationId(1),new Command.Set(Bytes.utf8("k"),Bytes.copyOf(new byte[2097152])))));
        assertThrows(IllegalArgumentException.class,()->RaftWire.encode(new PeerEnvelope(new NodeId(0),new NodeId(1),new RaftMessage.Append(1,1,0,0,List.of(huge),0))));
    }
}
