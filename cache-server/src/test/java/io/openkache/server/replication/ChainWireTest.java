package io.openkache.server.replication;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.chain.ChainMessage;
import io.openkache.core.replication.rose.RoseMessage;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ChainWireTest {
    @Test void everyChainAndRoseMessageRoundTrips() {
        var set=new ChainMessage.Request(new ClientId(7),new OperationId(3),new Command.Set(Bytes.utf8("k"),Bytes.utf8("v")));
        var delete=new ChainMessage.Request(new ClientId(7),new OperationId(4),new Command.Delete(Bytes.utf8("k")));
        List<Result> results=List.of(Result.ok(Bytes.utf8("v"),3,RoutingVersion.ZERO,new OwnershipEpoch(1)),
                Result.of(ResultStatus.MISS,RoutingVersion.ZERO,new OwnershipEpoch(2)),
                Result.redirect(ResultStatus.NOT_PRIMARY,new Redirect(new NodeId(2)),new RoutingVersion(5),new OwnershipEpoch(1)));
        var messages=new ArrayList<ChainMessage>(List.of(new ChainMessage.Forward(1,1,set),new ChainMessage.Forward(2,9,delete),
                new ChainMessage.Progress(1,0),new ChainMessage.Ack(1,7),new ChainMessage.VersionQuery(1,5,Bytes.utf8("k")),
                new ChainMessage.VersionReply(1,5,Bytes.utf8("k"),0),new ChainMessage.VersionReply(3,6,Bytes.copyOf(new byte[0]),12)));
        for(int i=0;i<results.size();i++) messages.add(new ChainMessage.Completed(1,i+1,new ClientId(7),new OperationId(i),results.get(i)));
        for(var m:messages) {
            var e=new PeerEnvelope(new NodeId(0),new NodeId(1),m); assertEquals(e,ChainWire.decode(ChainWire.encode(e)));
        }
        var rose=new RoseMessage.Request(new ClientId(7),new OperationId(3),new Command.Set(Bytes.utf8("k"),Bytes.utf8("v")));
        for(RoseMessage m:List.of(new RoseMessage.Append(1,1,rose),new RoseMessage.Applied(2,0))) {
            var e=new PeerEnvelope(new NodeId(2),new NodeId(0),m); assertEquals(e,RoseWire.decode(RoseWire.encode(e)));
        }
    }
    @Test void rejectsForeignTypesTrailingBytesBadTagsAndOversizedFields() {
        var chain=new PeerEnvelope(new NodeId(0),new NodeId(1),new ChainMessage.Ack(1,1));
        assertThrows(IllegalArgumentException.class,()->RoseWire.encode(chain));
        var rose=new PeerEnvelope(new NodeId(0),new NodeId(1),new RoseMessage.Applied(1,1));
        assertThrows(IllegalArgumentException.class,()->ChainWire.encode(rose));
        var ack=ChainWire.encode(chain); byte[] extra=Arrays.copyOf(ack.body().toByteArray(),ack.body().size()+1);
        assertThrows(IllegalArgumentException.class,()->ChainWire.decode(new Frame(3,ChainWire.ACK,0,0,0,0,Bytes.copyOf(extra))));
        assertThrows(IllegalArgumentException.class,()->ChainWire.decode(new Frame(3,ChainWire.ACK,0,0,0,0,Bytes.copyOf(new byte[27]))));
        assertThrows(IllegalArgumentException.class,()->ChainWire.decode(new Frame(3,90,0,0,0,0,ack.body())));
        var completed=ChainWire.encode(new PeerEnvelope(new NodeId(2),new NodeId(0),
                new ChainMessage.Completed(1,1,new ClientId(1),new OperationId(1),Result.of(ResultStatus.OK,RoutingVersion.ZERO,new OwnershipEpoch(1)))));
        byte[] badStatus=completed.body().toByteArray(); badStatus[24+4+16]=(byte)200;
        assertThrows(IllegalArgumentException.class,()->ChainWire.decode(new Frame(3,ChainWire.COMPLETED,0,0,0,0,Bytes.copyOf(badStatus))));
        var huge=new ChainMessage.Request(new ClientId(1),new OperationId(1),new Command.Set(Bytes.utf8("k"),Bytes.copyOf(new byte[2097152])));
        assertThrows(IllegalArgumentException.class,()->ChainWire.encode(new PeerEnvelope(new NodeId(0),new NodeId(1),new ChainMessage.Forward(1,1,huge))));
        var appended=RoseWire.encode(new PeerEnvelope(new NodeId(0),new NodeId(1),new RoseMessage.Append(1,1,
                new RoseMessage.Request(new ClientId(1),new OperationId(1),new Command.Delete(Bytes.utf8("k"))))));
        byte[] badTag=appended.body().toByteArray(); badTag[24+4+16]=9;
        assertThrows(IllegalArgumentException.class,()->RoseWire.decode(new Frame(3,RoseWire.APPEND,0,0,0,0,Bytes.copyOf(badTag))));
    }
}
