package io.openkache.server.replication;

import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PbWireTest {
    @Test void allMessagesRoundTrip() {
        List<PeerMessage> messages=new ArrayList<>();
        messages.add(new PrimaryBackupMessage.Replicate(new OwnershipEpoch(1),7,new Command.Set(Bytes.utf8("k"),Bytes.utf8("value"))));
        messages.add(new PrimaryBackupMessage.ReplicateAck(new OwnershipEpoch(1),7));
        for(var kind:CrashOnlyPrimaryBackupEngine.Kind.values()) messages.add(new CrashOnlyPrimaryBackupEngine.Control(new OwnershipEpoch(2),kind));
        for(var message:messages) {
            var e=new PeerEnvelope(new NodeId(0),new NodeId(1),message); assertEquals(e,PbWire.decode(PbWire.encode(e)));
        }
    }
    @Test void corruptLengthKindAndTrailingBytesRejected() {
        var f=PbWire.encode(new PeerEnvelope(new NodeId(0),new NodeId(1),
                new CrashOnlyPrimaryBackupEngine.Control(new OwnershipEpoch(1),CrashOnlyPrimaryBackupEngine.Kind.HEARTBEAT)));
        byte[] data=f.body().toByteArray(); data[24]=99;
        assertThrows(IllegalArgumentException.class,()->PbWire.decode(new Frame(3,34,0,0,0,0,Bytes.copyOf(data))));
        assertThrows(IllegalArgumentException.class,()->PbWire.decode(new Frame(3,33,0,0,0,0,Bytes.empty())));
        assertThrows(IllegalArgumentException.class,()->PbWire.decode(new Frame(3,34,0,0,0,0,Bytes.copyOf(Arrays.copyOf(f.body().toByteArray(),26)))));
    }
    @Test void invalidNodeConfigFailsBeforeBind() {
        assertThrows(IllegalArgumentException.class,()->new PbNodeConfig(3,List.of("a","b","c"),7000,7001,0,50,500,10));
        assertThrows(IllegalArgumentException.class,()->new PbNodeConfig(0,List.of("a","b","c"),7000,7000,0,50,500,10));
    }
}
