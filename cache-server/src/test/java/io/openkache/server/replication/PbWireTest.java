package io.openkache.server.replication;

import io.openkache.core.api.Command;
import io.openkache.core.failure.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.spi.*;
import io.openkache.server.netty.*;
import io.openkache.server.failure.codec.SwimMessageCodec;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PbWireTest {
    @Test void allMessagesRoundTrip() {
        List<PeerMessage> messages=new ArrayList<>();
        messages.add(new PrimaryBackupMessage.Replicate(new OwnershipEpoch(1),7,new Command.Set(Bytes.utf8("k"),Bytes.utf8("value"))));
        messages.add(new PrimaryBackupMessage.ReplicateAck(new OwnershipEpoch(1),7));
        for(var kind:CrashOnlyPrimaryBackupEngine.Kind.values()) messages.add(new CrashOnlyPrimaryBackupEngine.Control(new OwnershipEpoch(2),kind));
        for (var kind : PrimaryBackupFence.Kind.values()) {
            boolean lease = kind == PrimaryBackupFence.Kind.ACQUIRE || kind == PrimaryBackupFence.Kind.GRANT;
            messages.add(new PrimaryBackupFence.Message(new OwnershipEpoch(2), kind, lease ? 5 : 0,
                    lease ? 500_000_000 : 0, kind == PrimaryBackupFence.Kind.VOTE ? Bytes.copyOf(new byte[32]) : Bytes.empty()));
        }
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
    @Test void swimMessagesRoundTripAndCorruptionIsRejected() {
        var update=new MembershipUpdate(new NodeId(0),3,MemberStatus.SUSPECT,new NodeId(1));
        List<PeerMessage> messages=List.of(
                new SwimMessage.Ping(7,new NodeId(1),List.of(update)),
                new SwimMessage.Ack(8,new NodeId(0),new NodeId(1),List.of()),
                new SwimMessage.PingRequest(9,new NodeId(1),new NodeId(0),List.of(update)));
        for(var message:messages) {
            var envelope=new PeerEnvelope(new NodeId(1),new NodeId(2),message); byte[] encoded=SwimMessageCodec.encode(envelope);
            assertEquals(envelope,SwimMessageCodec.decode(encoded));
        }
        byte[] invalid=SwimMessageCodec.encode(new PeerEnvelope(new NodeId(1),new NodeId(2),messages.getFirst()));
        invalid[4]=0;
        assertThrows(SwimMessageCodec.MalformedSwimMessageException.class,()->SwimMessageCodec.decode(invalid));
        assertThrows(SwimMessageCodec.MalformedSwimMessageException.class,()->SwimMessageCodec.decode(new byte[49]));
    }
    @Test void invalidNodeConfigFailsBeforeBind() {
        assertThrows(IllegalArgumentException.class,()->new PbNodeConfig(3,List.of("a","b","c"),7000,7001,0,50,500,10));
        assertThrows(IllegalArgumentException.class,()->new PbNodeConfig(0,List.of("a","b","c"),7000,7000,0,50,500,10));
    }
    @Test void truncatedFencingVoteIsRejected() {
        var vote = new PrimaryBackupFence.Message(new OwnershipEpoch(2), PrimaryBackupFence.Kind.VOTE,
                0, 0, Bytes.copyOf(new byte[32]));
        var frame = PbWire.encode(new PeerEnvelope(new NodeId(2), new NodeId(1), vote));
        byte[] shortBody = Arrays.copyOf(frame.body().toByteArray(), frame.body().size() - 1);
        assertThrows(IllegalArgumentException.class,
                () -> PbWire.decode(new Frame(3, PbWire.FENCE, 0, 0, 0, 0, Bytes.copyOf(shortBody))));
    }
    @Test void swimNodePropertiesAreLoadedAndRestrictedToSupportedProtocols() throws Exception {
        Path file=Files.createTempFile("openkache-swim-",".properties");
        try {
            Files.writeString(file,"node.id=1\npeer.0=127.0.0.1\npeer.1=127.0.0.2\npeer.2=127.0.0.3\n"
                    +"protocol=PB\nfailure.detector=SWIM\nfailure.port=7012\nfailure.startup.grace.ms=25\n"
                    +"failure.probe.interval.ms=100\nfailure.direct.timeout.ms=25\nfailure.indirect.timeout.ms=25\nfailure.suspicion.ms=250\n");
            PbNodeConfig config=PbNodeConfig.load(file);
            assertTrue(config.swimFailureDetector()); assertFalse(config.lifeguardFailureDetector());
            assertEquals(7012,config.failurePort()); assertEquals(25,config.failureStartupGraceMs());
            Files.writeString(file,Files.readString(file).replace("failure.detector=SWIM","failure.detector=SWIM_LIFEGUARD")
                    +"failure.lifeguard.awareness.max.multiplier=6\nfailure.lifeguard.suspicion.max.multiplier=3\nfailure.lifeguard.max.confirmations=1\n");
            PbNodeConfig lifeguard=PbNodeConfig.load(file);
            assertTrue(lifeguard.lifeguardFailureDetector()); assertEquals(6,lifeguard.failureAwarenessMaxMultiplier());
            Files.writeString(file,Files.readString(file).replace("protocol=PB","protocol=PB_FENCED"));
            assertEquals("PB_FENCED",PbNodeConfig.load(file).protocol());
            Files.writeString(file,Files.readString(file).replace("protocol=PB_FENCED","protocol=RAFT"));
            assertEquals("RAFT",PbNodeConfig.load(file).protocol());
            Files.writeString(file,Files.readString(file).replace("protocol=RAFT","protocol=VR"));
            assertThrows(IllegalArgumentException.class,()->PbNodeConfig.load(file));
            Files.writeString(file,"node.id=1\npeer.0=127.0.0.1\npeer.1=127.0.0.2\npeer.2=127.0.0.3\nprotocol=PB_FENCED\n");
            assertEquals("SWIM",PbNodeConfig.load(file).failureDetector());
            Files.writeString(file,Files.readString(file)+"failure.detector=FIXED_TIMEOUT\n");
            assertThrows(IllegalArgumentException.class,()->PbNodeConfig.load(file));
        } finally { Files.deleteIfExists(file); }
    }
}
