package io.openkache.server.routing;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.routing.TableMessage.GetTable;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.routing.TableMessage.TableReply;
import io.openkache.server.netty.Frame;
import io.openkache.server.netty.Protocol;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class B1WireTest {

    private static final NodeId N0 = new NodeId(0);
    private static final NodeId N1 = new NodeId(1);
    private static final RoutingTable TABLE = new HashRangeScheme(2).initial(List.of(N0, N1));

    @Test
    void tablePushRoundTrip() {
        PeerEnvelope original = new PeerEnvelope(N0, N1, new TablePush(TABLE));
        Frame frame = B1Wire.encode(original);
        assertEquals(B1Wire.TABLE_PUSH, frame.opcode());
        assertEquals(Protocol.CLASS_PEER, frame.messageClass());

        PeerEnvelope decoded = B1Wire.decode(frame);
        assertEquals(N0, decoded.from());
        assertEquals(N1, decoded.to());
        TablePush push = (TablePush) decoded.message();
        assertEquals(TABLE.version(), push.table().version());
        assertEquals(TABLE.tablets(), push.table().tablets());
    }

    @Test
    void getTableRoundTrip() {
        PeerEnvelope original = new PeerEnvelope(N1, N0, new GetTable(TABLE.version()));
        PeerEnvelope decoded = B1Wire.decode(B1Wire.encode(original));
        assertEquals(TABLE.version(), ((GetTable) decoded.message()).since());
    }

    @Test
    void tableReplyWithTableRoundTrip() {
        PeerEnvelope original = new PeerEnvelope(N0, N1,
                new TableReply(Optional.of(TABLE), TABLE.version()));
        TableReply reply = (TableReply) B1Wire.decode(B1Wire.encode(original)).message();
        assertTrue(reply.table().isPresent());
        assertEquals(TABLE.tablets(), reply.table().get().tablets());
    }

    @Test
    void tableReplyEmptyRoundTrip() {
        PeerEnvelope original = new PeerEnvelope(N0, N1,
                new TableReply(Optional.empty(), new RoutingVersion(7)));
        TableReply reply = (TableReply) B1Wire.decode(B1Wire.encode(original)).message();
        assertTrue(reply.table().isEmpty());
        assertEquals(new RoutingVersion(7), reply.current());
    }

    @Test
    void unknownOpcodeIsRejected() {
        Frame bad = new Frame(Protocol.CLASS_PEER, 99, 0, 0, 0, 0,
                io.openkache.core.model.Bytes.copyOf(new byte[16]));
        assertThrows(IllegalArgumentException.class, () -> B1Wire.decode(bad));
    }
}
