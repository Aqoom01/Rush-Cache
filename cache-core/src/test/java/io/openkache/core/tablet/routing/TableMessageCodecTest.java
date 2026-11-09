package io.openkache.core.tablet.routing;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.TableMessage.GetTable;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.routing.TableMessage.TableReply;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** TableMessageCodec 단위 테스트. encode → decode 왕복과 redirect 인코딩을 검증한다. */
class TableMessageCodecTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final RoutingTable TABLE = new FixedSlotScheme(4).initial(List.of(N1, N2));

    @Test
    void tablePushRoundTrip() {
        TablePush original = new TablePush(TABLE);
        TableMessage decoded = TableMessageCodec.decode(TableMessageCodec.encode(original));
        assertInstanceOf(TablePush.class, decoded);
        TablePush back = (TablePush) decoded;
        assertEquals(TABLE.version(), back.table().version());
        assertEquals(TABLE.size(), back.table().size());
        assertEquals(TABLE.tablets(), back.table().tablets());
    }

    @Test
    void getTableRoundTrip() {
        GetTable original = new GetTable(RoutingVersion.ZERO);
        TableMessage decoded = TableMessageCodec.decode(TableMessageCodec.encode(original));
        assertInstanceOf(GetTable.class, decoded);
        assertEquals(RoutingVersion.ZERO, ((GetTable) decoded).since());
    }

    @Test
    void tableReplyWithTableRoundTrip() {
        TableReply original = new TableReply(Optional.of(TABLE), TABLE.version());
        TableMessage decoded = TableMessageCodec.decode(TableMessageCodec.encode(original));
        assertInstanceOf(TableReply.class, decoded);
        TableReply back = (TableReply) decoded;
        assertTrue(back.table().isPresent());
        assertEquals(TABLE.version(), back.table().get().version());
    }

    @Test
    void tableReplyEmptyRoundTrip() {
        TableReply original = new TableReply(Optional.empty(), TABLE.version());
        TableMessage decoded = TableMessageCodec.decode(TableMessageCodec.encode(original));
        assertInstanceOf(TableReply.class, decoded);
        assertTrue(((TableReply) decoded).table().isEmpty());
    }

    @Test
    void encodeDecodeTablePreservesAllFields() {
        byte[] bytes = TableMessageCodec.encodeTable(TABLE);
        RoutingTable restored = TableMessageCodec.decodeTable(bytes);
        assertEquals(TABLE.version(), restored.version());
        assertEquals(TABLE.nextTabletId(), restored.nextTabletId());
        assertEquals(TABLE.tablets(), restored.tablets());
    }

    @Test
    void encodeRedirectWithAddress() {
        NodeAddress addr = new NodeAddress("10.0.0.1", 7001);
        byte[] bytes = TableMessageCodec.encodeRedirect(Optional.of(addr));
        Optional<NodeAddress> decoded = TableMessageCodec.decodeRedirect(bytes);
        assertTrue(decoded.isPresent());
        assertEquals("10.0.0.1", decoded.get().host());
        assertEquals(7001, decoded.get().port());
    }

    @Test
    void encodeRedirectEmpty() {
        byte[] bytes = TableMessageCodec.encodeRedirect(Optional.empty());
        assertTrue(TableMessageCodec.decodeRedirect(bytes).isEmpty());
    }

    @Test
    void unknownOpcodeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> TableMessageCodec.decode(new byte[]{99}));
    }
}
