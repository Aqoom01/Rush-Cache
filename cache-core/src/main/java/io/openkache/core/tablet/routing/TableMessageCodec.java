package io.openkache.core.tablet.routing;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import io.openkache.core.tablet.routing.TableMessage.GetTable;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.routing.TableMessage.TableReply;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * TableMessage와 라우팅 표의 wire 인코딩. docs/protocol-v1.md §6을 따른다.
 *
 * <ul>
 *   <li>opcode 5: TABLE_PUSH  — body = encodeTable(table)
 *   <li>opcode 6: GET_TABLE   — body = 8바이트 sinceVersion
 *   <li>opcode 7: TABLE_REPLY — body = 8바이트 currentVersion + 1바이트 hasTable + (optional) encodeTable
 * </ul>
 *
 * <p>상태가 없는 순수 함수 집합이다.
 */
public final class TableMessageCodec {

    public static final int OP_TABLE_PUSH  = 5;
    public static final int OP_GET_TABLE   = 6;
    public static final int OP_TABLE_REPLY = 7;

    private TableMessageCodec() {}

    // -----------------------------------------------------------------------
    // TableMessage encode
    // -----------------------------------------------------------------------

    /**
     * TableMessage를 wire body로 직렬화한다. 첫 1바이트가 opcode(5·6·7)이다.
     */
    public static byte[] encode(TableMessage message) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            switch (message) {
                case TablePush p -> {
                    out.writeByte(OP_TABLE_PUSH);
                    writeTable(out, p.table());
                }
                case GetTable g -> {
                    out.writeByte(OP_GET_TABLE);
                    out.writeLong(g.since().value());
                }
                case TableReply r -> {
                    out.writeByte(OP_TABLE_REPLY);
                    out.writeLong(r.current().value());
                    if (r.table().isPresent()) {
                        out.writeByte(1);
                        writeTable(out, r.table().get());
                    } else {
                        out.writeByte(0);
                    }
                }
            }
            return buf.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("encode failed", e);
        }
    }

    /**
     * wire body를 TableMessage로 역직렬화한다. 첫 1바이트가 opcode.
     *
     * @throws IllegalArgumentException 알 수 없는 opcode 또는 잘못된 형식
     */
    public static TableMessage decode(byte[] body) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            int op = in.readByte() & 0xFF;
            return switch (op) {
                case OP_TABLE_PUSH  -> new TablePush(readTable(in));
                case OP_GET_TABLE   -> new GetTable(new RoutingVersion(in.readLong()));
                case OP_TABLE_REPLY -> {
                    long current = in.readLong();
                    int has = in.readByte() & 0xFF;
                    Optional<RoutingTable> table =
                            has == 1 ? Optional.of(readTable(in)) : Optional.empty();
                    yield new TableReply(table, new RoutingVersion(current));
                }
                default -> throw new IllegalArgumentException("unknown opcode: " + op);
            };
        } catch (IOException e) {
            throw new IllegalArgumentException("decode failed", e);
        }
    }

    // -----------------------------------------------------------------------
    // RoutingTable encode (§6-3)
    // -----------------------------------------------------------------------

    /** RoutingTable을 §6-3 형식으로 직렬화한다. */
    public static byte[] encodeTable(RoutingTable table) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            writeTable(new DataOutputStream(buf), table);
            return buf.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("encodeTable failed", e);
        }
    }

    /** §6-3 형식의 바이트 배열을 RoutingTable로 역직렬화한다. */
    public static RoutingTable decodeTable(byte[] bytes) {
        try {
            return readTable(new DataInputStream(new ByteArrayInputStream(bytes)));
        } catch (IOException e) {
            throw new IllegalArgumentException("decodeTable failed", e);
        }
    }

    // -----------------------------------------------------------------------
    // MOVED redirect encode (§6-1)
    // -----------------------------------------------------------------------

    /**
     * redirect 주소를 §6-1 형식으로 직렬화한다. 주소가 없으면 hostLength=0 (port 생략).
     */
    public static byte[] encodeRedirect(Optional<NodeAddress> address) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            if (address.isEmpty()) {
                out.writeInt(0);
            } else {
                byte[] hostBytes = address.get().hostBytes();
                out.writeInt(hostBytes.length);
                out.write(hostBytes);
                out.writeShort(address.get().port());
            }
            return buf.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("encodeRedirect failed", e);
        }
    }

    /**
     * §6-1 형식의 바이트 배열에서 redirect 주소를 읽는다. hostLength=0이면 empty.
     */
    public static Optional<NodeAddress> decodeRedirect(byte[] bytes) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int hostLength = in.readInt();
            if (hostLength == 0) {
                return Optional.empty();
            }
            byte[] hostBytes = new byte[hostLength];
            in.readFully(hostBytes);
            int port = in.readUnsignedShort();
            return Optional.of(new NodeAddress(new String(hostBytes, StandardCharsets.UTF_8), port));
        } catch (IOException e) {
            throw new IllegalArgumentException("decodeRedirect failed", e);
        }
    }

    // -----------------------------------------------------------------------
    // private helpers
    // -----------------------------------------------------------------------

    private static void writeTable(DataOutputStream out, RoutingTable table) throws IOException {
        out.writeLong(table.version().value());
        out.writeLong(table.nextTabletId());
        List<Tablet> tablets = table.tablets();
        out.writeInt(tablets.size());
        for (Tablet t : tablets) {
            out.writeLong(t.id().value());
            out.writeLong(t.range().start());
            out.writeLong(t.range().end());
            out.writeLong(t.owner().value());
            out.writeLong(t.epoch().value());
        }
    }

    private static RoutingTable readTable(DataInputStream in) throws IOException {
        long version = in.readLong();
        long nextTabletId = in.readLong();
        int count = in.readInt();
        List<Tablet> tablets = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long tabletId = in.readLong();
            long start    = in.readLong();
            long end      = in.readLong();
            long ownerId  = in.readLong();
            long epoch    = in.readLong();
            tablets.add(new Tablet(
                    new TabletId(tabletId),
                    new TabletRange(start, end),
                    new NodeId(ownerId),
                    new OwnershipEpoch(epoch)));
        }
        return RoutingTable.of(new RoutingVersion(version), tablets, nextTabletId);
    }
}
