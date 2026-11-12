package io.openkache.server.routing;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.TableMessage;
import io.openkache.core.tablet.routing.TableMessageCodec;
import io.openkache.server.netty.Frame;
import io.openkache.server.netty.Protocol;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * B1 라우팅 표 peer 확장(protocol-v1.md §6-2). frame body는 {@code from(u64), to(u64)} 뒤에
 * opcode별 내용이고, 표 직렬화는 {@link TableMessageCodec}(§6-3)를 그대로 쓴다.
 */
public final class B1Wire {

    public static final int TABLE_PUSH  = TableMessageCodec.OP_TABLE_PUSH;
    public static final int GET_TABLE   = TableMessageCodec.OP_GET_TABLE;
    public static final int TABLE_REPLY = TableMessageCodec.OP_TABLE_REPLY;
    /** 실험 제어(§6-4). controller 노드 전용이며 B1Server가 처리한다. */
    public static final int CONTROL = 8;
    /** 진단: 이 노드의 표를 §6-3 형식으로 돌려준다(빈 body). */
    public static final int TABLE_DUMP = 9;
    static final String STALE_EXPECTED_VERSION = "stale expected version";

    private B1Wire() {}

    /** 불변 peer envelope를 frame으로 변환한다. */
    public static Frame encode(PeerEnvelope envelope) {
        if (!(envelope.message() instanceof TableMessage message)) {
            throw new IllegalArgumentException("unsupported B1 message: " + envelope.message());
        }
        int opcode;
        byte[] payload;
        switch (message) {
            case TableMessage.TablePush p -> {
                opcode = TABLE_PUSH;
                payload = TableMessageCodec.encodeTable(p.table());
            }
            case TableMessage.GetTable g -> {
                opcode = GET_TABLE;
                payload = ByteBuffer.allocate(8).putLong(g.since().value()).array();
            }
            case TableMessage.TableReply r -> {
                opcode = TABLE_REPLY;
                byte[] table = r.table().map(TableMessageCodec::encodeTable).orElse(new byte[0]);
                ByteBuffer b = ByteBuffer.allocate(9 + table.length);
                b.putLong(r.current().value()).put((byte) (r.table().isPresent() ? 1 : 0)).put(table);
                payload = b.array();
            }
        }
        ByteBuffer body = ByteBuffer.allocate(16 + payload.length);
        body.putLong(envelope.from().value()).putLong(envelope.to().value()).put(payload);
        return new Frame(Protocol.CLASS_PEER, opcode, 0, 0, 0, 0, Bytes.copyOf(body.array()));
    }

    /** 잘못된 길이·opcode는 IllegalArgumentException으로 거절한다. */
    public static PeerEnvelope decode(Frame frame) {
        try {
            ByteBuffer b = ByteBuffer.wrap(frame.body().toByteArray());
            NodeId from = new NodeId(b.getLong());
            NodeId to = new NodeId(b.getLong());
            TableMessage message = switch (frame.opcode()) {
                case TABLE_PUSH -> new TableMessage.TablePush(readTable(b));
                case GET_TABLE -> new TableMessage.GetTable(new RoutingVersion(b.getLong()));
                case TABLE_REPLY -> {
                    long current = b.getLong();
                    int has = Byte.toUnsignedInt(b.get());
                    Optional<RoutingTable> table = switch (has) {
                        case 0 -> Optional.empty();
                        case 1 -> Optional.of(readTable(b));
                        default -> throw new IllegalArgumentException("invalid hasTable: " + has);
                    };
                    yield new TableMessage.TableReply(table, new RoutingVersion(current));
                }
                default -> throw new IllegalArgumentException("unknown B1 opcode: " + frame.opcode());
            };
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("trailing B1 bytes: " + b.remaining());
            }
            return new PeerEnvelope(from, to, message);
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("short B1 body", e);
        }
    }

    private static RoutingTable readTable(ByteBuffer b) {
        byte[] rest = new byte[b.remaining()];
        b.get(rest);
        RoutingTable table = TableMessageCodec.decodeTable(rest);
        // decodeTable이 읽고 남긴 바이트 수를 되돌릴 방법이 없으므로 표는 payload의 마지막 필드여야 한다 (§6-2)
        return table;
    }
}
