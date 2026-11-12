package io.openkache.core.failure;

import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerMessage;
import java.util.List;
import java.util.Objects;

/** SWIM failure detector가 주고받는 불변 peer 메시지. */
public sealed interface SwimMessage extends PeerMessage
        permits SwimMessage.Ping, SwimMessage.Ack, SwimMessage.PingRequest {

    // 어떤 probe의 메세지인지
    long probeId();

    // piggyback하는 멤버쉽 목록
    List<MembershipUpdate> updates();

    /** target이 ACK를 {@code from}에게 보내고, 필요하면 {@code origin}까지 relay하게 하는 probe. */
    /** origin이 있는 이유는, 만약에 ping-req에 의해 요청받은 ping이라면, ping-req를 보낸 origin에게도 ack을 받았다는 ack을 보내줘야하기 때문이다.*/
    record Ping(long probeId, NodeId origin, List<MembershipUpdate> updates) implements SwimMessage {

        public Ping {
            requireProbeId(probeId);
            Objects.requireNonNull(origin, "origin");
            updates = List.copyOf(Objects.requireNonNull(updates, "updates"));
        }
    }

    /** direct ACK 또는 helper가 origin으로 relay하는 indirect ACK. */
    record Ack(long probeId, NodeId target, NodeId origin, List<MembershipUpdate> updates)
            implements SwimMessage {

        public Ack {
            requireProbeId(probeId);
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(origin, "origin");
            updates = List.copyOf(Objects.requireNonNull(updates, "updates"));
        }
    }

    /** helper에게 {@code target}을 대신 probe하도록 요청한다. */
    record PingRequest(long probeId, NodeId origin, NodeId target, List<MembershipUpdate> updates)
            implements SwimMessage {

        public PingRequest {
            requireProbeId(probeId);
            Objects.requireNonNull(origin, "origin");
            Objects.requireNonNull(target, "target");
            updates = List.copyOf(Objects.requireNonNull(updates, "updates"));
        }
    }

    private static void requireProbeId(long probeId) {
        if (probeId < 0) {
            throw new IllegalArgumentException("probeId must be >= 0: " + probeId);
        }
    }
}
