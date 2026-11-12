package io.openkache.core.replication.rose;

import io.openkache.core.api.Command;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import io.openkache.core.spi.PeerMessage;
import java.util.Objects;

/** 단일 파티션 bounded-lag 실험 메시지. epoch는 소유권 세대이며 논문의 snapshot epoch와 다르다. */
public sealed interface RoseMessage extends PeerMessage {
    long epoch();
    record Request(ClientId client,OperationId operation,Command command) {
        public Request {
            Objects.requireNonNull(client); Objects.requireNonNull(operation); Objects.requireNonNull(command);
            if(!(command instanceof Command.Set) && !(command instanceof Command.Delete)) throw new IllegalArgumentException("write required");
        }
    }
    record Append(long epoch,int index,Request request) implements RoseMessage {
        public Append { if(epoch<1 || index<1) throw new IllegalArgumentException(); Objects.requireNonNull(request); }
    }
    record Applied(long epoch,int index) implements RoseMessage {
        public Applied { if(epoch<1 || index<0) throw new IllegalArgumentException(); }
    }
}
