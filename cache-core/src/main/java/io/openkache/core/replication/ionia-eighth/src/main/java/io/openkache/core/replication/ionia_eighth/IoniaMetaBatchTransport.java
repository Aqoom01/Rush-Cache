package io.openkache.core.replication.ionia_eighth;

import io.openkache.core.api.Command;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaClientTransport;
import java.util.*;
import java.util.concurrent.CompletionStage;

/** One stage is one complete, bounded response envelope. No cached Meta or shared read certificate. */
public interface IoniaMetaBatchTransport extends IoniaClientTransport {
    record Item(long token, Request request) {
        public Item { if (token <= 0) throw new IllegalArgumentException("nonpositive token"); Objects.requireNonNull(request);
            if (!(request.command() instanceof Command.Get)) throw new IllegalArgumentException("Meta is GET only"); }
        /** Exact encoded item size: token + six longs + GET tag + key length + key. */
        public long encodedBytes() { return 61L + request.command().key().size(); }
    }
    record Answer(long token, MetaReply reply) {
        public Answer { if (token <= 0) throw new IllegalArgumentException("nonpositive token"); Objects.requireNonNull(reply); }
    }
    CompletionStage<List<Answer>> metaBatch(NodeId to, List<Item> items);
}
