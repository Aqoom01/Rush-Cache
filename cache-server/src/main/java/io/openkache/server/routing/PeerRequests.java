package io.openkache.server.routing;

import io.openkache.core.model.NodeId;
import java.util.concurrent.CompletableFuture;

/** Request/reply transport used by the migration source and its destination proxy. */
interface PeerRequests {
    boolean knows(NodeId node);
    CompletableFuture<PeerCaller.Reply> call(NodeId target, int opcode, byte[] body);
}
