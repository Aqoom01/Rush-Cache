package io.openkache.core.replication.ionia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.spi.TimeSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class IoniaClientTest {
    private static final List<NodeId> NODES = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
    private static final ClientId CLIENT = new ClientId(1000);
    private static final Command.Set SET = new Command.Set(Bytes.utf8("key"), Bytes.utf8("value"));
    private static final RequestContext CONTEXT = new RequestContext(CLIENT, new OperationId(1),
            RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(Long.MAX_VALUE));
    private static final TimeSource CLOCK = new TimeSource() {
        @Override public long monotonicNanos() { return 0; }
        @Override public long epochMillis() { return 0; }
    };

    @Test
    void twoRecordAcksAreInsufficientAndTheThreeRequestsAreStartedWithoutWaiting() {
        ManualTransport transport = new ManualTransport();
        try (IoniaClient client = new IoniaClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
            CompletionStage<Result> result = client.execute(SET, CONTEXT);
            assertEquals(3, transport.records.size());
            for (int i = 0; i < 2; i++) {
                NodeId node = NODES.get(i);
                transport.records.get(node).complete(ack(node, 0, 1));
            }
            assertFalse(result.toCompletableFuture().isDone());
            NodeId last = NODES.getLast();
            transport.records.get(last).complete(ack(last, 0, 1));
            assertEquals(ResultStatus.OK, result.toCompletableFuture().join().status());
        }
    }

    @Test
    void wrongViewAndWrongOperationCannotProduceSuccessfulWrites() {
        for (boolean wrongView : List.of(true, false)) {
            ManualTransport transport = new ManualTransport();
            try (IoniaClient client = new IoniaClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
                CompletionStage<Result> result = client.execute(SET, CONTEXT);
                NodeId first = NODES.getFirst();
                transport.records.get(first).complete(ack(first, wrongView ? 1 : 0, wrongView ? 1 : 2));
                assertEquals(ResultStatus.BAD_REQUEST, result.toCompletableFuture().join().status());
            }
        }
    }

    @Test
    void failedFastCallUsesFallbackAndDoesNotTreatLocalSendAsRemoteAck() {
        ManualTransport transport = new ManualTransport();
        try (IoniaClient client = new IoniaClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
            CompletionStage<Result> result = client.execute(SET, CONTEXT);
            transport.records.get(NODES.getFirst()).completeExceptionally(new IllegalStateException("disconnected"));
            transport.reply.complete(new IoniaClientTransport.Reply(NODES.getFirst(), 0, CLIENT, new OperationId(1),
                    Result.of(ResultStatus.UNAVAILABLE, RoutingVersion.ZERO, OwnershipEpoch.ZERO)));
            assertEquals(ResultStatus.UNAVAILABLE, result.toCompletableFuture().join().status());
        }
    }

    @Test
    void leaderRepliesMustMatchTheRequestedNodeAndOperation() {
        ManualTransport transport = new ManualTransport();
        try (IoniaClient client = new IoniaClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
            CompletionStage<Result> result = client.execute(new Command.Get(SET.key()), CONTEXT);
            assertEquals(List.of(NODES.getFirst()), transport.executeTargets);
            transport.reply.complete(new IoniaClientTransport.Reply(NODES.getLast(), 0, CLIENT, new OperationId(1),
                    Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO)));
            assertEquals(ResultStatus.BAD_REQUEST, result.toCompletableFuture().join().status());
        }
    }

    @Test
    void requestAndReplyTypesRejectMissingIdentityAndNegativeTimeouts() {
        assertThrows(IllegalArgumentException.class, () -> new IoniaClientTransport.Request(
                0, SET, CLIENT, new OperationId(1), RoutingVersion.ZERO, OwnershipEpoch.ZERO, -1));
        assertThrows(IllegalArgumentException.class, () -> ack(NODES.getFirst(), -1, 1));
        assertThrows(NullPointerException.class, () -> ack(null, 0, 1));
        assertThrows(NullPointerException.class, () -> new IoniaClientTransport.Reply(
                NODES.getFirst(), 0, CLIENT, new OperationId(1), null));
    }

    @Test
    void closeCompletesPendingLeaderCallsAndDoesNotCloseTheBorrowedTransport() {
        ManualTransport transport = new ManualTransport();
        IoniaClient client = new IoniaClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK);
        CompletionStage<Result> result = client.execute(new Command.Get(SET.key()), CONTEXT);
        client.close();
        client.close();
        assertEquals(ResultStatus.UNAVAILABLE, result.toCompletableFuture().join().status());
        assertFalse(transport.reply.isDone());
        assertTrue(transport.records.isEmpty());
    }

    private static IoniaClientTransport.WriteAck ack(NodeId node, long view, long operation) {
        return new IoniaClientTransport.WriteAck(node, view, CLIENT, new OperationId(operation), ResultStatus.OK);
    }

    private static final class ManualTransport implements IoniaClientTransport {
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) {
            return CompletableFuture.completedFuture(new IoniaEngine.ViewInfo(0, NODES.getFirst(), IoniaEngine.Status.NORMAL));
        }
        private final Map<NodeId, CompletableFuture<WriteAck>> records = new HashMap<>();
        private final CompletableFuture<Reply> reply = new CompletableFuture<>();
        private final List<NodeId> executeTargets = new ArrayList<>();

        @Override public CompletionStage<WriteAck> record(NodeId to, Request request) {
            CompletableFuture<WriteAck> response = new CompletableFuture<>();
            records.put(to, response);
            return response;
        }
        @Override public CompletionStage<Reply> execute(NodeId to, Request request) {
            executeTargets.add(to);
            return reply;
        }
    }
}
