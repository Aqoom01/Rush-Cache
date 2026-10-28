package io.openkache.core.replication.ionia_second;

import io.openkache.core.replication.ionia.*;

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
import io.openkache.core.runtime.TimerEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class IoniaLeaderReadClientTest {
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
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
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
            try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
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
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
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
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
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
        IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK);
        CompletionStage<Result> result = client.execute(new Command.Get(SET.key()), CONTEXT);
        client.close();
        client.close();
        assertEquals(ResultStatus.UNAVAILABLE, result.toCompletableFuture().join().status());
        assertFalse(transport.reply.isDone());
        assertTrue(transport.records.isEmpty());
    }

    @Test
    void LR01_LR04_LR07_getAlwaysFollowsTheCurrentLeaderRatherThanTheInitialReplica() {
        ManualTransport transport = new ManualTransport();
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
            for (int view = 0; view < 3; view++) {
                transport.view = view;
                transport.reply = new CompletableFuture<>();
                NodeId leader = NODES.get(view);
                client.updateView(view, leader);
                CompletionStage<Result> read = client.execute(new Command.Get(SET.key()), CONTEXT);
                Result expected = view == 0 ? Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO)
                        : Result.ok(Bytes.utf8("view-" + view), 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
                transport.reply.complete(new IoniaClientTransport.Reply(leader, view, CLIENT, CONTEXT.operationId(), expected));
                assertEquals(expected, read.toCompletableFuture().join());
            }
            assertEquals(NODES, transport.executeTargets);
            assertEquals(9, transport.probes);
            assertEquals(0, transport.followerReads);
            assertEquals(0, transport.metaQueries);
        }
    }

    @Test
    void LR05_aDataReplyAloneCannotCertifyAnIsolatedOldLeader() {
        ManualTransport transport = new ManualTransport();
        transport.probeOverrides.put(NODES.get(1), CompletableFuture.completedFuture(
                new IoniaEngine.ViewInfo(1, NODES.get(1), IoniaEngine.Status.NORMAL)));
        transport.probeOverrides.put(NODES.get(2), CompletableFuture.completedFuture(
                new IoniaEngine.ViewInfo(1, NODES.get(1), IoniaEngine.Status.NORMAL)));
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
            CompletionStage<Result> read = client.execute(new Command.Get(SET.key()), CONTEXT);
            transport.reply.complete(new IoniaClientTransport.Reply(NODES.getFirst(), 0, CLIENT, CONTEXT.operationId(),
                    Result.ok(Bytes.utf8("stale"), 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO)));
            assertEquals(ResultStatus.UNAVAILABLE, read.toCompletableFuture().join().status());
        }
    }

    @Test
    void LR06_getWaitsForTheMajorityCertificateAndRejectsAnEndpointChangeInFlight() {
        for (boolean changeView : List.of(false, true)) {
            ManualTransport transport = new ManualTransport();
            NODES.forEach(node -> transport.probeOverrides.put(node, new CompletableFuture<>()));
            try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
                CompletionStage<Result> read = client.execute(new Command.Get(SET.key()), CONTEXT);
                transport.reply.complete(new IoniaClientTransport.Reply(NODES.getFirst(), 0, CLIENT, CONTEXT.operationId(),
                        Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO)));
                IoniaEngine.ViewInfo current = new IoniaEngine.ViewInfo(0, NODES.getFirst(), IoniaEngine.Status.NORMAL);
                transport.probeOverrides.get(NODES.getFirst()).complete(current);
                assertFalse(read.toCompletableFuture().isDone());
                if (changeView) client.updateView(1, NODES.get(1));
                transport.probeOverrides.get(NODES.get(1)).complete(current);
                assertEquals(changeView ? ResultStatus.UNAVAILABLE : ResultStatus.MISS, read.toCompletableFuture().join().status());
            }
        }
    }

    @Test
    void LR06_wrongNodeViewClientOrOperationCannotAuthorizeReadData() {
        for (int wrong = 0; wrong < 4; wrong++) {
            ManualTransport transport = new ManualTransport();
            try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
                CompletionStage<Result> read = client.execute(new Command.Get(SET.key()), CONTEXT);
                transport.reply.complete(new IoniaClientTransport.Reply(wrong == 0 ? NODES.getLast() : NODES.getFirst(),
                        wrong == 1 ? 1 : 0, wrong == 2 ? new ClientId(99) : CLIENT,
                        wrong == 3 ? new OperationId(99) : CONTEXT.operationId(),
                        Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO)));
                assertEquals(ResultStatus.BAD_REQUEST, read.toCompletableFuture().join().status());
            }
        }
    }

    @Test
    void LR06_lateDataReplyCannotReplaceATimedOutResult() {
        long[] now = {0};
        TimeSource clock = new TimeSource() {
            public long monotonicNanos() { return now[0]; }
            public long epochMillis() { return 0; }
        };
        ManualTransport transport = new ManualTransport();
        RequestContext context = new RequestContext(CLIENT, new OperationId(1), RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(10));
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, clock)) {
            CompletionStage<Result> read = client.execute(new Command.Get(SET.key()), context);
            now[0] = 10;
            client.onTimer(new TimerEvent(IoniaEngine.TICK, 10));
            assertEquals(ResultStatus.TIMEOUT, read.toCompletableFuture().join().status());
            transport.reply.complete(new IoniaClientTransport.Reply(NODES.getFirst(), 0, CLIENT, context.operationId(),
                    Result.ok(Bytes.utf8("late"), 1, RoutingVersion.ZERO, OwnershipEpoch.ZERO)));
            assertEquals(ResultStatus.TIMEOUT, read.toCompletableFuture().join().status());
        }
    }

    @Test
    void LR08_fiveNodeFastSuccessStillRequiresTheLeader() {
        List<NodeId> five = List.of(NODES.getFirst(), NODES.get(1), NODES.get(2), new NodeId(4), new NodeId(5));
        ManualTransport transport = new ManualTransport();
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, five, five.getFirst(), 0, transport, CLOCK)) {
            CompletionStage<Result> write = client.execute(SET, CONTEXT);
            for (NodeId follower : five.subList(1, 5)) transport.records.get(follower).complete(ack(follower, 0, 1));
            assertFalse(write.toCompletableFuture().isDone());
            assertTrue(transport.executeTargets.isEmpty());
            transport.records.get(five.getFirst()).complete(ack(five.getFirst(), 0, 1));
            assertEquals(ResultStatus.OK, write.toCompletableFuture().join().status());
        }
    }

    @Test
    void LR08_timerFallbackKeepsTheLogicalIdentityPayloadAndRemainingDeadline() {
        long[] now = {0};
        TimeSource clock = new TimeSource() {
            public long monotonicNanos() { return now[0]; }
            public long epochMillis() { return 0; }
        };
        ManualTransport transport = new ManualTransport();
        RequestContext context = new RequestContext(CLIENT, new OperationId(1), RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(100));
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, clock,
                new IoniaLeaderReadClient.Options(5))) {
            CompletionStage<Result> write = client.execute(SET, context);
            now[0] = 5;
            client.onTimer(new TimerEvent(IoniaEngine.TICK, 5));
            assertEquals(List.of(NODES.getFirst()), transport.executeTargets);
            IoniaClientTransport.Request slow = transport.executionRequests.getFirst();
            assertEquals(CLIENT, slow.clientId()); assertEquals(context.operationId(), slow.operationId());
            assertEquals(SET, slow.command()); assertEquals(95, slow.timeoutNanos());
            transport.reply.complete(new IoniaClientTransport.Reply(NODES.getFirst(), 0, CLIENT, context.operationId(),
                    Result.of(ResultStatus.OK, RoutingVersion.ZERO, OwnershipEpoch.ZERO)));
            assertEquals(ResultStatus.OK, write.toCompletableFuture().join().status());
        }
    }

    @Test
    void LR04_discoveryChoosesTheNewestNormalLeaderAndRejectsViewRollback() {
        ManualTransport transport = new ManualTransport();
        transport.probeOverrides.put(NODES.get(1), CompletableFuture.completedFuture(
                new IoniaEngine.ViewInfo(1, NODES.get(1), IoniaEngine.Status.NORMAL)));
        transport.probeOverrides.put(NODES.get(2), CompletableFuture.completedFuture(
                new IoniaEngine.ViewInfo(2, NODES.get(2), IoniaEngine.Status.RECOVERING)));
        try (IoniaLeaderReadClient client = new IoniaLeaderReadClient(CLIENT, NODES, NODES.getFirst(), 0, transport, CLOCK)) {
            assertEquals(NODES.get(1), client.refreshView().toCompletableFuture().join().leaderId());
            assertThrows(IllegalArgumentException.class, () -> client.updateView(0, NODES.getFirst()));
            assertThrows(IllegalArgumentException.class, () -> client.updateView(1, NODES.get(2)));
            client.execute(new Command.Get(SET.key()), CONTEXT);
            assertEquals(List.of(NODES.get(1)), transport.executeTargets);
        }
    }

    private static IoniaClientTransport.WriteAck ack(NodeId node, long view, long operation) {
        return new IoniaClientTransport.WriteAck(node, view, CLIENT, new OperationId(operation), ResultStatus.OK);
    }

    private static final class ManualTransport implements IoniaClientTransport {
        private long view;
        private int probes, followerReads, metaQueries;
        private final Map<NodeId, CompletableFuture<IoniaEngine.ViewInfo>> probeOverrides = new HashMap<>();
        @Override public CompletionStage<ReadReply> followerRead(NodeId to, Request request) {
            followerReads++; return CompletableFuture.failedFuture(new AssertionError("unexpected follower data read"));
        }
        @Override public CompletionStage<MetaReply> metaQuery(NodeId to, Request request) {
            metaQueries++; return CompletableFuture.failedFuture(new AssertionError("unexpected meta query"));
        }
        @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) {
            probes++;
            return probeOverrides.getOrDefault(to, CompletableFuture.completedFuture(
                    new IoniaEngine.ViewInfo(view, NODES.get((int) view % 3), IoniaEngine.Status.NORMAL)));
        }
        private final Map<NodeId, CompletableFuture<WriteAck>> records = new HashMap<>();
        private CompletableFuture<Reply> reply = new CompletableFuture<>();
        private final List<NodeId> executeTargets = new ArrayList<>();
        private final List<Request> executionRequests = new ArrayList<>();

        @Override public CompletionStage<WriteAck> record(NodeId to, Request request) {
            CompletableFuture<WriteAck> response = new CompletableFuture<>();
            records.put(to, response);
            return response;
        }
        @Override public CompletionStage<Reply> execute(NodeId to, Request request) {
            executeTargets.add(to);
            executionRequests.add(request);
            return reply;
        }
    }
}
