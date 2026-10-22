package io.openkache.core.replication.ionia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.TimeSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 가상 시계/수동 전달로 기록 ACK, 순서 합의, KV 적용을 분리해서 검증한다. 실제 공통 KvEngine을 사용한다. */
class IoniaStateMachineTest {
    private static final NodeId A = new NodeId(1);
    private static final NodeId B = new NodeId(2);
    private static final NodeId C = new NodeId(3);
    private static final List<NodeId> NODES = List.of(A, B, C);
    private static final ClientId CLIENT = new ClientId(1000);
    private static final Bytes KEY = Bytes.utf8("a");
    private static final Command.Set SET = new Command.Set(KEY, Bytes.utf8("hello"));
    private final Cluster cluster = new Cluster();

    @AfterEach
    void close() {
        cluster.client.close();
        cluster.engines.values().forEach(IoniaEngine::close);
        cluster.kvs.values().forEach(KvEngine::close);
    }

    @Test
    void allThreeRecordAcksCanCompleteBeforeOrderingAndKvApplication() {
        CompletionStage<Result> set = cluster.client.execute(SET, context(1));
        cluster.runOwners();
        assertEquals(ResultStatus.OK, result(set).status());
        assertEquals(Set.copyOf(NODES), cluster.recordTargets);
        assertEquals(1, cluster.engines.get(A).progress().lastLogIndex());
        for (NodeId node : NODES) {
            assertEquals(1, cluster.engines.get(node).progress().recordedCount());
            assertEquals(0, cluster.engines.get(node).progress().commitIndex());
            assertEquals(0, cluster.engines.get(node).progress().appliedIndex());
            assertEquals(ResultStatus.MISS, cluster.readLocal(node).status());
        }
        // Transport.send는 이미 완료됐지만 원격 PrepareAck를 아직 전달하지 않았다.
        assertFalse(cluster.messages.isEmpty());
    }

    @Test
    void setOverwriteGetDeleteAndDeleteRetryHaveTheSameOutcomeOnEveryReplica() {
        cluster.client.execute(SET, context(1));
        cluster.client.execute(new Command.Set(KEY, Bytes.utf8("world")), context(2));
        cluster.pump();
        CompletionStage<Result> get = cluster.client.execute(new Command.Get(KEY), context(3));
        cluster.pump();
        assertEquals(Bytes.utf8("world"), result(get).value().orElseThrow());
        for (NodeId node : NODES) {
            assertEquals(2, cluster.readLocal(node).valueVersion());
            assertEquals(Bytes.utf8("world"), cluster.readLocal(node).value().orElseThrow());
        }
        CompletionStage<Result> delete = cluster.client.execute(new Command.Delete(KEY), context(4));
        cluster.pump();
        assertEquals(ResultStatus.OK, result(delete).status());
        CompletionStage<Result> retry = cluster.client.execute(new Command.Delete(KEY), context(4));
        cluster.pump();
        assertEquals(ResultStatus.OK, result(retry).status()); // 재실행했다면 MISS가 되므로 결과도 재사용해야 한다.
        for (NodeId node : NODES) {
            assertEquals(3, cluster.engines.get(node).progress().appliedIndex());
            assertEquals(ResultStatus.MISS, cluster.readLocal(node).status());
        }
        CompletionStage<Result> missing = cluster.client.execute(new Command.Delete(KEY), context(5));
        cluster.pump();
        assertEquals(ResultStatus.MISS, result(missing).status());
    }

    @Test
    void leaderReadWaitsForEarlierAcknowledgedWritesToBeApplied() {
        CompletionStage<Result> set = cluster.client.execute(SET, context(1));
        cluster.runOwners();
        assertEquals(ResultStatus.OK, result(set).status());
        CompletionStage<Result> get = cluster.client.execute(new Command.Get(KEY), context(2));
        cluster.runOwners();
        assertFalse(get.toCompletableFuture().isDone());
        cluster.pump();
        assertEquals(Bytes.utf8("hello"), result(get).value().orElseThrow());
        assertEquals(List.of(A), cluster.executeTargets);
    }

    @Test
    void duplicateRequestsAndProtocolMessagesDoNotApplyTheWriteTwice() {
        cluster.client.execute(SET, context(1));
        cluster.runOwners();
        cluster.messages.addAll(List.copyOf(cluster.messages));
        cluster.pump();
        cluster.client.execute(SET, context(1));
        cluster.pump();
        for (NodeId node : NODES) {
            cluster.engines.get(node).onPeerMessage(A, new IoniaMessage.Commit(0, 0));
        }
        cluster.runOwners();
        for (NodeId node : NODES) {
            assertEquals(1, cluster.engines.get(node).progress().lastLogIndex());
            assertEquals(1, cluster.engines.get(node).progress().appliedIndex());
            assertEquals(1, cluster.readLocal(node).valueVersion());
        }
    }

    @Test
    void sameIdentityWithAnotherPayloadIsRejectedWithoutChangingData() {
        cluster.client.execute(SET, context(1));
        cluster.pump();
        CompletionStage<Result> conflict =
                cluster.client.execute(new Command.Set(KEY, Bytes.utf8("conflict")), context(1));
        cluster.pump();
        assertEquals(ResultStatus.BAD_REQUEST, result(conflict).status());
        for (NodeId node : NODES) {
            assertEquals(1, cluster.engines.get(node).progress().recordedCount());
            assertEquals(Bytes.utf8("hello"), cluster.readLocal(node).value().orElseThrow());
        }
    }

    @Test
    void commitArrivingBeforePrepareDoesNotSkipMissingEntries() {
        cluster.client.execute(SET, context(1));
        cluster.runOwners();
        cluster.deliver(message -> message.to().equals(B) && message.message() instanceof IoniaMessage.Prepare);
        cluster.deliver(message -> message.to().equals(A) && message.message() instanceof IoniaMessage.PrepareAck);
        cluster.deliver(message -> message.to().equals(C) && message.message() instanceof IoniaMessage.Commit);
        assertEquals(0, cluster.engines.get(C).progress().lastLogIndex());
        assertEquals(0, cluster.engines.get(C).progress().appliedIndex());
        cluster.deliver(message -> message.to().equals(C) && message.message() instanceof IoniaMessage.Prepare);
        assertEquals(1, cluster.engines.get(C).progress().appliedIndex());
        assertEquals(Bytes.utf8("hello"), cluster.readLocal(C).value().orElseThrow());
        cluster.pump();
    }

    @Test
    void aDroppedPrepareIsRetransmittedByTheLeaderTick() {
        cluster.client.execute(SET, context(1));
        cluster.runOwners();
        cluster.messages.removeIf(message -> message.to().equals(C));
        cluster.pump();
        assertEquals(1, cluster.engines.get(A).progress().appliedIndex());
        assertEquals(0, cluster.engines.get(C).progress().appliedIndex());
        cluster.tick();
        cluster.pump();
        assertEquals(1, cluster.engines.get(C).progress().appliedIndex());
        assertEquals(Bytes.utf8("hello"), cluster.readLocal(C).value().orElseThrow());
    }

    @Test
    void timeoutDoesNotUndoAcceptedWritesAndRetryDoesNotApplyAgain() {
        cluster.hiddenAcks.add(C);
        CompletionStage<Result> set = cluster.client.execute(SET, context(1));
        cluster.pump();
        assertFalse(set.toCompletableFuture().isDone());
        cluster.time.now = 2000;
        cluster.tick();
        cluster.pump();
        assertEquals(ResultStatus.TIMEOUT, result(set).status());
        cluster.hiddenAcks.clear();
        CompletionStage<Result> retry = cluster.client.execute(SET, context(1, 4000));
        cluster.pump();
        assertEquals(ResultStatus.OK, result(retry).status());
        for (NodeId node : NODES) {
            assertEquals(1, cluster.readLocal(node).valueVersion());
        }
    }

    @Test
    void expiredDeleteStillAppliesAfterCommitUsingAnInternalLifetime() {
        cluster.client.execute(SET, context(1));
        cluster.pump();
        CompletionStage<Result> delete = cluster.client.execute(new Command.Delete(KEY), context(2));
        cluster.runOwners();
        cluster.time.now = 2000;
        cluster.tick();
        cluster.runOwners();
        assertEquals(ResultStatus.TIMEOUT, result(delete).status());
        cluster.pump();
        for (NodeId node : NODES) {
            assertEquals(2, cluster.engines.get(node).progress().appliedIndex());
            assertEquals(ResultStatus.MISS, cluster.readLocal(node).status());
        }
    }

    @Test
    void followerReadsAreRefusedInsteadOfReturningPotentiallyStaleData() {
        cluster.client.execute(SET, context(1));
        cluster.runOwners();
        CompletionStage<Result> read = cluster.engines.get(B).execute(new Command.Get(KEY), context(2));
        cluster.runOwners();
        assertEquals(ResultStatus.NOT_PRIMARY, result(read).status());
    }

    @Test
    void wrongViewNonMembersAndFollowerOrderProposalsCannotAdvanceCommit() {
        cluster.client.execute(SET, context(1));
        cluster.runOwners();
        cluster.engines.get(A).onPeerMessage(new NodeId(99), new IoniaMessage.PrepareAck(0, 1));
        cluster.engines.get(A).onPeerMessage(B, new IoniaMessage.PrepareAck(1, 1));
        cluster.engines.get(A).onPeerMessage(B, new IoniaMessage.PrepareAck(0, 2));
        cluster.engines.get(B).onPeerMessage(C, new IoniaMessage.Prepare(0, List.of(
                new IoniaMessage.LogEntry(1, CLIENT, new OperationId(1), SET))));
        cluster.runOwners();
        assertEquals(0, cluster.engines.get(A).progress().commitIndex());
        assertEquals(0, cluster.engines.get(B).progress().lastLogIndex());
    }

    @Test
    void aForgedNodeAckCannotCountAsAnotherReplica() {
        cluster.ackTransform = (node, ack) -> node.equals(C)
                ? new IoniaClientTransport.WriteAck(B, ack.view(), ack.clientId(), ack.operationId(), ack.status())
                : ack;
        CompletionStage<Result> set = cluster.client.execute(SET, context(1));
        cluster.runOwners();
        assertEquals(ResultStatus.BAD_REQUEST, result(set).status());
    }

    @Test
    void requestIdentityAndDeadlineAreCheckedBeforeSending() {
        RequestContext wrongClient = new RequestContext(new ClientId(2000), new OperationId(1),
                RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(1000));
        assertEquals(ResultStatus.BAD_REQUEST, result(cluster.client.execute(SET, wrongClient)).status());
        assertEquals(ResultStatus.TIMEOUT, result(cluster.client.execute(SET, context(1, 0))).status());
        assertTrue(cluster.recordTargets.isEmpty());
    }

    @Test
    void mailboxRejectionReturnsBusyAndCloseFinishesOutstandingCalls() {
        cluster.owners.get(A).accepting = false;
        CompletionStage<Result> busy = cluster.client.execute(SET, context(1));
        cluster.runOwners();
        assertEquals(ResultStatus.BUSY, result(busy).status());
        cluster.owners.get(A).accepting = true;
        cluster.hiddenAcks.add(C);
        CompletionStage<Result> pending = cluster.client.execute(SET, context(2));
        cluster.runOwners();
        assertFalse(pending.toCompletableFuture().isDone());
        cluster.client.close();
        assertEquals(ResultStatus.UNAVAILABLE, result(pending).status());
        assertEquals(ResultStatus.UNAVAILABLE, result(cluster.client.execute(SET, context(3))).status());
    }

    @Test
    void engineCloseCompletesAReadWaitingForCommit() {
        cluster.client.execute(SET, context(1));
        cluster.runOwners();
        CompletionStage<Result> read = cluster.client.execute(new Command.Get(KEY), context(2));
        cluster.runOwners();
        assertFalse(read.toCompletableFuture().isDone());
        cluster.engines.get(A).close();
        assertEquals(ResultStatus.UNAVAILABLE, result(read).status());
    }

    @Test
    void kvFailureDoesNotAdvanceAppliedIndexAndStopsFurtherServing() {
        cluster.client.execute(SET, context(1));
        cluster.runOwners();
        cluster.kvs.get(A).close();
        cluster.pump();
        assertEquals(1, cluster.engines.get(A).progress().commitIndex());
        assertEquals(0, cluster.engines.get(A).progress().appliedIndex());
        assertEquals(ResultStatus.UNAVAILABLE,
                result(cluster.engines.get(A).execute(new Command.Get(KEY), context(2))).status());
    }

    @Test
    void relativeTimeoutIsRebasedToTheReceivingClock() {
        IoniaClientTransport.Request request = new IoniaClientTransport.Request(
                0, SET, CLIENT, new OperationId(1), RoutingVersion.ZERO, OwnershipEpoch.ZERO, 50);
        cluster.time.now = 5000;
        assertEquals(5050, request.contextAt(cluster.time).deadline().monotonicNanos());
    }

    private static RequestContext context(long operationId) {
        return context(operationId, 1000);
    }

    private static RequestContext context(long operationId, long deadline) {
        return new RequestContext(CLIENT, new OperationId(operationId),
                RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(deadline));
    }

    private static Result result(CompletionStage<Result> stage) {
        assertTrue(stage.toCompletableFuture().isDone(), "test must drive the owner/network before observing results");
        return stage.toCompletableFuture().join();
    }

    private static final class Clock implements TimeSource {
        private long now;
        @Override public long monotonicNanos() { return now; }
        @Override public long epochMillis() { return 0; }
    }

    private static final class TestMailbox implements Mailbox {
        private final Queue<Runnable> tasks;
        private boolean accepting = true;
        private TestMailbox(Queue<Runnable> tasks) { this.tasks = tasks; }
        @Override public boolean offer(Runnable task) { return accepting && tasks.offer(task); }
        @Override public int capacity() { return Integer.MAX_VALUE; }
    }

    private static final class Cluster implements IoniaClientTransport {
        private final Clock time = new Clock();
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private final List<PeerEnvelope> messages = new ArrayList<>();
        private final Map<NodeId, TestMailbox> owners = new HashMap<>();
        private final Map<NodeId, KvEngine> kvs = new HashMap<>();
        private final Map<NodeId, IoniaEngine> engines = new HashMap<>();
        private final Set<NodeId> recordTargets = new HashSet<>();
        private final List<NodeId> executeTargets = new ArrayList<>();
        private final Set<NodeId> hiddenAcks = new HashSet<>();
        private BiFunction<NodeId, WriteAck, WriteAck> ackTransform = (node, ack) -> ack;
        private final IoniaClient client;

        private Cluster() {
            for (NodeId node : NODES) {
                TestMailbox owner = new TestMailbox(tasks);
                KvEngine kv = new KvEngine(node, List.of(new TestMailbox(tasks)), time);
                owners.put(node, owner);
                kvs.put(node, kv);
                engines.put(node, new IoniaEngine(new IoniaConfig(node, A, NODES, 0), kv, owner,
                        envelope -> {
                            messages.add(envelope);
                            return CompletableFuture.completedFuture(null);
                        }, time));
            }
            client = new IoniaClient(CLIENT, NODES, A, 0, this, time);
        }

        @Override
        public CompletionStage<WriteAck> record(NodeId to, Request request) {
            recordTargets.add(to);
            CompletionStage<WriteAck> actual = engines.get(to).recordWrite(request);
            return hiddenAcks.contains(to) ? new CompletableFuture<>()
                    : actual.thenApply(ack -> ackTransform.apply(to, ack));
        }

        @Override
        public CompletionStage<Reply> execute(NodeId to, Request request) {
            executeTargets.add(to);
            return engines.get(to).execute(request);
        }

        private void runOwners() {
            int limit = 10000;
            while (!tasks.isEmpty()) {
                assertTrue(limit-- > 0, "owner work did not settle");
                tasks.remove().run();
            }
        }

        private void deliver(Predicate<PeerEnvelope> selector) {
            PeerEnvelope envelope = messages.stream().filter(selector).findFirst().orElseThrow();
            messages.remove(envelope);
            engines.get(envelope.to()).onPeerMessage(envelope.from(), envelope.message());
            runOwners();
        }

        private void pump() {
            int limit = 10000;
            while (!tasks.isEmpty() || !messages.isEmpty()) {
                assertTrue(limit-- > 0, "protocol did not settle");
                runOwners();
                if (!messages.isEmpty()) {
                    PeerEnvelope envelope = messages.removeFirst();
                    engines.get(envelope.to()).onPeerMessage(envelope.from(), envelope.message());
                }
            }
        }

        private void tick() {
            TimerEvent tick = new TimerEvent(IoniaEngine.TICK, time.now);
            client.onTimer(tick);
            engines.values().forEach(engine -> engine.onTimer(tick));
        }

        private Result readLocal(NodeId node) {
            CompletionStage<Result> result = kvs.get(node).execute(new Command.Get(KEY), context(9999, Long.MAX_VALUE));
            runOwners();
            return result(result);
        }
    }
}
