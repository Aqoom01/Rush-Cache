package io.openkache.core.replication.ionia_fifth;

import io.openkache.core.replication.ionia.*;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaMessage.LogEntry;
import io.openkache.core.runtime.*;
import io.openkache.core.spi.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import org.junit.jupiter.api.*;

/** Deterministic acceptance fixtures. Failures report the delivery trace; no sleeps or wall-clock races. */
@Timeout(20)
class IoniaFifthRegressionTest {
    static final NodeId A = new NodeId(1), B = new NodeId(2), C = new NodeId(3);
    static final ClientId CLIENT = new ClientId(7);
    static final Bytes K = Bytes.utf8("key");
    static Command.Set set(String value) { return new Command.Set(K, Bytes.utf8(value)); }
    static LogEntry entry(long id) { return new LogEntry(id, CLIENT, new OperationId(id), set("v" + id)); }
    static RequestContext context(long id) { return new RequestContext(CLIENT, new OperationId(id), RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(1_000_000_000L)); }
    static IoniaClientTransport.Request request(long id, Command command, long view) {
        return new IoniaClientTransport.Request(view, command, CLIENT, new OperationId(id), RoutingVersion.ZERO, OwnershipEpoch.ZERO, 1_000_000_000L);
    }
    static Result result(CompletionStage<Result> stage) { assertTrue(stage.toCompletableFuture().isDone(), "fixture did not drive completion"); return stage.toCompletableFuture().join(); }

    @Test void T04_fiveReplicaThresholdsAndRotatingLeaderAreExact() {
        List<NodeId> nodes = List.of(A, B, C, new NodeId(4), new NodeId(5));
        IoniaConfig config = new IoniaConfig(A, A, nodes, 0);
        assertEquals(4, config.fastWriteQuorumSize()); assertEquals(3, config.orderingQuorumSize()); assertEquals(2, config.recoveryQuorumSize());
        assertEquals(B, config.leaderFor(1)); assertEquals(A, config.leaderFor(5));
    }
    @Test void T06_lateAckAfterTimeoutCannotChangeTheObservedResult() {
        Clock clock = new Clock(); Map<NodeId, CompletableFuture<IoniaClientTransport.WriteAck>> acks = new HashMap<>();
        IoniaClientTransport transport = new IoniaClientTransport() {
            public CompletionStage<WriteAck> record(NodeId to, Request request) { CompletableFuture<WriteAck> ack = new CompletableFuture<>(); acks.put(to, ack); return ack; }
            public CompletionStage<Reply> execute(NodeId to, Request request) { return new CompletableFuture<>(); }
        };
        try (IoniaClient client = new IoniaClient(CLIENT, List.of(A, B, C), A, 0, transport, clock)) {
            CompletionStage<Result> write = client.execute(set("v"), context(1));
            for (NodeId node : List.of(A, B)) acks.get(node).complete(new IoniaClientTransport.WriteAck(node, 0, CLIENT, new OperationId(1), ResultStatus.OK));
            clock.now = 2_000_000_000L; client.onTimer(new TimerEvent(IoniaEngine.TICK, clock.now));
            assertEquals(ResultStatus.TIMEOUT, result(write).status());
            acks.get(C).complete(new IoniaClientTransport.WriteAck(C, 0, CLIENT, new OperationId(1), ResultStatus.OK));
            assertEquals(ResultStatus.TIMEOUT, result(write).status());
        }
    }
    @Test void RC16_entireClusterRamLogLossCannotFabricateRecovery() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("acknowledged"), context(1)); cluster.runOwners();
            for (NodeId node : cluster.nodes) { cluster.restart(node, 0); cluster.engines.get(node).startRecovery(UUID.randomUUID()); }
            cluster.pump();
            for (NodeId node : cluster.nodes) assertEquals(IoniaEngine.Status.RECOVERING, cluster.engines.get(node).viewInfo().status());
        }
    }
    @Test void aRestartWithAnUnfinishedHigherViewCausesSurvivorsToAdvance() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.restart(B, 2); cluster.engines.get(B).startRecovery(UUID.randomUUID()); cluster.pump(); cluster.tick(); cluster.pump();
            assertEquals(2, cluster.engines.get(A).viewInfo().view());
            assertEquals(IoniaEngine.Status.NORMAL, cluster.engines.get(B).viewInfo().status(), cluster.trace.toString());
        }
    }
    @Test void RC15_stateReplacementWaitsForOldApplyBeforeResettingKv() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.stores.get(B).held.add(K); cluster.client.execute(set("v"), context(1)); cluster.pump();
            cluster.engines.get(B).startViewChange(1); cluster.engines.get(C).startViewChange(1); cluster.pump();
            assertEquals(IoniaEngine.Status.RECOVERING, cluster.engines.get(B).viewInfo().status());
            cluster.stores.get(B).release(K); cluster.pump();
            assertEquals(IoniaEngine.Status.NORMAL, cluster.engines.get(B).viewInfo().status());
            assertEquals(Bytes.utf8("v"), cluster.stores.get(B).values.get(K));
        }
    }
    @Test void T02_twoOfThreeFallsBackThroughConsensusRatherThanCountingTwoFastAcks() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.hiddenAcks.add(C);
            CompletionStage<Result> write = cluster.client.execute(set("v"), context(1)); cluster.pump();
            assertFalse(write.toCompletableFuture().isDone());
            cluster.clock.now = 20_000_000; cluster.client.onTimer(new TimerEvent(IoniaEngine.TICK, cluster.clock.now)); cluster.pump();
            assertEquals(ResultStatus.OK, result(write).status()); assertEquals(List.of(A), cluster.executed);
            assertEquals(1, cluster.engines.get(A).progress().commitIndex());
        }
    }
    @Test void T03_oneReplicaCannotCompleteEitherPath() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.offline.addAll(List.of(B, C));
            CompletionStage<Result> write = cluster.client.execute(set("v"), context(1)); cluster.pump();
            assertFalse(write.toCompletableFuture().isDone());
            cluster.clock.now = 2_000_000_000L; cluster.tick(); cluster.pump();
            assertEquals(ResultStatus.TIMEOUT, result(write).status()); assertEquals(0, cluster.engines.get(A).progress().commitIndex());
        }
    }
    @Test void T04_fourOfFiveFastAcksIncludeTheLeader() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.hiddenAcks.add(new NodeId(5));
            CompletionStage<Result> write = cluster.client.execute(set("v"), context(1)); cluster.runOwners();
            assertEquals(ResultStatus.OK, result(write).status()); assertEquals(0, cluster.engines.get(A).progress().commitIndex());
        }
        try (Cluster cluster = new Cluster(5)) {
            cluster.hiddenAcks.add(A);
            CompletionStage<Result> write = cluster.client.execute(set("v"), context(1)); cluster.runOwners();
            assertFalse(write.toCompletableFuture().isDone());
        }
    }
    @Test void T04_threeOfFiveUsesSlowConsensus() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.offline.addAll(List.of(new NodeId(4), new NodeId(5)));
            CompletionStage<Result> write = cluster.client.execute(set("v"), context(1)); cluster.pump();
            assertEquals(ResultStatus.OK, result(write).status()); assertEquals(1, cluster.engines.get(A).progress().commitIndex());
        }
    }
    @Test void T09_parallelCompletionNeverPublishesAnAppliedGap() {
        try (Cluster cluster = new Cluster(3)) {
            Bytes slow = Bytes.utf8("slow"), fast = Bytes.utf8("fast");
            cluster.stores.get(A).held.add(slow);
            cluster.client.execute(new Command.Set(slow, Bytes.utf8("a")), context(1));
            cluster.client.execute(new Command.Set(fast, Bytes.utf8("b")), context(2)); cluster.pump();
            assertEquals(2, cluster.engines.get(A).progress().commitIndex());
            assertEquals(0, cluster.engines.get(A).progress().appliedIndex());
            assertEquals(Bytes.utf8("b"), cluster.stores.get(A).values.get(fast));
            cluster.stores.get(A).release(slow); cluster.pump(); assertEquals(2, cluster.engines.get(A).progress().appliedIndex());
        }
    }
    @Test void T07_sameKeyQueuePreservesConsensusOrderEvenWhenFirstApplyIsDelayed() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.stores.get(A).held.add(K);
            cluster.client.execute(set("first"), context(1)); cluster.client.execute(set("second"), context(2)); cluster.pump();
            assertFalse(cluster.stores.get(A).values.containsKey(K)); assertEquals(1, cluster.stores.get(A).blocked.size());
            cluster.stores.get(A).release(K); cluster.pump(); assertEquals(Bytes.utf8("second"), cluster.stores.get(A).values.get(K));
        }
    }
    @Test void T12_T15_pendingMetaSynchronouslyReturnsLeaderData() {
        try (Cluster cluster = new Cluster(3)) {
            CompletionStage<Result> write = cluster.client.execute(set("new"), context(1)); cluster.runOwners();
            assertEquals(ResultStatus.OK, result(write).status());
            CompletionStage<IoniaClientTransport.MetaReply> meta = cluster.engines.get(A).metaQuery(request(2, new Command.Get(K), 0));
            cluster.runOwners(); assertFalse(meta.toCompletableFuture().isDone());
            cluster.pump(); assertEquals(Bytes.utf8("new"), meta.toCompletableFuture().join().data().value().orElseThrow());
        }
    }
    @Test void T14_staleFollowerValueOrMissIsRejectedAndRetriedAtLeader() {
        for (boolean hasOldValue : List.of(false, true)) try (Cluster cluster = new Cluster(3)) {
            if (hasOldValue) { cluster.client.execute(set("old"), context(1)); cluster.pump(); }
            cluster.drop = message -> message.to().equals(C);
            cluster.client.execute(set("new"), context(2)); cluster.pump();
            if (hasOldValue) assertEquals(Bytes.utf8("old"), cluster.stores.get(C).values.get(K));
            else assertFalse(cluster.stores.get(C).values.containsKey(K));
            CompletionStage<Result> read = cluster.client.readAtFollower(C, new Command.Get(K), context(3)); cluster.pump();
            assertEquals(Bytes.utf8("new"), result(read).value().orElseThrow()); assertEquals(List.of(A), cluster.executed);
        }
    }
    @Test void T14_currentFollowerValueIsAcceptedWithoutLeaderDataRead() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("new"), context(1)); cluster.pump();
            CompletionStage<Result> read = cluster.client.readAtFollower(C, new Command.Get(K), context(2)); cluster.pump();
            assertEquals(Bytes.utf8("new"), result(read).value().orElseThrow()); assertTrue(cluster.executed.isEmpty());
        }
    }
    @Test void T13_followerCapturesIndexBeforePhysicalValueRead() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("old"), context(1)); cluster.pump();
            cluster.stores.get(C).holdReadReplies = true;
            CompletionStage<IoniaClientTransport.ReadReply> read = cluster.engines.get(C).followerRead(request(2, new Command.Get(K), 0));
            cluster.pump(); assertFalse(read.toCompletableFuture().isDone());
            cluster.client.execute(set("new"), context(3)); cluster.pump();
            assertEquals(2, cluster.engines.get(C).progress().appliedIndex());
            cluster.stores.get(C).delayedReadReplies.forEach(Runnable::run); cluster.pump();
            assertEquals(Bytes.utf8("old"), read.toCompletableFuture().join().result().value().orElseThrow());
            assertEquals(1, read.toCompletableFuture().join().appliedIndex());
        }
    }
    @Test void T18_RC05_successfulUncommittedWriteSurvivesLeaderCrash() {
        try (Cluster cluster = new Cluster(3)) {
            CompletionStage<Result> write = cluster.client.execute(set("survives"), context(1)); cluster.runOwners();
            assertEquals(ResultStatus.OK, result(write).status()); assertEquals(0, cluster.engines.get(A).progress().commitIndex());
            cluster.crash(A); cluster.engines.get(B).startViewChange(1); cluster.engines.get(C).startViewChange(1); cluster.pump();
            assertEquals(IoniaEngine.Status.NORMAL, cluster.engines.get(B).viewInfo().status(), cluster.trace.toString());
            assertEquals(Bytes.utf8("survives"), cluster.stores.get(B).values.get(K));
            assertEquals(Bytes.utf8("survives"), cluster.stores.get(C).values.get(K));
        }
    }
    @Test void T19_RC06_fiveNodesRecoverFourAckWriteAfterTwoCrashes() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.hiddenAcks.add(new NodeId(5));
            CompletionStage<Result> write = cluster.client.execute(set("survives"), context(1)); cluster.runOwners(); assertEquals(ResultStatus.OK, result(write).status());
            cluster.crash(A); cluster.crash(new NodeId(5));
            for (NodeId node : List.of(B, C, new NodeId(4))) cluster.engines.get(node).startViewChange(1);
            cluster.pump(); assertEquals(Bytes.utf8("survives"), cluster.stores.get(B).values.get(K), cluster.trace.toString());
        }
    }
    @Test void RC01_RC02_oldPrepareIsFencedAndInsufficientViewVotesCannotNormalize() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.engines.get(B).startViewChange(1); cluster.runOwners();
            cluster.engines.get(B).onPeerMessage(A, new IoniaMessage.Prepare(0, List.of(entry(1)))); cluster.runOwners();
            assertEquals(0, cluster.engines.get(B).progress().lastLogIndex()); assertEquals(IoniaEngine.Status.VIEW_CHANGE, cluster.engines.get(B).viewInfo().status());
            assertFalse(cluster.engines.get(B).recordWrite(request(1, set("x"), 1)).toCompletableFuture().isDone()); cluster.runOwners();
        }
    }
    @Test void RC03_RC04_highestNormalViewThenLargestOpNumberSelectConsensus() {
        IoniaConfig config = new IoniaConfig(A, A, List.of(A, B, C), 0);
        IoniaRecovery.State oldLong = new IoniaRecovery.State(0, List.of(entry(1), entry(2)), List.of(), 1);
        IoniaRecovery.State newShort = new IoniaRecovery.State(1, List.of(entry(1)), List.of(), 1);
        assertEquals(List.of(entry(1)), IoniaRecovery.recover(config, Map.of(A, oldLong, B, newShort)).consensus());
        IoniaRecovery.State sameLong = new IoniaRecovery.State(1, List.of(entry(1), entry(2)), List.of(), 1);
        assertEquals(2, IoniaRecovery.recover(config, Map.of(A, sameLong, B, newShort)).consensus().size());
    }
    @Test void RC08_T21_presentAbsentCountsRestoreTheMissingEdge() {
        List<LogEntry> restored = IoniaRecovery.recoverDurability(2, List.of(List.of(entry(1), entry(2)), List.of(entry(1)), List.of(entry(2))));
        assertEquals(List.of(new OperationId(1), new OperationId(2)), restored.stream().map(LogEntry::operationId).toList());
    }
    @Test void RC07_incompleteClientWriteMayBeInRecoveredSet() {
        assertEquals(1, IoniaRecovery.recoverDurability(2, List.of(List.of(entry(1)), List.of(entry(1)))).size());
    }
    @Test void RC09_concurrentWritesAllowEitherTopologicalOrder() {
        List<LogEntry> restored = IoniaRecovery.recoverDurability(2, List.of(List.of(entry(1), entry(2)), List.of(entry(2), entry(1))));
        assertEquals(Set.of(new OperationId(1), new OperationId(2)), Set.copyOf(restored.stream().map(LogEntry::operationId).toList()));
    }
    @Test void RC10_cycleFailsClosed() {
        assertThrows(IllegalArgumentException.class, () -> IoniaRecovery.recoverDurability(2,
                List.of(List.of(entry(1), entry(2), entry(3)), List.of(entry(2), entry(3), entry(1)), List.of(entry(3), entry(1), entry(2)))));
    }
    @Test void RC11_T20_finalizedPositionsArePreservedWithoutDuplicatingDurableRequests() {
        IoniaRecovery.State state = new IoniaRecovery.State(0, List.of(entry(1)), List.of(entry(1), entry(2)), 1);
        IoniaRecovery.State result = IoniaRecovery.recover(new IoniaConfig(A, A, List.of(A, B, C), 0), Map.of(A, state, B, state));
        assertEquals(List.of(entry(1), entry(2)), result.consensus()); assertEquals(1, result.commitIndex());
    }
    @Test void recoveryRejectsConflictingCommittedPrefixesAndPayloads() {
        IoniaRecovery.State first = new IoniaRecovery.State(0, List.of(entry(1)), List.of(), 1);
        LogEntry changed = new LogEntry(1, CLIENT, new OperationId(1), set("different"));
        IoniaRecovery.State second = new IoniaRecovery.State(0, List.of(changed), List.of(), 1);
        assertThrows(IllegalArgumentException.class, () -> IoniaRecovery.recover(new IoniaConfig(A, A, List.of(A, B, C), 0), Map.of(A, first, B, second)));
        assertThrows(IllegalArgumentException.class, () -> IoniaRecovery.recoverDurability(2, List.of(List.of(entry(1)), List.of(changed))));
    }
    @Test void RC12_RC13_recoveryNeedsSameNonceAndHighestViewPrimary() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.restart(B, 0); UUID nonce = UUID.randomUUID(); cluster.engines.get(B).startRecovery(nonce); cluster.runOwners(); cluster.messages.clear();
            IoniaRecovery.State empty = new IoniaRecovery.State(0, List.of(), List.of(), 0);
            cluster.engines.get(B).onPeerMessage(A, new IoniaMessage.RecoveryResponse(0, UUID.randomUUID(), empty));
            cluster.engines.get(B).onPeerMessage(C, new IoniaMessage.RecoveryResponse(0, nonce, null)); cluster.runOwners();
            assertEquals(IoniaEngine.Status.RECOVERING, cluster.engines.get(B).viewInfo().status());
            cluster.engines.get(B).onPeerMessage(A, new IoniaMessage.RecoveryResponse(0, nonce, empty)); cluster.runOwners();
            assertEquals(IoniaEngine.Status.NORMAL, cluster.engines.get(B).viewInfo().status());
        }
    }
    @Test void RC12_aLowerViewPrimaryDoesNotSatisfyHighestViewResponse() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.restart(C, 0); UUID nonce = UUID.randomUUID(); cluster.engines.get(C).startRecovery(nonce); cluster.runOwners(); cluster.messages.clear();
            cluster.engines.get(C).onPeerMessage(A, new IoniaMessage.RecoveryResponse(0, nonce, new IoniaRecovery.State(0, List.of(), List.of(), 0)));
            cluster.engines.get(C).onPeerMessage(B, new IoniaMessage.RecoveryResponse(2, nonce, null)); cluster.runOwners();
            assertEquals(IoniaEngine.Status.RECOVERING, cluster.engines.get(C).viewInfo().status());
        }
    }
    @Test void T27_T30_restartReplaysAuthoritativeStateAndClearsStaleKeys() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("latest"), context(1)); cluster.pump(); cluster.restart(B, 0);
            cluster.stores.get(B).values.put(Bytes.utf8("ghost"), Bytes.utf8("invalid"));
            cluster.engines.get(B).startRecovery(UUID.randomUUID()); cluster.pump();
            assertEquals(IoniaEngine.Status.NORMAL, cluster.engines.get(B).viewInfo().status());
            assertEquals(Bytes.utf8("latest"), cluster.stores.get(B).values.get(K)); assertFalse(cluster.stores.get(B).values.containsKey(Bytes.utf8("ghost")));
            assertEquals(1, cluster.engines.get(B).progress().appliedIndex());
        }
    }
    @Test void T11_T28_replayedDeleteRetryKeepsOriginalResultWithoutDeletingLaterValue() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("first"), context(1)); cluster.pump();
            CompletionStage<Result> deleted = cluster.client.execute(new Command.Delete(K), context(2)); cluster.pump();
            assertEquals(ResultStatus.OK, result(deleted).status());
            cluster.client.execute(set("later"), context(3)); cluster.pump();
            cluster.restart(B, 0); cluster.engines.get(B).startRecovery(UUID.randomUUID()); cluster.pump();
            cluster.engines.get(B).startViewChange(1); cluster.engines.get(C).startViewChange(1); cluster.pump();
            cluster.client.updateView(1, B);
            CompletionStage<Result> retried = cluster.client.execute(new Command.Delete(K), context(2)); cluster.pump();
            assertEquals(ResultStatus.OK, result(retried).status());
            assertEquals(3, cluster.engines.get(B).progress().lastLogIndex());
            assertEquals(Bytes.utf8("later"), cluster.stores.get(B).values.get(K));
        }
    }
    @Test void T17_isolatedOldLeaderReadCannotPassAViewCertificate() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("old"), context(1)); cluster.pump();
            cluster.drop = message -> message.from().equals(A) || message.to().equals(A);
            cluster.engines.get(B).startViewChange(1); cluster.engines.get(C).startViewChange(1); cluster.pump();
            cluster.engines.get(B).recordWrite(request(2, set("new"), 1)); cluster.pump();
            try (IoniaClient oldClient = new IoniaClient(CLIENT, cluster.nodes, A, 0, cluster, cluster.clock)) {
                CompletionStage<Result> stale = oldClient.execute(new Command.Get(K), context(3)); cluster.pump();
                assertEquals(ResultStatus.UNAVAILABLE, result(stale).status());
            }
        }
    }
    @Test void T22_viewMetadataIsSavedBeforeNewViewMessagesAreSent() {
        try (Cluster cluster = new Cluster(3)) {
            List<Long> saved = new ArrayList<>();
            IoniaFifthEngine checked = new IoniaFifthEngine(new IoniaConfig(B, A, cluster.nodes, 0), cluster.stores.get(B), cluster.mailbox,
                    envelope -> { assertFalse(saved.isEmpty()); assertEquals(1L, saved.getLast()); return CompletableFuture.completedFuture(null); },
                    cluster.clock, saved::add, false, 0, Long.MAX_VALUE, new IoniaApplySettings(2,2));
            checked.startViewChange(1); cluster.runOwners(); assertEquals(List.of(1L), saved); checked.close();
        }
    }
    @Test void RC15_metaWhileRecoveringNeverReturnsNormalData() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.restart(A, 0); CompletionStage<IoniaClientTransport.MetaReply> meta = cluster.engines.get(A).metaQuery(request(1, new Command.Get(K), 0));
            cluster.runOwners(); assertEquals(ResultStatus.UNAVAILABLE, meta.toCompletableFuture().join().data().status());
        }
    }

    static final class Clock implements TimeSource {
        long now; public long monotonicNanos() { return now; } public long epochMillis() { return 0; }
    }
    static final class MemoryStorage implements IoniaStorage {
        final Queue<Runnable> tasks; final Map<Bytes, Bytes> values = new HashMap<>(); final Map<Bytes, Long> versions = new HashMap<>();
        final Set<Bytes> held = new HashSet<>(); final Map<Bytes,List<Runnable>> blocked=new HashMap<>();
        final List<Bytes> startedKeys=new ArrayList<>();
        int active,peak,resets; boolean throwApply,exceptionalApply,cancelApply,failReset;
        ResultStatus rejectApply; Bytes failOnlyKey;
        boolean holdReadReplies; final List<Runnable> delayedReadReplies = new ArrayList<>();
        MemoryStorage(Queue<Runnable> tasks) { this.tasks = tasks; }
        public CompletionStage<Result> execute(Command command, RequestContext context) {
            CompletableFuture<Result> future = new CompletableFuture<>();
            if(!(command instanceof Command.Get)) {
                if(throwApply && (failOnlyKey==null || failOnlyKey.equals(command.key())))throw new IllegalStateException("injected apply rejection");
                if(exceptionalApply && (failOnlyKey==null || failOnlyKey.equals(command.key())))return CompletableFuture.failedFuture(new IllegalStateException("injected failure"));
                if(cancelApply && (failOnlyKey==null || failOnlyKey.equals(command.key()))){future.cancel(false);return future;}
                if(rejectApply!=null && (failOnlyKey==null || failOnlyKey.equals(command.key())))return CompletableFuture.completedFuture(plain(rejectApply));
                startedKeys.add(command.key());active++;peak=Math.max(peak,active);
                future.whenComplete((value,error)->active--);
            }
            Runnable apply = () -> {
                if (command instanceof Command.Get get) {
                    Result value = values.containsKey(get.key()) ? Result.ok(values.get(get.key()), versions.get(get.key()), RoutingVersion.ZERO, OwnershipEpoch.ZERO) : plain(ResultStatus.MISS);
                    if (holdReadReplies) delayedReadReplies.add(() -> future.complete(value)); else future.complete(value);
                }
                else if (command instanceof Command.Set set) { values.put(set.key(), set.value()); long version = versions.merge(set.key(), 1L, Long::sum);
                    future.complete(new Result(ResultStatus.OK, Optional.empty(), version, RoutingVersion.ZERO, OwnershipEpoch.ZERO, Optional.empty())); }
                else { boolean existed = values.remove(command.key()) != null; versions.remove(command.key()); future.complete(plain(existed ? ResultStatus.OK : ResultStatus.MISS)); }
            };
            if (!(command instanceof Command.Get) && held.contains(command.key())) blocked.computeIfAbsent(command.key(),ignored->new ArrayList<>()).add(apply); else tasks.add(apply);
            return future;
        }
        void release(Bytes key) { held.remove(key);tasks.addAll(blocked.getOrDefault(key,List.of()));blocked.remove(key); }
        public CompletionStage<Void> reset() { resets++;if(failReset)return CompletableFuture.failedFuture(new IllegalStateException("injected reset failure")); values.clear(); versions.clear(); return CompletableFuture.completedFuture(null); }
        public EngineSnapshot snapshot() { return new EngineSnapshot(A, 0, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
        public void onPeerMessage(NodeId from, PeerMessage message) { } public void onTimer(TimerEvent timer) { } public void close() { }
    }
    static Result plain(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
    static final class Cluster implements IoniaClientTransport, AutoCloseable {
        final Clock clock = new Clock(); final Queue<Runnable> tasks = new ArrayDeque<>();
        final Mailbox mailbox = new Mailbox() { public boolean offer(Runnable task) { return tasks.offer(task); } public int capacity() { return 100000; } };
        final List<NodeId> nodes = new ArrayList<>(); final Map<NodeId, IoniaFifthEngine> engines = new HashMap<>();
        final Map<NodeId, MemoryStorage> stores = new HashMap<>(); final List<PeerEnvelope> messages = new ArrayList<>();
        final Set<NodeId> offline = new HashSet<>(), hiddenAcks = new HashSet<>(); final List<NodeId> executed = new ArrayList<>();
        int followerCalls, metaCalls;
        final List<String> trace = new ArrayList<>(); Predicate<PeerEnvelope> drop = envelope -> false; final IoniaClient client;
        final IoniaApplySettings settings;
        Cluster(int count) { this(count,2); }
        Cluster(int count,int limit) { this(count,new IoniaApplySettings(limit,2)); }
        Cluster(int count,IoniaApplySettings settings) {
            this.settings=settings;
            for (int i = 1; i <= count; i++) nodes.add(new NodeId(i));
            for (NodeId node : nodes) create(node, false, 0);
            client = new IoniaClient(CLIENT, nodes, A, 0, this, clock);
        }
        void create(NodeId node, boolean recovering, long view) {
            MemoryStorage storage = new MemoryStorage(tasks); stores.put(node, storage);
            engines.put(node, new IoniaFifthEngine(new IoniaConfig(node, A, nodes, 0), storage, mailbox,
                    envelope -> { messages.add(envelope); return CompletableFuture.completedFuture(null); }, clock,
                    IoniaViewStore.simulation(), recovering, view, Long.MAX_VALUE, settings));
        }
        void crash(NodeId node) { offline.add(node); engines.get(node).close(); }
        void restart(NodeId node, long view) { engines.get(node).close(); offline.remove(node); create(node, true, view); }
        public CompletionStage<WriteAck> record(NodeId to, Request request) {
            if (offline.contains(to)) return CompletableFuture.failedFuture(new IllegalStateException("offline"));
            CompletionStage<WriteAck> response = engines.get(to).recordWrite(request);
            return hiddenAcks.contains(to) ? new CompletableFuture<>() : response;
        }
        public CompletionStage<Reply> execute(NodeId to, Request request) {
            executed.add(to); return offline.contains(to) ? CompletableFuture.failedFuture(new IllegalStateException("offline")) : engines.get(to).execute(request);
        }
        public CompletionStage<ReadReply> followerRead(NodeId to, Request request) { followerCalls++; return engines.get(to).followerRead(request); }
        public CompletionStage<MetaReply> metaQuery(NodeId to, Request request) { metaCalls++; return engines.get(to).metaQuery(request); }
        public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) { return offline.contains(to) ? CompletableFuture.failedFuture(new IllegalStateException("offline")) : CompletableFuture.completedFuture(engines.get(to).viewInfo()); }
        void runOne() { if (!tasks.isEmpty()) tasks.remove().run(); }
        void runOwners() { int limit = 100000; while (!tasks.isEmpty()) { assertTrue(limit-- > 0, trace.toString()); runOne(); } }
        void pump() {
            int limit = 100000;
            while (!tasks.isEmpty() || !messages.isEmpty()) {
                assertTrue(limit-- > 0, trace.toString()); runOwners();
                if (!messages.isEmpty()) {
                    PeerEnvelope message = messages.removeFirst(); trace.add(message.from() + "->" + message.to() + ":" + message.message());
                    if (!offline.contains(message.from()) && !offline.contains(message.to()) && !drop.test(message)) engines.get(message.to()).onPeerMessage(message.from(), message.message());
                }
            }
            for (IoniaFifthEngine engine : engines.values()) assertTrue(engine.failureReason().isEmpty(), engine.failureReason().toString() + trace);
        }
        void tick() { client.onTimer(new TimerEvent(IoniaEngine.TICK, clock.now)); engines.values().forEach(engine -> engine.onTimer(new TimerEvent(IoniaEngine.TICK, clock.now))); }
        public void close() { client.close(); engines.values().forEach(IoniaFifthEngine::close); }
    }
}
