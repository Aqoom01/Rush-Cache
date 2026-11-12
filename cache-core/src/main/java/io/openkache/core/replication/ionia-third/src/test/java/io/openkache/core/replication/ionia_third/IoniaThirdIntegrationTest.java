package io.openkache.core.replication.ionia_third;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_third.IoniaThirdRegressionTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class IoniaThirdIntegrationTest {
    static RequestContext fresh(Cluster c, long id) { return new RequestContext(CLIENT, new OperationId(id), RoutingVersion.ZERO,
            OwnershipEpoch.ZERO, new Deadline(c.clock.now + 10_000_000_000L)); }
    static void advance(Cluster c) { c.clock.now += IoniaHistorySettings.TRIM_INTERVAL_NANOS; c.tick(); c.pump(); }
    static void fill(Cluster c, int count) {
        for (int i = 0; i < count; i++) c.client.execute(new Command.Set(Bytes.utf8("key" + i), Bytes.utf8("v")), fresh(c, i + 1));
        c.pump();
    }
    @Test void V02_05_lazyOwnerTickPublishesTargetAndDecisionAtomically() {
        for (int n : new int[]{0, 1, 2, 3}) try (Cluster c = new Cluster(3, n)) {
            fill(c, 3); assertEquals(3, c.engines.get(A).historyDiagnostics().history().entries());
            advance(c); var diagnostic = c.engines.get(A).historyDiagnostics(); var h = diagnostic.history();
            assertEquals(n == 0 ? 0 : n, h.entries()); assertEquals(n == 0 ? 3 : 3 - n, h.lastTrimmedIndex());
            assertEquals(1, h.trimCalls()); assertEquals(0, diagnostic.decisionView()); assertEquals(0, diagnostic.decisionGeneration());
            assertEquals(3, h.lastDecision().orElseThrow().safeThrough()); assertEquals(1_000_000_000L, h.lastDecision().orElseThrow().timeNanos());
        }
    }
    @Test void V02_06_activeFollowerGapAllowsOvershootUntilItActuallyApplies() {
        try (Cluster c = new Cluster(3, 1)) {
            c.stores.get(C).held.add(Bytes.utf8("key0")); fill(c, 3); advance(c);
            var h = c.engines.get(A).historyDiagnostics().history(); assertEquals(3, h.entries()); assertEquals(2, h.overshoot());
            assertEquals(0, h.lastDecision().orElseThrow().safeThrough());
            c.stores.get(C).release(Bytes.utf8("key0")); c.pump(); advance(c);
            assertEquals(1, c.engines.get(A).historyDiagnostics().history().entries());
        }
    }
    @Test void V02_09_capturedValueAndMissCannotBeCertifiedByLaterApplyAndTrim() {
        for (boolean miss : new boolean[]{false, true}) try (Cluster c = new Cluster(3, 1)) {
            long id = 1;
            if (!miss) { c.client.execute(set("old"), fresh(c, id++)); c.pump(); }
            c.stores.get(C).holdReadReplies = true;
            GateTransport gate = new GateTransport(c);
            try (IoniaClient reader = new IoniaClient(CLIENT, c.nodes, A, 0, gate, c.clock, new IoniaClient.Options(C, true, 10_000_000))) {
                CompletionStage<Result> read = reader.execute(new Command.Get(K), fresh(c, id++)); c.pump();
                c.client.execute(set("new"), fresh(c, id++));
                c.client.execute(new Command.Set(Bytes.utf8("other1"), Bytes.utf8("x")), fresh(c, id++));
                c.client.execute(new Command.Set(Bytes.utf8("other2"), Bytes.utf8("y")), fresh(c, id)); c.pump(); advance(c);
                c.stores.get(C).delayedReadReplies.forEach(Runnable::run); c.pump();
                assertFalse(read.toCompletableFuture().isDone());
                gate.releaseMeta(); c.pump();
                assertEquals(Bytes.utf8("new"), result(read).value().orElseThrow());
                assertEquals(miss ? ResultStatus.MISS : ResultStatus.OK, gate.captured.result().status());
                assertTrue(gate.captured.appliedIndex() < gate.meta.index()); assertEquals(1, gate.leaderRetries);
            }
        }
    }
    @Test void V02_10_pendingSetAndDeleteStillReturnDataBeforeIndex() {
        try (Cluster c = new Cluster(3, 1)) {
            c.stores.get(A).held.add(K); c.client.execute(set("value"), fresh(c, 1)); c.runOwners();
            CompletionStage<IoniaClientTransport.MetaReply> pendingSet = c.engines.get(A).metaQuery(request(2, new Command.Get(K), 0));
            c.pump(); assertFalse(pendingSet.toCompletableFuture().isDone());
            c.stores.get(A).release(K); c.pump(); assertEquals(Bytes.utf8("value"), pendingSet.toCompletableFuture().join().data().value().orElseThrow());
            c.stores.get(A).held.add(K); CompletionStage<Result> delete = c.client.execute(new Command.Delete(K), fresh(c, 3)); c.pump();
            CompletionStage<IoniaClientTransport.MetaReply> pendingDelete = c.engines.get(A).metaQuery(new IoniaClientTransport.Request(0, new Command.Get(K), CLIENT, new OperationId(4), RoutingVersion.ZERO, OwnershipEpoch.ZERO, 10_000_000_000L));
            c.pump(); assertFalse(pendingDelete.toCompletableFuture().isDone()); advance(c);
            assertFalse(pendingDelete.toCompletableFuture().isDone()); c.stores.get(A).release(K); c.pump();
            assertEquals(ResultStatus.OK, result(delete).status()); assertEquals(ResultStatus.MISS, pendingDelete.toCompletableFuture().join().data().status());
        }
    }
    @Test void V02_12_newViewRebuildDropsOldDecisionAndOldViewReports() {
        try (Cluster c = new Cluster(3, 1)) {
            fill(c, 3); advance(c); c.engines.get(B).startViewChange(1); c.engines.get(C).startViewChange(1); c.pump();
            var next = c.engines.get(B).historyDiagnostics(); assertEquals(1, next.view()); assertEquals(1, next.generation());
            assertEquals(3, next.history().entries()); assertEquals(-1, next.decisionView()); assertTrue(next.history().lastDecision().isEmpty());
            var unchanged = next.history(); c.engines.get(B).onPeerMessage(A, new IoniaMessage.Applied(0, 3)); c.pump();
            assertEquals(unchanged, c.engines.get(B).historyDiagnostics().history()); advance(c);
            assertEquals(1, c.engines.get(B).historyDiagnostics().decisionView());
        }
    }
    @Test void V02_15_originalClientReallyUsesFollowerAndMetaWithPositiveBudget() {
        try (Cluster c = new Cluster(3, 1)) {
            fill(c, 3); advance(c);
            CompletionStage<Result> get = c.client.readAtFollower(C, new Command.Get(Bytes.utf8("key0")), fresh(c, 10)); c.pump();
            assertEquals(Bytes.utf8("v"), result(get).value().orElseThrow()); assertEquals(1, c.followerCalls); assertEquals(1, c.metaCalls);
        }
    }
    private static final class GateTransport implements IoniaClientTransport {
        private final Cluster delegate; private Request held; private NodeId heldNode;
        private final CompletableFuture<MetaReply> response = new CompletableFuture<>();
        ReadReply captured; MetaReply meta; int leaderRetries;
        GateTransport(Cluster delegate) { this.delegate = delegate; }
        public CompletionStage<WriteAck> record(NodeId to, Request request) { return delegate.record(to, request); }
        public CompletionStage<Reply> execute(NodeId to, Request request) { leaderRetries++; return delegate.execute(to, request); }
        public CompletionStage<ReadReply> followerRead(NodeId to, Request request) {
            return delegate.followerRead(to, request).thenApply(value -> { captured = value; return value; });
        }
        public CompletionStage<MetaReply> metaQuery(NodeId to, Request request) { heldNode = to; held = request; return response; }
        public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) { return delegate.probe(to); }
        void releaseMeta() { delegate.metaQuery(heldNode, held).whenComplete((value, error) -> { meta = value; if (error == null) response.complete(value); else response.completeExceptionally(error); }); }
    }
}
