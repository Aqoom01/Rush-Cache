package io.openkache.core.replication.ionia_second;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.ionia_second.LeaderReadFixture.*;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class IoniaLeaderReadIntegrationTest {
    @Test void LR01_LR03_LR07_crudUsesOnlyLeaderDataReadsAndKeepsDeleteResults() {
        try (Cluster cluster = new Cluster(3)) {
            CompletionStage<Result> miss = cluster.client.execute(new Command.Get(K), context(1)); cluster.pump();
            assertEquals(ResultStatus.MISS, result(miss).status());
            CompletionStage<Result> write = cluster.client.execute(set("value"), context(2)); cluster.pump();
            assertEquals(ResultStatus.OK, result(write).status());
            CompletionStage<Result> hit = cluster.client.execute(new Command.Get(K), context(3)); cluster.pump();
            assertEquals(Bytes.utf8("value"), result(hit).value().orElseThrow());
            CompletionStage<Result> delete = cluster.client.execute(new Command.Delete(K), context(4)); cluster.pump();
            assertEquals(ResultStatus.OK, result(delete).status());
            CompletionStage<Result> again = cluster.client.execute(new Command.Delete(K), context(5)); cluster.pump();
            assertEquals(ResultStatus.MISS, result(again).status());
            CompletionStage<Result> gone = cluster.client.execute(new Command.Get(K), context(6)); cluster.pump();
            assertEquals(ResultStatus.MISS, result(gone).status());
            assertEquals(List.of(A, A, A), cluster.dataReads); assertTrue(cluster.probes >= 9);
        }
    }

    @Test void LR02_fastAckDoesNotLetAReadBypassPendingApply() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.stores.get(A).held.add(K);
            CompletionStage<Result> write = cluster.client.execute(set("latest"), context(1)); cluster.pump();
            assertEquals(ResultStatus.OK, result(write).status());
            assertEquals(0, cluster.engines.get(A).progress().appliedIndex());
            CompletionStage<Result> read = cluster.client.execute(new Command.Get(K), context(2)); cluster.pump();
            assertFalse(read.toCompletableFuture().isDone());
            cluster.stores.get(A).release(K); cluster.pump();
            assertEquals(Bytes.utf8("latest"), result(read).value().orElseThrow());
        }
    }

    @Test void LR03_deleteAndFollowingGetWaitForTheSameKey() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("before"), context(1)); cluster.pump();
            cluster.stores.get(A).held.add(K);
            CompletionStage<Result> delete = cluster.client.execute(new Command.Delete(K), context(2));
            CompletionStage<Result> read = cluster.client.execute(new Command.Get(K), context(3)); cluster.pump();
            assertFalse(delete.toCompletableFuture().isDone()); assertFalse(read.toCompletableFuture().isDone());
            cluster.stores.get(A).release(K); cluster.pump();
            assertEquals(ResultStatus.OK, result(delete).status()); assertEquals(ResultStatus.MISS, result(read).status());
        }
    }

    @Test void LR04_LR08_acknowledgedUncommittedWriteSurvivesThreeAndFiveNodeFailures() {
        for (int count : List.of(3, 5)) try (Cluster cluster = new Cluster(count)) {
            if (count == 5) cluster.hiddenAcks.add(cluster.nodes.getLast());
            CompletionStage<Result> write = cluster.client.execute(set("durable"), context(1)); cluster.runOwners();
            assertEquals(ResultStatus.OK, result(write).status());
            assertEquals(0, cluster.engines.get(A).progress().commitIndex());
            cluster.crash(A);
            if (count == 5) cluster.crash(cluster.nodes.getLast());
            for (NodeId node : cluster.nodes) if (!cluster.offline.contains(node)) cluster.engines.get(node).startViewChange(1);
            cluster.pump();
            assertEquals(B, cluster.client.refreshView().toCompletableFuture().join().leaderId());
            CompletionStage<Result> read = cluster.client.execute(new Command.Get(K), context(2)); cluster.pump();
            assertEquals(Bytes.utf8("durable"), result(read).value().orElseThrow());
            assertEquals(List.of(B), cluster.dataReads);
            CompletionStage<Result> next = cluster.client.execute(set("after"), context(3)); cluster.pump();
            assertEquals(ResultStatus.OK, result(next).status());
            assertEquals(2, cluster.engines.get(B).progress().appliedIndex());
        }
    }

    @Test void LR05_partitionedOldLeaderCannotReturnAStaleSuccessfulRead() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("old"), context(1)); cluster.pump();
            cluster.drop = message -> message.from().equals(A) || message.to().equals(A);
            cluster.engines.get(B).startViewChange(1); cluster.engines.get(C).startViewChange(1); cluster.pump();
            CompletionStage<IoniaClientTransport.Reply> update = cluster.engines.get(B).execute(request(2, set("new"), 1)); cluster.pump();
            assertEquals(ResultStatus.OK, update.toCompletableFuture().join().result().status());
            CompletionStage<Result> oldRead = cluster.client.execute(new Command.Get(K), context(3)); cluster.pump();
            assertEquals(ResultStatus.UNAVAILABLE, result(oldRead).status());
            cluster.client.refreshView().toCompletableFuture().join();
            CompletionStage<Result> fresh = cluster.client.execute(new Command.Get(K), context(4)); cluster.pump();
            assertEquals(Bytes.utf8("new"), result(fresh).value().orElseThrow());
            assertEquals(List.of(A, B), cluster.dataReads);
        }
    }

    @Test void LR08_twoOfThreeUsesOrderedFallbackAndDeduplicatesRetries() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.crash(C);
            CompletionStage<Result> write = cluster.client.execute(set("same"), context(1)); cluster.pump();
            assertEquals(ResultStatus.OK, result(write).status());
            assertTrue(cluster.executed.contains(A));
            CompletionStage<Result> retry = cluster.client.execute(set("same"), context(1)); cluster.pump();
            assertEquals(ResultStatus.OK, result(retry).status());
            assertEquals(1, cluster.engines.get(A).progress().lastLogIndex());
            CompletionStage<Result> conflict = cluster.client.execute(set("different"), context(1)); cluster.pump();
            assertEquals(ResultStatus.BAD_REQUEST, result(conflict).status());
        }
    }

    @Test void LR06_LR08_oneOfThreeTimesOutWithoutFalseSuccess() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.crash(B); cluster.crash(C);
            CompletionStage<Result> write = cluster.client.execute(set("unconfirmed"), context(1)); cluster.pump();
            assertFalse(write.toCompletableFuture().isDone());
            cluster.clock.now = 1_000_000_000L;
            cluster.client.onTimer(new TimerEvent(IoniaEngine.TICK, cluster.clock.now));
            assertEquals(ResultStatus.TIMEOUT, result(write).status());
            assertEquals(0, cluster.engines.get(A).progress().commitIndex());
        }
    }

    @Test void LR03_LR04_deleteRetryAfterViewChangeCannotDeleteANewerValue() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.client.execute(set("first"), context(1)); cluster.pump();
            CompletionStage<Result> delete = cluster.client.execute(new Command.Delete(K), context(2)); cluster.pump();
            assertEquals(ResultStatus.OK, result(delete).status());
            cluster.crash(A);
            cluster.engines.get(B).startViewChange(1); cluster.engines.get(C).startViewChange(1); cluster.pump();
            cluster.client.refreshView().toCompletableFuture().join();
            cluster.client.execute(set("second"), context(3)); cluster.pump();
            CompletionStage<Result> retry = cluster.client.execute(new Command.Delete(K), context(2)); cluster.pump();
            assertEquals(ResultStatus.OK, result(retry).status());
            CompletionStage<Result> read = cluster.client.execute(new Command.Get(K), context(4)); cluster.pump();
            assertEquals(Bytes.utf8("second"), result(read).value().orElseThrow());
            assertEquals(List.of(B), cluster.dataReads);
        }
    }
}
