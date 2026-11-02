package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.core.replication.experiment.FailoverExperiment.ms;

class FailoverExperimentTest {
    @Test void localApplyBarrierMustFinishBeforeServing() {
        var time = new io.openkache.core.replication.VirtualTimeSource();
        var barrier = new java.util.concurrent.CompletableFuture<Result>();
        CacheEngine kv = new CacheEngine() {
            @Override public java.util.concurrent.CompletionStage<Result> execute(Command command, RequestContext context) { return barrier; }
            @Override public void onPeerMessage(NodeId from, io.openkache.core.spi.PeerMessage message) {}
            @Override public void onTimer(io.openkache.core.runtime.TimerEvent event) {}
            @Override public EngineSnapshot snapshot() { return new EngineSnapshot(new NodeId(1), 0, RoutingVersion.ZERO, new OwnershipEpoch(1)); }
            @Override public void close() {}
        };
        var engine = new CrashOnlyPrimaryBackupEngine(new PrimaryBackupConfig(new NodeId(1), new NodeId(0),
                java.util.List.of(new NodeId(1), new NodeId(2)), new OwnershipEpoch(1)), kv,
                new io.openkache.core.replication.DirectMailbox(), e -> java.util.concurrent.CompletableFuture.completedFuture(null),
                time, java.time.Duration.ofMillis(10), java.time.Duration.ofMillis(50));
        try {
            time.advanceTo(ms(50)); engine.onTimer(new io.openkache.core.runtime.TimerEvent(CrashOnlyPrimaryBackupEngine.TICK, ms(50)));
            engine.onPeerMessage(new NodeId(2), new CrashOnlyPrimaryBackupEngine.Control(new OwnershipEpoch(2), CrashOnlyPrimaryBackupEngine.Kind.READY));
            assertEquals(-1, engine.readyAtNanos());
            barrier.complete(Result.of(ResultStatus.OK, RoutingVersion.ZERO, new OwnershipEpoch(1)));
            assertEquals(ms(50), engine.readyAtNanos());
        } finally { engine.close(); }
    }
    @Test void allConditionsPreserveAcknowledgedWritesAndReplay() {
        for (int d : new int[] {1, 5, 10}) for (int t : new int[] {50, 100, 200}) for (int r : new int[] {1, 10, 25}) {
            var m = FailoverExperiment.run(d, t, r);
            assertEquals(m, FailoverExperiment.run(d, t, r));
            assertEquals(10, m.preserved()); assertEquals(2L*d, m.transition());
            assertEquals(m.total(), m.detection()+m.transition()+m.clientAndWrite());
        }
    }
    @Test void healthyHeartbeatsPreventPromotion() {
        try (var c = new FailoverExperiment.Cluster(5, 50)) {
            c.advance(ms(500));
            assertEquals(-1, c.engines.get(new NodeId(1)).detectedAtNanos());
            assertEquals(ResultStatus.NOT_PRIMARY, c.set(2, "x", 1).join().status());
        }
    }
    @Test void readinessIsRequiredAndRetried() {
        try (var c = new FailoverExperiment.Cluster(5, 50)) {
            c.advance(ms(20)); c.dropReady = true; c.crash(); c.advance(ms(150));
            assertEquals(-1, c.engines.get(new NodeId(1)).readyAtNanos());
            assertEquals(ResultStatus.UNAVAILABLE, c.set(1, "blocked", 2).join().status());
            c.dropReady = false; c.advance(ms(200));
            var f = c.set(1, "resumed", 2); c.advance(ms(220));
            assertEquals(ResultStatus.OK, f.join().status()); c.verify("resumed");
        }
    }
    @Test void staleEpochAndLateOldPrimaryMessagesAreRejected() {
        try (var c = new FailoverExperiment.Cluster(5, 50)) {
            c.advance(ms(20)); c.crash(); c.advance(ms(100));
            assertEquals(ResultStatus.STALE_EPOCH, c.set(1, "stale", 1).join().status());
            var f = c.set(1, "new", 2); c.advance(ms(120)); assertEquals(ResultStatus.OK, f.join().status());
            for (int id : new int[] {1, 2}) c.engines.get(new NodeId(id)).onPeerMessage(new NodeId(0),
                    new PrimaryBackupMessage.Replicate(new OwnershipEpoch(1), 99, new Command.Set(Bytes.utf8("new"), Bytes.utf8("old"))));
            c.verify("new");
            assertEquals(new NodeId(1), c.set(2, "redirect", 2).join().redirect().orElseThrow().target());
        }
    }
    @Test void missingSurvivingBackupAckNeverSucceeds() {
        try (var c = new FailoverExperiment.Cluster(5, 50)) {
            c.advance(ms(20)); c.crash(); c.advance(ms(100)); c.dead.add(new NodeId(2));
            var f = c.set(1, "no-ack", 2); c.advance(ms(1101));
            assertEquals(ResultStatus.TIMEOUT, f.join().status());
        }
    }
    @Test void crashDuringReplicationDoesNotInventSuccess() {
        try (var c = new FailoverExperiment.Cluster(5, 50)) {
            var f = c.set(0, "in-flight", 1); c.advance(ms(6)); c.crash();
            assertEquals(ResultStatus.UNAVAILABLE, f.join().status()); c.advance(ms(100));
            var next = c.set(1, "after-pending", 2); c.advance(ms(120));
            assertEquals(ResultStatus.OK, next.join().status()); c.verify("after-pending");
        }
    }
}
