package io.openkache.core.replication.primarybackup;

import io.openkache.core.api.*;
import io.openkache.core.failure.MemberStatus;
import io.openkache.core.failure.MembershipEvent;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.VirtualTimeSource;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerEnvelope;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

final class PrimaryBackupFencingTest {
    private static final NodeId OLD = new NodeId(0), NEXT = new NodeId(1), BACKUP = new NodeId(2);
    private static final Bytes KEY = Bytes.utf8("key"), VALUE = Bytes.utf8("committed");
    private static final Duration LEASE = Duration.ofMillis(100), RENEW = Duration.ofMillis(10);
    private static final List<NodeId> NODES = List.of(OLD, NEXT, BACKUP);

    @Test void suspectDoesNotPromoteAndDeadMustWaitForBothGrantsToExpire() {
        var cluster = new Cluster();
        cluster.start();
        assertEquals(ResultStatus.OK, cluster.set(0, 1, KEY, VALUE).status());
        cluster.isolateOld();
        cluster.fd(MemberStatus.SUSPECT);
        cluster.advance(30);
        assertEquals(1, cluster.epoch(1));
        assertEquals(ResultStatus.OK, cluster.get(0, 1, KEY).status());

        cluster.fd(MemberStatus.DEAD);
        cluster.fd(MemberStatus.DEAD);
        cluster.advance(70);
        assertEquals(ResultStatus.UNAVAILABLE, cluster.get(0, 1, KEY).status());
        assertEquals(ResultStatus.UNAVAILABLE, cluster.set(0, 1, KEY, Bytes.utf8("stale")).status());
        assertEquals(1, cluster.epoch(1), "holder cutoff alone does not release either grant");
        cluster.advance(100);
        assertEquals(1, cluster.epoch(1));
        cluster.advance(80);

        assertEquals(2, cluster.epoch(1));
        assertEquals(2, cluster.epoch(2));
        assertEquals(VALUE, cluster.get(1, 2, KEY).value().orElseThrow());
        assertEquals(VALUE, cluster.local(2, KEY).value().orElseThrow());
        assertEquals(ResultStatus.STALE_EPOCH, cluster.get(1, 1, KEY).status());
        var redirect = cluster.get(2, 2, KEY);
        assertEquals(ResultStatus.NOT_PRIMARY, redirect.status());
        assertEquals(NEXT, redirect.redirect().orElseThrow().target());
        assertEquals(2, cluster.nodes.get(1).fenceObservation().votes());

        cluster.drop = envelope -> false;
        cluster.fd(MemberStatus.DEAD);
        cluster.nodes.get(2).onPeerMessage(OLD, new PrimaryBackupMessage.Replicate(
                new OwnershipEpoch(1), 500, new Command.Set(KEY, Bytes.utf8("stale"))));
        cluster.advance(70);
        assertEquals(2, cluster.epoch(1));
        assertEquals(ResultStatus.UNAVAILABLE, cluster.get(0, 1, KEY).status());
        assertEquals(VALUE, cluster.local(2, KEY).value().orElseThrow());
        assertEquals(ResultStatus.OK, cluster.set(1, 2, KEY, Bytes.utf8("new")).status());
        assertEquals(Bytes.utf8("new"), cluster.local(2, KEY).value().orElseThrow());
    }

    @Test void missingVoteAndMissingReadyBothPreventServing() {
        var cluster = new Cluster();
        cluster.start();
        cluster.fd(MemberStatus.DEAD);
        cluster.drop = e -> e.from().equals(OLD) || e.to().equals(OLD)
                || e.message() instanceof PrimaryBackupFence.Message m && m.kind() == PrimaryBackupFence.Kind.VOTE;
        cluster.advance(350);
        assertEquals(1, cluster.epoch(1));
        assertNotEquals(ResultStatus.OK, cluster.set(1, 2, KEY, VALUE).status());

        cluster.drop = e -> e.from().equals(OLD) || e.to().equals(OLD)
                || e.message() instanceof CrashOnlyPrimaryBackupEngine.Control c
                && c.kind() == CrashOnlyPrimaryBackupEngine.Kind.READY;
        cluster.advance(100);
        assertEquals(2, cluster.epoch(1));
        assertEquals(ResultStatus.UNAVAILABLE, cluster.set(1, 2, KEY, VALUE).status());
        cluster.isolateOld();
        cluster.advance(100);
        assertEquals(ResultStatus.OK, cluster.set(1, 2, KEY, VALUE).status());
    }

    @Test void delayedGrantDoesNotReviveAnExpiredRoundAndSuspectCannotOpenWrites() {
        var cluster = new Cluster();
        var delayed = new ArrayList<PeerEnvelope>();
        cluster.drop = e -> {
            if (e.message() instanceof PrimaryBackupFence.Message m && m.kind() == PrimaryBackupFence.Kind.GRANT) {
                delayed.add(e);
                return true;
            }
            return false;
        };
        cluster.start();
        assertEquals(2, delayed.size());
        cluster.tickOld = false;
        cluster.isolateOld();
        cluster.fd(MemberStatus.SUSPECT);
        cluster.advance(350);
        delayed.forEach(e -> cluster.nodes.getFirst().onPeerMessage(e.from(), e.message()));
        assertEquals(ResultStatus.UNAVAILABLE, cluster.get(0, 1, KEY).status());
        assertEquals(1, cluster.epoch(1));
        assertEquals(0, cluster.nodes.get(1).fenceObservation().votes());
    }

    @Test void oldPrimaryRemainsFencedAfterConnectivityReturns() {
        var cluster = new Cluster();
        cluster.start();
        assertEquals(ResultStatus.MISS, cluster.get(0, 1, KEY).status());
        cluster.isolateOld();
        cluster.advance(150);
        cluster.drop = e -> false;
        cluster.advance(100);
        assertEquals("FENCED", cluster.nodes.getFirst().fenceObservation().state());
        assertEquals(ResultStatus.UNAVAILABLE, cluster.set(0, 1, KEY, VALUE).status());
        assertEquals(1, cluster.epoch(1));
    }

    @Test void deadEventSurvivesAFullOwnerMailbox() {
        var cluster = new Cluster();
        cluster.start();
        cluster.isolateOld();
        cluster.candidateOwner.rejecting = true;
        cluster.fd(MemberStatus.DEAD);
        cluster.advance(250);
        assertEquals(1, cluster.epoch(1));

        cluster.candidateOwner.rejecting = false;
        cluster.advance(100);
        assertEquals(2, cluster.epoch(1));
        assertEquals(ResultStatus.OK, cluster.set(1, 2, KEY, VALUE).status());
    }

    @Test void delayedReplicationCannotOverwriteANewerWriteAfterPromotion() {
        var cluster = new Cluster();
        cluster.start();
        cluster.isolateOld();
        cluster.fd(MemberStatus.DEAD);
        cluster.advance(300);
        assertEquals(2, cluster.epoch(1));

        var newer = Bytes.utf8("newer");
        assertEquals(ResultStatus.OK, cluster.set(1, 2, KEY, VALUE).status());
        assertEquals(ResultStatus.OK, cluster.set(1, 2, KEY, newer).status());
        cluster.nodes.get(2).onPeerMessage(NEXT, new PrimaryBackupMessage.Replicate(
                new OwnershipEpoch(2), 1, new Command.Set(KEY, VALUE)));
        assertEquals(newer, cluster.local(2, KEY).value().orElseThrow());
    }

    @Test void backupApplyAndInstallCompletionsSurviveAFullOwnerMailbox() {
        var cluster = new Cluster();
        cluster.start();
        cluster.candidateKv.paused = true;
        var written = cluster.submit(0, 1, new Command.Set(KEY, VALUE));
        cluster.pump();
        assertFalse(written.toCompletableFuture().isDone());
        cluster.candidateOwner.rejecting = true;
        cluster.candidateKv.resume();
        cluster.pump();
        assertEquals(ResultStatus.OK, written.toCompletableFuture().join().status());

        cluster.candidateOwner.rejecting = false;
        cluster.candidateKv.paused = true;
        cluster.isolateOld();
        cluster.fd(MemberStatus.DEAD);
        cluster.advance(300);
        assertEquals(1, cluster.epoch(1));
        assertEquals(1, cluster.candidateKv.tasks.size(), "installation waits for the KV barrier");
        cluster.candidateOwner.rejecting = true;
        cluster.candidateKv.resume();
        cluster.candidateOwner.rejecting = false;
        cluster.advance(100);
        assertEquals(2, cluster.epoch(1));
        assertEquals(VALUE, cluster.get(1, 2, KEY).value().orElseThrow());
    }

    @Test void unequalAppliedHistoriesStayClosed() {
        var cluster = new Cluster();
        cluster.start();
        assertEquals(ResultStatus.OK, cluster.set(0, 1, KEY, VALUE).status());
        cluster.drop = e -> e.to().equals(BACKUP) && e.message() instanceof PrimaryBackupMessage.Replicate;
        var ambiguous = cluster.submit(0, 1, new Command.Set(KEY, Bytes.utf8("partial")));
        cluster.pump();
        assertFalse(ambiguous.toCompletableFuture().isDone());
        cluster.isolateOld();
        cluster.fd(MemberStatus.DEAD);
        cluster.advance(300);
        assertEquals(1, cluster.epoch(1));
        assertEquals("HISTORY_NOT_READY", cluster.nodes.get(1).fenceObservation().state());
        assertNotEquals(ResultStatus.OK, cluster.get(1, 2, KEY).status());
    }

    @Test void suspendedPrimaryCannotCompleteAnOldWriteAfterFailover() {
        var cluster = new Cluster();
        cluster.start();
        cluster.primaryKv.paused = true;
        var delayed = cluster.submit(0, 1, new Command.Set(KEY, VALUE));
        cluster.pump();
        assertFalse(delayed.toCompletableFuture().isDone());
        assertEquals(1, cluster.primaryKv.tasks.size());
        cluster.isolateOld();
        cluster.tickOld = false;
        cluster.fd(MemberStatus.DEAD);
        cluster.advance(300);
        assertEquals(2, cluster.epoch(1));
        assertEquals(VALUE, cluster.get(1, 2, KEY).value().orElseThrow());
        cluster.primaryKv.resume();
        assertEquals(ResultStatus.UNAVAILABLE, delayed.toCompletableFuture().join().status());
        assertEquals(ResultStatus.UNAVAILABLE, cluster.get(0, 1, KEY).status());
    }

    private static final class Cluster {
        final List<VirtualTimeSource> times = List.of(new VirtualTimeSource(), new VirtualTimeSource(), new VirtualTimeSource());
        final List<KvEngine> stores = new ArrayList<>();
        final List<CrashOnlyPrimaryBackupEngine> nodes = new ArrayList<>();
        final ArrayDeque<PeerEnvelope> network = new ArrayDeque<>();
        final PausableMailbox primaryKv = new PausableMailbox();
        final PausableMailbox candidateKv = new PausableMailbox();
        final PausableMailbox candidateOwner = new PausableMailbox();
        Predicate<PeerEnvelope> drop = e -> false;
        boolean tickOld = true;

        Cluster() {
            for (int i = 0; i < 3; i++) {
                var time = times.get(i);
                time.advance(Duration.ofSeconds(5L * i)); // independent clock origins
                var self = NODES.get(i);
                var store = new KvEngine(self, List.of(i == 0 ? primaryKv : i == 1 ? candidateKv : new DirectMailbox()), time);
                stores.add(store);
                nodes.add(new CrashOnlyPrimaryBackupEngine(
                        new PrimaryBackupConfig(self, OLD, List.of(NEXT, BACKUP), new OwnershipEpoch(1)),
                        store, i == 1 ? candidateOwner : new DirectMailbox(), envelope -> {
                            if (!drop.test(envelope)) network.add(envelope);
                            return CompletableFuture.completedFuture(null);
                        }, time, RENEW, LEASE, LEASE));
            }
        }

        void start() { tick(); }
        void isolateOld() { drop = e -> e.from().equals(OLD) || e.to().equals(OLD); }
        void fd(MemberStatus status) {
            nodes.get(1).onMembershipChanged(new MembershipEvent(OLD, MemberStatus.ALIVE, status, 0, times.get(1).monotonicNanos()));
        }
        long epoch(int node) { return nodes.get(node).snapshot().ownershipEpoch().value(); }
        void tick() {
            for (int i = tickOld ? 0 : 1; i < 3; i++)
                nodes.get(i).onTimer(new TimerEvent(CrashOnlyPrimaryBackupEngine.TICK, times.get(i).monotonicNanos()));
            pump();
        }
        void advance(int milliseconds) {
            for (int elapsed = 0; elapsed < milliseconds; elapsed += 10) {
                times.forEach(time -> time.advance(RENEW));
                tick();
            }
        }
        void pump() {
            while (!network.isEmpty()) {
                var message = network.removeFirst();
                nodes.get((int) message.to().value()).onPeerMessage(message.from(), message.message());
            }
        }
        RequestContext context(int node, long epoch) {
            return new RequestContext(new ClientId(10), new OperationId(20), RoutingVersion.ZERO,
                    new OwnershipEpoch(epoch), Deadline.after(times.get(node), Duration.ofSeconds(1)));
        }
        CompletionStage<Result> submit(int node, long epoch, Command command) {
            return nodes.get(node).execute(command, context(node, epoch));
        }
        Result set(int node, long epoch, Bytes key, Bytes value) {
            var result = submit(node, epoch, new Command.Set(key, value));
            pump();
            assertTrue(result.toCompletableFuture().isDone());
            return result.toCompletableFuture().join();
        }
        Result get(int node, long epoch, Bytes key) {
            return submit(node, epoch, new Command.Get(key)).toCompletableFuture().join();
        }
        Result local(int node, Bytes key) {
            return stores.get(node).execute(new Command.Get(key), context(node, epoch(node))).toCompletableFuture().join();
        }
    }

    private static final class PausableMailbox implements Mailbox {
        final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        boolean paused;
        boolean rejecting;
        @Override public boolean offer(Runnable task) {
            if (rejecting) return false;
            if (paused) tasks.add(task); else task.run();
            return true;
        }
        @Override public int capacity() { return Integer.MAX_VALUE; }
        void resume() {
            paused = false;
            while (!tasks.isEmpty()) tasks.removeFirst().run();
        }
    }
}
