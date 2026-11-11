package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.*;

import io.openkache.core.migration.MigrationScheduler.Status;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MigrationAdmissionTest {
    @ParameterizedTest
    @ValueSource(strings = {"mongodb", "slamig"})
    void policiesSerializeMovesFromSharedSource(String policy) {
        var jobs = List.of(job(0, 1), job(1, 2));
        var scheduler = scheduler(policy, 4, 4, jobs, MigrationAdmissionTest::runningPeer);
        assertTrue(scheduler.step(0));
        assertTrue(scheduler.step(1));
        assertEquals(1, scheduler.maxObservedConcurrent);
        assertEquals(1, scheduler.admissionOrder.size());
    }

    @Test
    void fifoHonorsGlobalAndNodeLimits() {
        var jobs = List.of(job(0, 1), job(1, 2), job(2, 3));
        var global = scheduler("fifo", 2, 4, jobs, MigrationAdmissionTest::runningPeer);
        global.step(0);
        global.step(1);
        assertEquals(List.of(0L, 1L), global.admissionOrder);
        assertEquals(2, global.maxObservedConcurrent);

        var perNode = scheduler("fifo", 4, 1, List.of(job(0, 1), job(1, 2)), MigrationAdmissionTest::runningPeer);
        perNode.step(0);
        assertEquals(1, perNode.maxObservedConcurrent);
    }

    @Test
    void busyStartReturnsToQueueAndRetriesOnNextPoll() {
        var job = job(0, 1);
        var calls = new AtomicInteger();
        var scheduler = scheduler("fifo", 4, 4, List.of(job), (node, op, body) ->
                calls.getAndIncrement() == 0
                        ? response(WireProtocol.STATUS_UNAVAILABLE, new byte[0], "tablet already moving")
                        : runningPeer(node, op, body));
        assertTrue(scheduler.step(0));
        assertEquals(Status.QUEUED, job.state);
        assertEquals(1, job.retries);
        assertTrue(scheduler.admissionOrder.isEmpty());
        assertTrue(scheduler.step(1));
        assertEquals(Status.RUNNING, job.state);
        assertEquals(List.of(0L), scheduler.admissionOrder);
    }

    private static MigrationAdmission scheduler(String policy, int global, int perNode,
                                                List<MigrationAdmission.Job> jobs, MigrationAdmission.Peer peer) {
        return new MigrationAdmission(new ClusterConfig.Scheduler(policy, global, perNode, 100), jobs, 0, peer, () -> 0);
    }

    private static MigrationAdmission.Job job(long tablet, long to) {
        return new MigrationAdmission.Job(new ClusterConfig.Migration(0, tablet, to, ClusterConfig.Method.PRE_COPY,
                0, 65536, 4096, 10), new Change(new TabletId(tablet), new NodeId(0), new NodeId(to)), 1024, null);
    }

    private static WireClient.Response runningPeer(long node, int op, byte[] body) {
        byte[] payload = op == ClusterRunner.OP_MIGRATE ? ByteBuffer.allocate(8).putLong(42).array() : new byte[]{1};
        return response(WireProtocol.STATUS_OK, payload, "");
    }

    private static WireClient.Response response(int status, byte[] value, String detail) {
        return new WireClient.Response(status, 0, value, detail, 0, new byte[0]);
    }
}
