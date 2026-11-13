package io.openkache.core.integration;

import io.openkache.core.model.Bytes;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class IntegratedClusterTest {
    private static SystemVariant variant(String replication, String distribution, String split, String copy) {
        return new SystemVariant(replication, "SWIM", distribution, split, "GREEDY", copy);
    }
    @Test void rejectsUnsupportedAndIncompatibleMethods() {
        assertThrows(IllegalArgumentException.class, () -> variant("IONIA", "RANGE", "HEAT", "PRE"));
        assertThrows(IllegalArgumentException.class, () -> variant("PB", "SLOT", "HEAT", "PRE"));
        assertEquals(120, SystemVariant.all().stream().distinct().count());
    }
    @Test void stopAndPrecopyActuallyMigrateThroughReplicatedDestination() {
        for (String copy : new String[]{"STOP", "PRE"}) {
            try (var cluster = new IntegratedCluster(variant("PB", "RANGE", "HEAT", copy), "hotspot", 1)) {
                var result = cluster.run();
                assertTrue(result.safe(), result.toString());
                assertTrue(result.migrationBytes() > 0);
                assertTrue(result.completedMoves() > 0);
                assertEquals(result.plannedMoves(), result.completedMoves());
                assertTrue(result.freezeMs() > 0);
                for (var entry : cluster.oracle.entrySet()) {
                    long group = cluster.table.lookup(cluster.variant.scheme().keyHash(entry.getKey())).owner().value();
                    for (var replica : IntegratedCluster.members(group))
                        assertTrue(cluster.validValue(entry.getKey(), cluster.stores.get(replica).data.get(entry.getKey())));
                }
                assertEquals(result.offered(), result.ok() + result.rejected() + result.pending());
            }
        }
    }
    @Test void crashDuringMigrationRecoversWithoutLosingAcknowledgedValues() {
        for (String replication : new String[]{"PB", "RAFT"}) {
            try (var cluster = new IntegratedCluster(variant(replication, "RANGE", "HEAT", "PRE"), "crash-overlap", 1)) {
                var result = cluster.run();
                assertTrue(result.safe(), result.toString());
                assertTrue(result.recoveryMs() >= 0);
                assertTrue(result.detectorBytes() > 0);
                assertEquals(1, cluster.dead.size());
                assertEquals(result.plannedMoves(), result.completedMoves());
            }
        }
    }
    @Test void workloadIsPairedAcrossMethodsAndRunsAreDeterministic() {
        IntegratedCluster.Measurement first, second, other;
        try (var cluster = new IntegratedCluster(variant("PB", "RANGE", "HEAT", "PRE"), "hotspot", 2)) { first = cluster.run(); }
        try (var cluster = new IntegratedCluster(variant("PB", "RANGE", "HEAT", "PRE"), "hotspot", 2)) { second = cluster.run(); }
        try (var cluster = new IntegratedCluster(variant("RAFT", "SLOT", "NONE", "STOP"), "hotspot", 2)) { other = cluster.run(); }
        assertEquals(first, second);
        assertEquals(first.tapeHash(), other.tapeHash());
        assertEquals(first.offered(), other.offered());
    }
    @Test void oracleRejectsLossAndAllowsLaterAmbiguousWrites() {
        try (var cluster = new IntegratedCluster(variant("PB", "RANGE", "NONE", "PRE"), "uniform", 1)) {
            Bytes key = Bytes.utf8("k"), old = Bytes.utf8("old"), ack = Bytes.utf8("ack"), ambiguous = Bytes.utf8("ambiguous");
            cluster.issued.put(key, java.util.Map.of(old, 1L, ack, 2L, ambiguous, 3L));
            cluster.acknowledged.put(key, 2L);
            assertFalse(cluster.validValue(key, old));
            assertFalse(cluster.validValue(key, null));
            assertFalse(cluster.validValue(key, Bytes.utf8("unissued")));
            assertTrue(cluster.validValue(key, ack));
            assertTrue(cluster.validValue(key, ambiguous));
        }
    }
    @Test void crashStopsAlreadyQueuedLocalKvWork() {
        try (var cluster = new IntegratedCluster(variant("PB", "RANGE", "NONE", "PRE"), "uniform", 1)) {
            var node = IntegratedCluster.members(1).getFirst();
            var key = Bytes.utf8("queued-before-crash");
            var context = new io.openkache.core.api.RequestContext(new io.openkache.core.model.ClientId(1),
                    new io.openkache.core.model.OperationId(1), io.openkache.core.model.RoutingVersion.ZERO,
                    io.openkache.core.model.OwnershipEpoch.ZERO, new io.openkache.core.api.Deadline(Long.MAX_VALUE));
            var result = cluster.stores.get(node).execute(new io.openkache.core.api.Command.Set(key, Bytes.utf8("v")), context);
            cluster.dead.add(node);
            cluster.advance(5);
            assertEquals(io.openkache.core.api.ResultStatus.UNAVAILABLE, result.toCompletableFuture().join().status());
            assertFalse(cluster.stores.get(node).data.containsKey(key));
        }
    }
    @Test void smallSeedIntervalsUseStudentTAndCannotClaimCertaintyFromOneSeed() {
        assertTrue(Double.isNaN(SystemIntegrationMain.ci(new double[]{1})));
        assertEquals(0, SystemIntegrationMain.ci(new double[]{2, 2, 2}));
        assertEquals(2.484, SystemIntegrationMain.ci(new double[]{1, 2, 3}), .001);
    }
}
