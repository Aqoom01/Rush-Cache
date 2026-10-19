package io.openkache.core.replication.ionia;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class IoniaConfigTest {

    private static final NodeId A = new NodeId(1);
    private static final NodeId B = new NodeId(2);
    private static final NodeId C = new NodeId(3);
    private static final NodeId OUTSIDER = new NodeId(4);
    private static final List<NodeId> REPLICAS = List.of(A, B, C);

    @Test
    void distinguishesTheFixedLeaderFromFollowers() {
        IoniaConfig leader = new IoniaConfig(A, A, REPLICAS, 0);
        IoniaConfig follower = new IoniaConfig(B, A, REPLICAS, 0);

        assertTrue(leader.isLeader());
        assertFalse(follower.isLeader());
        assertEquals(A, follower.leaderId());
        assertEquals(0, follower.view());
    }

    @Test
    void separatesFastWriteAndOrderingQuorums() {
        IoniaConfig config = new IoniaConfig(A, A, REPLICAS, 7);

        assertEquals(3, config.fastWriteQuorumSize());
        assertEquals(2, config.orderingQuorumSize());
        assertEquals(7, config.view());
    }

    @Test
    void keepsAnImmutableCopyOfReplicaMembership() {
        List<NodeId> input = new ArrayList<>(REPLICAS);
        IoniaConfig config = new IoniaConfig(A, A, input, 0);

        input.set(0, OUTSIDER);
        assertEquals(REPLICAS, config.replicas());
        assertThrows(UnsupportedOperationException.class, () -> config.replicas().set(0, OUTSIDER));
    }

    @Test
    void rejectsReplicaCountsOutsideTheThreeNodePrototype() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new IoniaConfig(A, A, List.of(), 0)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new IoniaConfig(A, A, List.of(A, B), 0)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new IoniaConfig(A, A, List.of(A, B, C, OUTSIDER), 0)));
    }

    @Test
    void rejectsDuplicateReplicaIds() {
        assertThrows(IllegalArgumentException.class,
                () -> new IoniaConfig(A, A, List.of(A, A, C), 0));
    }

    @Test
    void rejectsLocalNodeOutsideTheReplicationGroup() {
        assertThrows(IllegalArgumentException.class,
                () -> new IoniaConfig(OUTSIDER, A, REPLICAS, 0));
    }

    @Test
    void rejectsLeaderOutsideTheReplicationGroup() {
        assertThrows(IllegalArgumentException.class,
                () -> new IoniaConfig(A, OUTSIDER, REPLICAS, 0));
    }

    @Test
    void rejectsNegativeView() {
        assertThrows(IllegalArgumentException.class,
                () -> new IoniaConfig(A, A, REPLICAS, -1));
    }

    @Test
    void rejectsNullMembershipAndIdentifiers() {
        assertAll(
                () -> assertThrows(NullPointerException.class,
                        () -> new IoniaConfig(null, A, REPLICAS, 0)),
                () -> assertThrows(NullPointerException.class,
                        () -> new IoniaConfig(A, null, REPLICAS, 0)),
                () -> assertThrows(NullPointerException.class,
                        () -> new IoniaConfig(A, A, null, 0)),
                () -> assertThrows(NullPointerException.class,
                        () -> new IoniaConfig(A, A, Arrays.asList(A, B, null), 0)));
    }
}
