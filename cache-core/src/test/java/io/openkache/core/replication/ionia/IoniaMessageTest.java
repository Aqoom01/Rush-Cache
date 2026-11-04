package io.openkache.core.replication.ionia;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.replication.ionia.IoniaMessage.Commit;
import io.openkache.core.replication.ionia.IoniaMessage.LogEntry;
import io.openkache.core.replication.ionia.IoniaMessage.Prepare;
import io.openkache.core.replication.ionia.IoniaMessage.PrepareAck;
import io.openkache.core.spi.PeerEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class IoniaMessageTest {

    private static final ClientId CLIENT = new ClientId(10);
    private static final Bytes KEY = Bytes.utf8("key");
    private static final Command.Set SET = new Command.Set(KEY, Bytes.utf8("value"));

    @Test
    void carriesWriteIdentityAndOrderThroughTheCommonPeerEnvelope() {
        LogEntry set = entry(5);
        LogEntry delete = new LogEntry(6, CLIENT, new OperationId(6), new Command.Delete(KEY));
        Prepare prepare = new Prepare(2, List.of(set, delete));
        PeerEnvelope envelope = new PeerEnvelope(new NodeId(1), new NodeId(2), prepare);

        assertEquals(prepare, envelope.message());
        assertEquals(2, prepare.view());
        assertEquals(CLIENT, prepare.entries().getFirst().clientId());
        assertEquals(new OperationId(5), prepare.entries().getFirst().operationId());
        assertEquals(SET, prepare.entries().getFirst().command());
        assertEquals(delete, prepare.entries().getLast());
    }

    @Test
    void keepsPrepareEntriesIndependentOfTheSendersMutableList() {
        List<LogEntry> input = new ArrayList<>(List.of(entry(1), entry(2)));
        Prepare prepare = new Prepare(0, input);

        input.clear();
        assertEquals(List.of(entry(1), entry(2)), prepare.entries());
        assertThrows(UnsupportedOperationException.class, () -> prepare.entries().clear());
    }

    @Test
    void rejectsReadsInTheWriteLog() {
        assertThrows(IllegalArgumentException.class,
                () -> new LogEntry(1, CLIENT, new OperationId(1), new Command.Get(KEY)));
    }

    @Test
    void rejectsAnEmptyPrepareBatch() {
        assertThrows(IllegalArgumentException.class, () -> new Prepare(0, List.of()));
    }

    @Test
    void rejectsGapsRepeatedIndicesAndReversedOrder() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new Prepare(0, List.of(entry(1), entry(3)))),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new Prepare(0, List.of(entry(1), entry(1)))),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new Prepare(0, List.of(entry(2), entry(1)))));
    }

    @Test
    void handlesTheLargestLogIndexWithoutWrapping() {
        Prepare prepare = new Prepare(0, List.of(entry(Long.MAX_VALUE - 1), entry(Long.MAX_VALUE)));

        assertEquals(Long.MAX_VALUE, prepare.entries().getLast().index());
        assertThrows(IllegalArgumentException.class,
                () -> new Prepare(0, List.of(entry(Long.MAX_VALUE), entry(1))));
    }

    @Test
    void rejectsNonPositiveLogAndAcknowledgedIndices() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> entry(0)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new LogEntry(-1, CLIENT, new OperationId(1), SET)),
                () -> assertThrows(IllegalArgumentException.class, () -> new PrepareAck(0, 0)),
                () -> assertThrows(IllegalArgumentException.class, () -> new PrepareAck(0, -1)));
    }

    @Test
    void rejectsNegativeViewsForEveryPeerMessage() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new Prepare(-1, List.of(entry(1)))),
                () -> assertThrows(IllegalArgumentException.class, () -> new PrepareAck(-1, 1)),
                () -> assertThrows(IllegalArgumentException.class, () -> new Commit(-1, 0)));
    }

    @Test
    void acceptsAnEmptyCommitPrefixAndPositiveAcknowledgments() {
        assertEquals(0, new Commit(0, 0).commitIndex());
        assertEquals(8, new Commit(3, 8).commitIndex());
        assertEquals(8, new PrepareAck(3, 8).lastLogIndex());
        assertThrows(IllegalArgumentException.class, () -> new Commit(0, -1));
    }

    @Test
    void rejectsMissingLogIdentityPayloadAndBatchEntries() {
        assertAll(
                () -> assertThrows(NullPointerException.class,
                        () -> new LogEntry(1, null, new OperationId(1), SET)),
                () -> assertThrows(NullPointerException.class,
                        () -> new LogEntry(1, CLIENT, null, SET)),
                () -> assertThrows(NullPointerException.class,
                        () -> new LogEntry(1, CLIENT, new OperationId(1), null)),
                () -> assertThrows(NullPointerException.class, () -> new Prepare(0, null)),
                () -> assertThrows(NullPointerException.class,
                        () -> new Prepare(0, Arrays.asList(entry(1), null))));
    }

    private static LogEntry entry(long index) {
        return new LogEntry(index, CLIENT, new OperationId(index), SET);
    }
}
