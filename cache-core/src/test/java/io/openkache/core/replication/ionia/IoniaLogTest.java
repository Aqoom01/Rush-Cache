package io.openkache.core.replication.ionia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import java.util.List;
import org.junit.jupiter.api.Test;

class IoniaLogTest {
    private static final ClientId CLIENT = new ClientId(1000);
    private static final Command.Set SET = new Command.Set(Bytes.utf8("a"), Bytes.utf8("hello"));
    private final IoniaLog log = new IoniaLog();

    @Test
    void recordsDoNotImplyOrderCommitOrApplyAndRetriesDoNotDuplicate() {
        log.record(CLIENT, new OperationId(1), SET);
        log.record(CLIENT, new OperationId(1), SET);
        assertEquals(1, log.recordedCount());
        assertEquals(0, log.lastLogIndex());
        assertEquals(0, log.commitIndex());
        assertEquals(0, log.appliedIndex());
        assertEquals(log.order(CLIENT, new OperationId(1), SET), log.order(CLIENT, new OperationId(1), SET));
        assertEquals(1, log.lastLogIndex());
    }

    @Test
    void rejectsReusingAnIdentityForAnotherPayloadAndRejectsReads() {
        log.record(CLIENT, new OperationId(1), SET);
        assertThrows(IllegalArgumentException.class,
                () -> log.record(CLIENT, new OperationId(1), new Command.Delete(SET.key())));
        assertThrows(IllegalArgumentException.class,
                () -> log.record(CLIENT, new OperationId(2), new Command.Get(SET.key())));
        assertEquals(1, log.recordedCount());
    }

    @Test
    void validatesTheWholeBatchBeforeChangingAnyRecord() {
        log.record(CLIENT, new OperationId(2), SET);
        IoniaMessage.LogEntry conflict = new IoniaMessage.LogEntry(
                2, CLIENT, new OperationId(2), new Command.Delete(SET.key()));
        assertThrows(IllegalArgumentException.class,
                () -> log.accept(new IoniaMessage.Prepare(0, List.of(entry(1), conflict))));
        assertEquals(0, log.lastLogIndex());
        assertEquals(1, log.recordedCount());
    }

    @Test
    void acceptsMatchingOverlapAndProtectsOrderAndRequestIdentity() {
        log.accept(new IoniaMessage.Prepare(0, List.of(entry(1))));
        log.accept(new IoniaMessage.Prepare(0, List.of(entry(1), entry(2))));
        log.accept(new IoniaMessage.Prepare(0, List.of(entry(1))));
        assertEquals(2, log.lastLogIndex());
        assertEquals(2, log.recordedCount());
        assertThrows(IllegalArgumentException.class,
                () -> log.accept(new IoniaMessage.Prepare(0, List.of(entry(4)))));
        assertThrows(IllegalArgumentException.class, () -> log.accept(new IoniaMessage.Prepare(0, List.of(
                new IoniaMessage.LogEntry(3, CLIENT, new OperationId(1), SET)))));
        assertEquals(2, log.lastLogIndex());
    }

    @Test
    void rejectsDuplicateRequestIdsInsideOneBatchWithoutPartialMutation() {
        assertThrows(IllegalArgumentException.class, () -> log.accept(new IoniaMessage.Prepare(0, List.of(
                entry(1), new IoniaMessage.LogEntry(2, CLIENT, new OperationId(1), SET)))));
        assertEquals(0, log.recordedCount());
        assertEquals(0, log.lastLogIndex());
    }

    @Test
    void appliedAndCommittedIndicesMustBeContiguousAndNeverMoveBackwards() {
        log.accept(new IoniaMessage.Prepare(0, List.of(entry(1), entry(2))));
        assertThrows(IllegalArgumentException.class, () -> log.commitThrough(3));
        assertThrows(IllegalArgumentException.class, () -> log.markApplied(1));
        log.commitThrough(2);
        assertThrows(IllegalArgumentException.class, () -> log.markApplied(2));
        log.markApplied(1);
        log.markApplied(1);
        log.markApplied(2);
        log.commitThrough(1);
        assertEquals(2, log.commitIndex());
        assertEquals(2, log.appliedIndex());
    }

    @Test
    void exposesImmutableSuffixesAndExplicitlyMissingEntries() {
        log.accept(new IoniaMessage.Prepare(0, List.of(entry(1), entry(2))));
        assertEquals(List.of(entry(2)), log.entriesFrom(2));
        assertThrows(UnsupportedOperationException.class, () -> log.entriesFrom(1).clear());
        assertTrue(log.entriesFrom(3).isEmpty());
        assertTrue(log.entry(3).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> log.entriesFrom(0));
        assertThrows(IllegalArgumentException.class, () -> log.commitThrough(-1));
        assertThrows(IllegalArgumentException.class, () -> log.markApplied(0));
    }

    private static IoniaMessage.LogEntry entry(long index) {
        return new IoniaMessage.LogEntry(index, CLIENT, new OperationId(index), SET);
    }
}
