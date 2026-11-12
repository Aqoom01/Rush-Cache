package io.openkache.core.replication.primarybackup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage.Replicate;
import io.openkache.core.replication.primarybackup.PrimaryBackupMessage.ReplicateAck;
import org.junit.jupiter.api.Test;

class PrimaryBackupMessageTest {

    private static final Command.Set SET = new Command.Set(Bytes.utf8("k"), Bytes.utf8("v"));

    @Test
    void messagesCarryEpochAndOpNumber() {
        Replicate replicate = new Replicate(new OwnershipEpoch(3), 5, SET);
        ReplicateAck ack = new ReplicateAck(new OwnershipEpoch(3), 5);

        assertEquals(new OwnershipEpoch(3), replicate.epoch());
        assertEquals(5, replicate.opNumber());
        assertEquals(SET, replicate.command());
        assertEquals(replicate.epoch(), ack.epoch());
        assertEquals(replicate.opNumber(), ack.opNumber());
    }

    @Test
    void opNumberMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new Replicate(OwnershipEpoch.ZERO, 0, SET));
        assertThrows(IllegalArgumentException.class, () -> new ReplicateAck(OwnershipEpoch.ZERO, 0));
    }

    @Test
    void rejectsNulls() {
        assertThrows(NullPointerException.class, () -> new Replicate(null, 1, SET));
        assertThrows(NullPointerException.class, () -> new Replicate(OwnershipEpoch.ZERO, 1, null));
        assertThrows(NullPointerException.class, () -> new ReplicateAck(null, 1));
    }
}
