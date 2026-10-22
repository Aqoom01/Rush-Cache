package io.openkache.core.replication.primarybackup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import java.util.List;
import org.junit.jupiter.api.Test;

class PrimaryBackupConfigTest {

    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);

    @Test
    void primaryAndBackupRoles() {
        PrimaryBackupConfig primary = new PrimaryBackupConfig(N1, N1, List.of(N2, N3), OwnershipEpoch.ZERO);
        PrimaryBackupConfig backup = new PrimaryBackupConfig(N3, N1, List.of(N2, N3), OwnershipEpoch.ZERO);

        assertTrue(primary.isPrimary());
        assertFalse(backup.isPrimary());
        assertEquals(2, primary.requiredAcks());
        assertEquals(List.of(N2, N3), backup.backupIds());
    }

    @Test
    void rejectsInvalidGroups() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PrimaryBackupConfig(N1, N1, List.of(), OwnershipEpoch.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PrimaryBackupConfig(N1, N1, List.of(N2, N2), OwnershipEpoch.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PrimaryBackupConfig(N1, N1, List.of(N1, N2), OwnershipEpoch.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PrimaryBackupConfig(new NodeId(9), N1, List.of(N2, N3), OwnershipEpoch.ZERO));
    }
}
