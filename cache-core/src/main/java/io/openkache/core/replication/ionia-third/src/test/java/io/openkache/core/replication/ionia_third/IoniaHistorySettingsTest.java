package io.openkache.core.replication.ionia_third;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaConfig;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaHistorySettingsTest {
    @Test void V02_02_targetIsRequiredAndBounded() {
        for (String invalid : Arrays.asList(null, "", "-1", "2147483648", "1.0", "+1", " 1", "N"))
            assertThrows(IllegalArgumentException.class, () -> IoniaHistorySettings.parse(invalid), "input=" + invalid);
        assertThrows(IllegalArgumentException.class, () -> new IoniaHistorySettings(-1));
        assertEquals(0, IoniaHistorySettings.parse("0").targetEntries());
        assertEquals(Integer.MAX_VALUE, IoniaHistorySettings.parse("2147483647").targetEntries());
    }
    @Test void V02_16_profileIsCanonicalFixedLengthAndSeparatesMemberships() {
        IoniaConfig membership = new IoniaConfig(new NodeId(1), new NodeId(1), List.of(new NodeId(1), new NodeId(2), new NodeId(3)), 0);
        String first = new IoniaHistorySettings(0).configurationId(membership);
        String last = new IoniaHistorySettings(Integer.MAX_VALUE).configurationId(membership);
        assertEquals(first.length(), last.length()); assertNotEquals(first, last); assertNotEquals(membership.configurationId(), first);
        assertEquals(IoniaHistorySettings.parse("0001").profileId(), new IoniaHistorySettings(1).profileId());
        assertTrue(new IoniaHistorySettings(1).profileId().matches("[0-9a-f]{64}"));
        assertEquals("1387b0f3cf85efee05bf501bbf13f2a3cb16711a0b8022b7987b4d97c39d035f", new IoniaHistorySettings(1).profileId());
    }
}
