package io.openkache.core.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BytesTest {

    @Test
    void modifyingSourceAfterCreationDoesNotChangeContent() {
        byte[] source = {1, 2, 3};
        Bytes bytes = Bytes.copyOf(source);

        source[0] = 99;

        assertArrayEquals(new byte[] {1, 2, 3}, bytes.toByteArray());
    }

    @Test
    void modifyingReturnedArrayDoesNotChangeContent() {
        Bytes bytes = Bytes.copyOf(new byte[] {1, 2, 3});

        byte[] returned = bytes.toByteArray();
        returned[0] = 99;

        assertEquals(1, bytes.get(0));
        assertNotSame(returned, bytes.toByteArray());
    }

    @Test
    void equalityIsBasedOnContent() {
        Bytes a = Bytes.copyOf(new byte[] {1, 2, 3});
        Bytes b = Bytes.copyOf(new byte[] {1, 2, 3});

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, Bytes.copyOf(new byte[] {1, 2, 4}));
        assertNotEquals(a, Bytes.copyOf(new byte[] {1, 2}));
    }

    @Test
    void worksAsHashMapKey() {
        Map<Bytes, String> map = new HashMap<>();
        map.put(Bytes.utf8("key"), "value");

        assertEquals("value", map.get(Bytes.copyOf("key".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    @Test
    void emptyIsSharedAndEqualToEmptyCopy() {
        assertSame(Bytes.empty(), Bytes.copyOf(new byte[0]));
        assertTrue(Bytes.empty().isEmpty());
        assertEquals(0, Bytes.empty().size());
    }

    @Test
    void binaryDataRoundTrips() {
        byte[] all = new byte[256];
        for (int i = 0; i < all.length; i++) {
            all[i] = (byte) i;
        }

        assertArrayEquals(all, Bytes.copyOf(all).toByteArray());
    }

    @Test
    void rejectsNull() {
        assertThrows(NullPointerException.class, () -> Bytes.copyOf(null));
    }
}
