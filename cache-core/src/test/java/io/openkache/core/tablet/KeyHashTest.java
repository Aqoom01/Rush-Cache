package io.openkache.core.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.openkache.core.model.Bytes;
import org.junit.jupiter.api.Test;

class KeyHashTest {

    /** MurmurHash3 x64_128("hello", seed 0)의 앞 64비트. 공개 구현들과 대조한 값. */
    @Test
    void matchesPublicTestVector() {
        assertEquals(0xcbd8a7b341bd9b02L, KeyHash.hash64(Bytes.utf8("hello"), 0));
    }

    /** 별도 Python 구현(scratch)으로 뽑은 값. seed는 {@link KeyHash#SEED}. 블록 경계(16, 17바이트)와 긴 입력을 포함한다. */
    @Test
    void matchesIndependentReferenceImplementation() {
        assertEquals(0x613ed601cd9cbe97L, KeyHash.of(Bytes.empty()));
        assertEquals(0xd0d48859b9473e45L, KeyHash.of(Bytes.utf8("a")));
        assertEquals(0xeeb48164d4a9a373L, KeyHash.of(Bytes.utf8("user:1000")));
        assertEquals(0x22c3cdab0652bb0cL, KeyHash.of(Bytes.utf8("0123456789abcdef")));
        assertEquals(0x72fb8e2512e1353fL, KeyHash.of(Bytes.utf8("0123456789abcdef0")));
        assertEquals(0xcc95a87c1fa45565L, KeyHash.of(Bytes.utf8("The quick brown fox jumps over the lazy dog")));
        assertEquals(0xae072819d65ea298L, KeyHash.of(sequence(1)));
        assertEquals(0x720e100208324778L, KeyHash.of(sequence(7)));
        assertEquals(0xf6d0d06e4e527eefL, KeyHash.of(sequence(8)));
        assertEquals(0xaab655fbe2723271L, KeyHash.of(sequence(9)));
        assertEquals(0xd9a41a6ff431369cL, KeyHash.of(sequence(15)));
        assertEquals(0x963d7a53a02e32bcL, KeyHash.of(sequence(16)));
        assertEquals(0x4608f9fa91197eb2L, KeyHash.of(sequence(31)));
        assertEquals(0xb69272632e666ce2L, KeyHash.of(sequence(32)));
        assertEquals(0xbeb8080bc6deb663L, KeyHash.of(sequence(40)));
    }

    /** 바이트 0, 1, 2, ... len-1. */
    private static Bytes sequence(int len) {
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = (byte) i;
        }
        return Bytes.copyOf(data);
    }

    @Test
    void emptyKeyHasAHash() {
        assertEquals(KeyHash.of(Bytes.empty()), KeyHash.of(Bytes.copyOf(new byte[0])));
    }

    @Test
    void sameKeyGivesSameHash() {
        assertEquals(KeyHash.of(Bytes.utf8("user:1000")), KeyHash.of(Bytes.utf8("user:1000")));
    }

    @Test
    void differentKeysGiveDifferentHashes() {
        assertNotEquals(KeyHash.of(Bytes.utf8("user:1000")), KeyHash.of(Bytes.utf8("user:1001")));
    }

    /** 16바이트 블록 경계와 tail 길이 0~15를 모두 지나며 예외가 없고 길이마다 값이 다른지 본다. */
    @Test
    void handlesEveryTailLength() {
        long previous = KeyHash.of(Bytes.empty());
        for (int len = 1; len <= 40; len++) {
            byte[] data = new byte[len];
            for (int i = 0; i < len; i++) {
                data[i] = (byte) i;
            }
            long h = KeyHash.of(Bytes.copyOf(data));
            assertNotEquals(previous, h, "length " + len);
            previous = h;
        }
    }
}
