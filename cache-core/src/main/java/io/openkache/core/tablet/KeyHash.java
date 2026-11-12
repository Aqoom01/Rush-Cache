package io.openkache.core.tablet;

import io.openkache.core.model.Bytes;

/**
 * Key를 해시 공간(부호 없는 64비트)의 한 점으로 바꾼다. MurmurHash3 x64_128의 앞 64비트다.
 *
 * <p>클라이언트와 서버가 같은 Key에 대해 반드시 같은 값을 내야 하므로 seed는 상수로 고정한다. 모든 variant가 같은 해시를 쓰므로
 * 분배 방식 비교에서 해시 함수의 차이는 변수가 아니다. JDK만 사용한다(cache-core 의존성 규칙).
 *
 * <p>상태가 없으므로 어느 thread에서나 호출할 수 있다. 반환값은 부호 없는 값이므로 비교는 {@link Long#compareUnsigned}로 한다.
 */
public final class KeyHash {

    /** 해시 seed. 바꾸면 모든 Key의 Tablet이 달라지므로 클러스터 수명 동안 고정이다. */
    public static final long SEED = 0x4F4B_0001L;

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private KeyHash() {}

    /** {@code key}의 해시값. 부호 없는 64비트로 해석한다. */
    public static long of(Bytes key) {
        return hash64(key, SEED);
    }

    /**
     * MurmurHash3 x64_128의 첫 64비트. {@code key}를 복사하지 않고 읽는다.
     *
     * <p>공개 테스트 값: {@code "hello"}, seed 0 → {@code 0xcbd8a7b341bd9b02}.
     */
    @SuppressWarnings("fallthrough")
    static long hash64(Bytes key, long seed) {
        long h1 = seed;
        long h2 = seed;
        int len = key.size();
        int nblocks = len / 16;
        for (int i = 0; i < nblocks; i++) {
            long k1 = getLongLe(key, i * 16);
            long k2 = getLongLe(key, i * 16 + 8);
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        long k1 = 0;
        long k2 = 0;
        int t = nblocks * 16;
        switch (len & 15) {
            case 15: k2 ^= (long) (key.get(t + 14) & 0xff) << 48;
            case 14: k2 ^= (long) (key.get(t + 13) & 0xff) << 40;
            case 13: k2 ^= (long) (key.get(t + 12) & 0xff) << 32;
            case 12: k2 ^= (long) (key.get(t + 11) & 0xff) << 24;
            case 11: k2 ^= (long) (key.get(t + 10) & 0xff) << 16;
            case 10: k2 ^= (long) (key.get(t + 9) & 0xff) << 8;
            case 9:
                k2 ^= key.get(t + 8) & 0xff;
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;
            case 8: k1 ^= (long) (key.get(t + 7) & 0xff) << 56;
            case 7: k1 ^= (long) (key.get(t + 6) & 0xff) << 48;
            case 6: k1 ^= (long) (key.get(t + 5) & 0xff) << 40;
            case 5: k1 ^= (long) (key.get(t + 4) & 0xff) << 32;
            case 4: k1 ^= (long) (key.get(t + 3) & 0xff) << 24;
            case 3: k1 ^= (long) (key.get(t + 2) & 0xff) << 16;
            case 2: k1 ^= (long) (key.get(t + 1) & 0xff) << 8;
            case 1:
                k1 ^= key.get(t) & 0xff;
                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
            default:
                break;
        }
        h1 ^= len;
        h2 ^= len;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        return h1;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long getLongLe(Bytes b, int i) {
        long v = 0;
        for (int j = 7; j >= 0; j--) {
            v = (v << 8) | (b.get(i + j) & 0xffL);
        }
        return v;
    }
}
