package io.openkache.runner.benchmark;

/** v1 고정 SplitMix64 산술. long overflow는 의도한 modulo 2^64 연산이다. */
final class IndexedRandom {
    private long state;

    IndexedRandom(long seed) { state = seed; }

    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    long nextLong() {
        state += 0x9e3779b97f4a7c15L;
        return mix(state);
    }

    long bounded(long bound) {
        if (bound <= 0) { throw new IllegalArgumentException("bound must be positive"); }
        long bits;
        long value;
        do {
            bits = nextLong() >>> 1;
            value = bits % bound;
        } while (bits + (bound - 1) - value < 0);
        return value;
    }

    double unitDouble() { return (nextLong() >>> 11) * 0x1.0p-53; }
}

