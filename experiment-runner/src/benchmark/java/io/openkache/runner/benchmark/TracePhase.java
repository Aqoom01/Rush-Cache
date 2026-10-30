package io.openkache.runner.benchmark;

/** 요청열의 독립 namespace. 불변이며 여러 thread에서 공유할 수 있다. */
public enum TracePhase {
    LOAD(0, 0x243f6a8885a308d3L),
    WARMUP(1, 0x13198a2e03707344L),
    MEASURE(2, 0xa4093822299f31d0L);

    private final int tag;
    private final long salt;

    TracePhase(int tag, long salt) {
        this.tag = tag;
        this.salt = salt;
    }

    /** binary v1의 고정 phase 번호. ordinal에 의존하지 않는다. */
    public int tag() { return tag; }

    long salt() { return salt; }

    static TracePhase fromTag(int tag) {
        for (TracePhase phase : values()) {
            if (phase.tag == tag) { return phase; }
        }
        throw new IllegalArgumentException("unknown phase tag: " + tag);
    }
}

