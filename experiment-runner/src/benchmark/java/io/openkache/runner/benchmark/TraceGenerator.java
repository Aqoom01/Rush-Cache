package io.openkache.runner.benchmark;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * index로 재생성하는 논리 GET/SET/DELETE generator. 네트워크/IONIA 처리를 포함하지 않는다.
 * generator는 불변으로 공유 가능하며 반환한 Iterator는 한 thread가 소유한다.
 */
public final class TraceGenerator {
    private static final long OP_CHANNEL = 0x082efa98ec4e6c89L;
    private static final long KEY_CHANNEL = 0x452821e638d01377L;
    private static final long HOT_CHANNEL = 0xbe5466cf34e90c6cL;
    private static final long VALUE_CHANNEL = 0xc0ac29b7c97c50ddL;
    private final TraceConfig config;

    public TraceGenerator(TraceConfig config) { this.config = Objects.requireNonNull(config, "config"); }

    /** 생성기의 불변 설정. */
    public TraceConfig config() { return config; }

    /** phase 안의 globalIndex를 재생성한다. LOAD는 key마다 한 번씩 SET한다. */
    public LogicalRequest at(TracePhase phase, long globalIndex) {
        Objects.requireNonNull(phase, "phase");
        if (globalIndex < 0 || (phase == TracePhase.LOAD && globalIndex >= config.keyCount())) {
            throw new IllegalArgumentException("index outside phase");
        }
        int stream = (int) (globalIndex % config.streamCount());
        long sequence = globalIndex / config.streamCount();
        long client = (long) phase.tag() * config.streamCount() + stream;
        long root = IndexedRandom.mix(config.seed() ^ phase.salt() ^ IndexedRandom.mix(globalIndex));
        long keyIndex = globalIndex;
        int roll = 100; // LOAD는 항상 SET
        if (phase != TracePhase.LOAD) {
            keyIndex = new IndexedRandom(root ^ KEY_CHANNEL).bounded(config.keyCount());
            if (config.distribution() == TraceConfig.Distribution.FIXED_HOT
                    && new IndexedRandom(root ^ HOT_CHANNEL).unitDouble() < config.hotProbability()) {
                keyIndex = 0;
            }
            roll = (int) new IndexedRandom(root ^ OP_CHANNEL).bounded(100);
        }
        Bytes key = KeyValueCodec.key(keyIndex, config.keyBytes());
        Command command;
        if (phase != TracePhase.LOAD && roll < config.getPercent()) {
            command = new Command.Get(key);
        } else if (phase == TracePhase.LOAD || roll < config.getPercent() + config.setPercent()) {
            command = new Command.Set(key, KeyValueCodec.value(config.valueBytes(), config.seed(),
                    phase, client, sequence, keyIndex, root ^ VALUE_CHANNEL));
        } else {
            command = new Command.Delete(key);
        }
        return new LogicalRequest(phase, globalIndex, stream, client, sequence, command);
    }

    /**
     * stream별 globalIndex=streamId+k*streamCount를 소유한다. 나머지도 중복 없이 분배한다.
     * total은 모든 stream의 합계다. LOAD는 keyCount와 같아야 한다.
     */
    public Iterator<LogicalRequest> stream(TracePhase phase, long total, int streamId) {
        validateCount(phase, total);
        if (streamId < 0 || streamId >= config.streamCount()) {
            throw new IllegalArgumentException("invalid streamId");
        }
        return new Iterator<>() {
            private long nextIndex = streamId;
            private boolean available = nextIndex < total;

            @Override
            public boolean hasNext() { return available; }

            @Override
            public LogicalRequest next() {
                if (!available) { throw new NoSuchElementException(); }
                LogicalRequest request = at(phase, nextIndex);
                if (total - nextIndex <= config.streamCount()) {
                    available = false;
                } else {
                    nextIndex += config.streamCount();
                }
                return request;
            }
        };
    }

    void validateCount(TracePhase phase, long total) {
        Objects.requireNonNull(phase, "phase");
        if (total < 0 || (phase == TracePhase.LOAD && total != config.keyCount())) {
            throw new IllegalArgumentException("invalid phase total");
        }
    }
}

