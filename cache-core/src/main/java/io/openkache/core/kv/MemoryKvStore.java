package io.openkache.core.kv;

import io.openkache.core.model.Bytes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/** {@code HashMap} 저장소. TTL, eviction, 저장 용량 한도는 아직 합의되지 않아 구현하지 않는다. */
final class MemoryKvStore implements KvStore {

    private final Map<Bytes, Entry> entries = new HashMap<>();

    @Override
    public Optional<Entry> get(Bytes key) {
        return Optional.ofNullable(entries.get(key));
    }

    @Override
    public long set(Bytes key, Bytes value) {
        long nextVersion = entries.containsKey(key) ? entries.get(key).version() + 1 : 1;
        entries.put(key, new Entry(value, nextVersion));
        return nextVersion;
    }

    @Override
    public boolean delete(Bytes key) {
        return entries.remove(key) != null;
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public Partial scan(Predicate<Bytes> keyFilter, int limit) {
        List<KvEntry> matched = new ArrayList<>();
        for (Map.Entry<Bytes, Entry> e : entries.entrySet()) {
            if (keyFilter.test(e.getKey())) {
                matched.add(new KvEntry(e.getKey(), e.getValue().value(), e.getValue().version()));
            }
        }
        return KvStore.sorted(matched, limit);
    }

    @Override
    public void close() {}
}
