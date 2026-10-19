package io.openkache.core.kv;

import io.openkache.core.model.Bytes;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 한 worker가 소유한 key-value 저장소.
 *
 * <p>이 클래스는 동기화하지 않는다. {@link KvEngine}이 owner worker의 thread에서만 호출한다는 전제로 동작한다.
 * TTL, eviction, 저장 용량 한도는 아직 합의되지 않아 구현하지 않는다.
 */
final class KvStore {

    private final Map<Bytes, Entry> entries = new HashMap<>();

    /**
     * @param value 저장된 값
     * @param version 이 값이 쓰여졌을 때의 버전 (1부터 시작, SET마다 1 증가)
     */
    record Entry(Bytes value, long version) {}

    Optional<Entry> get(Bytes key) {
        return Optional.ofNullable(entries.get(key));
    }

    /** {@code key}에 {@code value}를 쓰고 새 버전을 반환한다. 같은 Key가 있으면 덮어쓴다. */
    long set(Bytes key, Bytes value) {
        long nextVersion = entries.containsKey(key) ? entries.get(key).version() + 1 : 1;
        entries.put(key, new Entry(value, nextVersion));
        return nextVersion;
    }

    /** {@code key}를 삭제한다. 있었으면 {@code true}. */
    boolean delete(Bytes key) {
        return entries.remove(key) != null;
    }

    int size() {
        return entries.size();
    }
}
