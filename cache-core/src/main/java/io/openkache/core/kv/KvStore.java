package io.openkache.core.kv;

import io.openkache.core.model.Bytes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

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

    /**
     * {@code keyFilter}를 통과한 항목을 {@link KvEntry#KEY_ORDER}로 정렬해 앞에서 {@code limit}개까지 돌려준다. 전체 결과가
     * Key 순서로 앞 {@code limit}개여야 하므로 worker마다 자기 몫의 앞 {@code limit}개를 내면 합쳐서 다시 자를 수 있다.
     *
     * @return 정렬된 항목과, 이 worker에서 {@code limit}에 걸려 잘린 항목이 있었는지
     */
    Partial scan(Predicate<Bytes> keyFilter, int limit) {
        List<KvEntry> matched = new ArrayList<>();
        for (Map.Entry<Bytes, Entry> e : entries.entrySet()) {
            if (keyFilter.test(e.getKey())) {
                matched.add(new KvEntry(e.getKey(), e.getValue().value(), e.getValue().version()));
            }
        }
        matched.sort((a, b) -> KvEntry.KEY_ORDER.compare(a.key(), b.key()));
        boolean truncated = matched.size() > limit;
        return new Partial(truncated ? List.copyOf(matched.subList(0, limit)) : matched, truncated);
    }

    /** worker 하나의 scan 결과. */
    record Partial(List<KvEntry> entries, boolean truncated) {}
}
