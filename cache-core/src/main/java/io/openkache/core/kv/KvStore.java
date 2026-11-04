package io.openkache.core.kv;

import io.openkache.core.model.Bytes;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * 한 worker가 소유한 key-value 저장소.
 *
 * <p>구현은 동기화하지 않는다. {@link KvEngine}이 owner worker의 thread에서만 호출한다는 전제로 동작한다. 예상 가능한 실패는
 * {@link StoreException}으로 알리고 {@link KvEngine}이 상태값으로 바꾼다.
 */
interface KvStore extends AutoCloseable {

    /**
     * @param value 저장된 값
     * @param version 이 값이 쓰여졌을 때의 버전 (1부터 시작, SET마다 1 증가)
     */
    record Entry(Bytes value, long version) {}

    /** worker 하나의 scan 결과. */
    record Partial(List<KvEntry> entries, boolean truncated) {}

    Optional<Entry> get(Bytes key) throws StoreException;

    /** {@code key}에 {@code value}를 쓰고 새 버전을 반환한다. 같은 Key가 있으면 덮어쓴다. */
    long set(Bytes key, Bytes value) throws StoreException;

    /** {@code key}를 삭제한다. 있었으면 {@code true}. */
    boolean delete(Bytes key);

    int size();

    /**
     * {@code keyFilter}를 통과한 항목을 {@link KvEntry#KEY_ORDER}로 정렬해 앞에서 {@code limit}개까지 돌려준다. 전체 결과가
     * Key 순서로 앞 {@code limit}개여야 하므로 worker마다 자기 몫의 앞 {@code limit}개를 내면 합쳐서 다시 자를 수 있다.
     *
     * @return 정렬된 항목과, 이 worker에서 {@code limit}에 걸려 잘린 항목이 있었는지
     */
    Partial scan(Predicate<Bytes> keyFilter, int limit) throws StoreException;

    /** 파일 등 자원을 닫는다. 여러 번 호출해도 안전하다. */
    @Override
    void close();

    /** {@code matched}를 Key 순서로 정렬해 앞 {@code limit}개만 남긴다. {@code matched}를 변경한다. */
    static Partial sorted(List<KvEntry> matched, int limit) {
        matched.sort((a, b) -> KvEntry.KEY_ORDER.compare(a.key(), b.key()));
        boolean truncated = matched.size() > limit;
        return new Partial(truncated ? List.copyOf(matched.subList(0, limit)) : matched, truncated);
    }
}
