package io.openkache.core.kv;

import io.openkache.core.model.Bytes;
import java.util.Comparator;
import java.util.Objects;

/**
 * SCAN/RANGE가 돌려주는 저장 항목 하나. 불변이며 {@code value}는 저장소의 복사본이 아니라 같은 {@link Bytes} 인스턴스지만,
 * {@link Bytes} 자체가 불변이므로 호출자가 바꿀 수 없다.
 *
 * @param key Key
 * @param value 저장된 값
 * @param version 이 값이 쓰였을 때의 버전(SET마다 1 증가, 1부터)
 */
public record KvEntry(Bytes key, Bytes value, long version) {

    /**
     * SCAN/RANGE 결과 정렬에 쓰는 Key 순서. 부호 없는 바이트 사전순이며, 앞부분이 같으면 짧은 쪽이 먼저다. {@code
     * core.balance.KeyRange#ORDER}와 같은 순서지만 공통 모듈이 연구 패키지를 참조하지 않도록 따로 둔다.
     */
    public static final Comparator<Bytes> KEY_ORDER = KvEntry::compareKeys;

    public KvEntry {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1: " + version);
        }
    }

    private static int compareKeys(Bytes a, Bytes b) {
        int shared = Math.min(a.size(), b.size());
        for (int i = 0; i < shared; i++) {
            int diff = Byte.toUnsignedInt(a.get(i)) - Byte.toUnsignedInt(b.get(i));
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(a.size(), b.size());
    }
}
