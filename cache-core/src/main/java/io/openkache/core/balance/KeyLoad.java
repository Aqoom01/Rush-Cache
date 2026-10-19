package io.openkache.core.balance;

import io.openkache.core.model.Bytes;
import java.util.Objects;

/**
 * 관찰 window 하나에서 key 하나가 받은 요청 수.
 *
 * @param key 대상 key
 * @param reads window 안의 읽기 요청 수 (0 이상)
 * @param writes window 안의 쓰기 요청 수 (0 이상)
 */
public record KeyLoad(Bytes key, long reads, long writes) {

    public KeyLoad {
        Objects.requireNonNull(key, "key");
        if (reads < 0 || writes < 0) {
            throw new IllegalArgumentException("request counts must be >= 0: reads=" + reads + " writes=" + writes);
        }
    }

    /** 읽기와 쓰기를 합한 요청 수. */
    public long requests() {
        return reads + writes;
    }
}
