package io.openkache.core.balance;

/**
 * 관찰 window 하나에서 key 하나가 받은 요청 수.
 *
 * @param keyHash key의 해시값({@link io.openkache.core.tablet.KeyHash#of}). 부호 없는 64비트로 해석한다
 * @param reads window 안의 읽기 요청 수 (0 이상)
 * @param writes window 안의 쓰기 요청 수 (0 이상)
 */
public record KeyLoad(long keyHash, long reads, long writes) {

    public KeyLoad {
        if (reads < 0 || writes < 0) {
            throw new IllegalArgumentException("request counts must be >= 0: reads=" + reads + " writes=" + writes);
        }
    }

    /** 읽기와 쓰기를 합한 요청 수. */
    public long requests() {
        return reads + writes;
    }
}
