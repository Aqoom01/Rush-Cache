package io.openkache.core.balance.split;

/**
 * 분할을 보류한 사유.
 *
 * <p>design.md §4의 나머지 사유(분할 시작 조건, 유예, 단일 hot key, 순차 쓰기)는 그 판단을 구현할 때 추가한다.
 */
public enum NoSplitReason {
    /** 범위 안에 key가 둘 미만이라 두 쪽 모두에 key가 있게 나눌 수 없다. */
    NO_VALID_POINT
}
