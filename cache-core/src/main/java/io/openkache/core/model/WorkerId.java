package io.openkache.core.model;

/**
 * 한 노드 안의 core worker를 식별한다.
 *
 * <p>값은 {@code 0}부터 {@code workers - 1}까지다. Netty EventLoop와는 별개의 식별자다.
 */
public record WorkerId(int value) {

    public WorkerId {
        if (value < 0) {
            throw new IllegalArgumentException("WorkerId must be >= 0: " + value);
        }
    }
}
